-- analytics/sql/data_marts/advanced_customer_analytics.sql

-- Customer Lifetime Value Analysis
CREATE OR REPLACE VIEW customer_lifetime_value AS
WITH customer_purchase_history AS (
    SELECT 
        user_id,
        MIN(DATE(event_timestamp)) as first_purchase_date,
        MAX(DATE(event_timestamp)) as last_purchase_date,
        COUNT(DISTINCT order_id) as total_orders,
        SUM(total_price) as total_revenue,
        AVG(total_price) as avg_order_value,
        COUNT(DISTINCT DATE(event_timestamp)) as purchase_days,
        DATEDIFF(MAX(DATE(event_timestamp)), MIN(DATE(event_timestamp))) as customer_lifespan_days
    FROM iceberg.default.silver_purchase_items
    GROUP BY user_id
),

customer_behavior AS (
    SELECT 
        ub.user_id,
        AVG(ub.total_events) as avg_daily_events,
        AVG(ub.sessions_count) as avg_daily_sessions,
        AVG(ub.page_views) as avg_daily_page_views,
        COUNT(*) as active_days,
        MAX(ub.analysis_date) as last_active_date
    FROM (
        SELECT user_id, analysis_date, total_events, sessions_count, page_views
        FROM iceberg.default.user_behavior_metrics ua
        LATERAL VIEW EXPLODE(
            -- This would need to be adapted based on actual schema
            ARRAY(ua.user_id)  -- Placeholder for user-level data
        ) AS user_id
    ) ub
    GROUP BY ub.user_id
),

customer_segments AS (
    SELECT 
        cph.*,
        cb.avg_daily_events,
        cb.avg_daily_sessions,
        cb.active_days,
        cb.last_active_date,
        
        -- CLV Calculation (simplified)
        CASE 
            WHEN customer_lifespan_days > 0 
            THEN (total_revenue / customer_lifespan_days) * 365 
            ELSE total_revenue 
        END as estimated_annual_clv,
        
        -- Customer Segmentation
        CASE 
            WHEN total_revenue >= 1000 AND total_orders >= 10 THEN 'VIP'
            WHEN total_revenue >= 500 AND total_orders >= 5 THEN 'High Value'
            WHEN total_revenue >= 100 AND total_orders >= 2 THEN 'Regular'
            WHEN total_orders = 1 THEN 'One-time'
            ELSE 'Low Value'
        END as customer_segment,
        
        -- Recency, Frequency, Monetary (RFM) Scores
        NTILE(5) OVER (ORDER BY DATEDIFF(CURRENT_DATE(), last_purchase_date)) as recency_score,
        NTILE(5) OVER (ORDER BY total_orders) as frequency_score,
        NTILE(5) OVER (ORDER BY total_revenue) as monetary_score
        
    FROM customer_purchase_history cph
    LEFT JOIN customer_behavior cb ON cph.user_id = cb.user_id
)

SELECT 
    *,
    -- RFM Combined Score
    (recency_score * 100) + (frequency_score * 10) + monetary_score as rfm_score,
    
    -- Customer Status
    CASE 
        WHEN DATEDIFF(CURRENT_DATE(), last_purchase_date) <= 30 THEN 'Active'
        WHEN DATEDIFF(CURRENT_DATE(), last_purchase_date) <= 90 THEN 'At Risk'
        WHEN DATEDIFF(CURRENT_DATE(), last_purchase_date) <= 180 THEN 'Dormant'
        ELSE 'Churned'
    END as customer_status,
    
    CURRENT_TIMESTAMP() as analysis_timestamp
FROM customer_segments;

-- Product Recommendation Engine Data
CREATE OR REPLACE VIEW product_recommendation_data AS
WITH product_affinity AS (
    SELECT 
        p1.product_id as product_a,
        p2.product_id as product_b,
        COUNT(*) as co_purchase_count,
        COUNT(DISTINCT p1.order_id) as orders_with_both
    FROM iceberg.default.silver_purchase_items p1
    JOIN iceberg.default.silver_purchase_items p2 
        ON p1.order_id = p2.order_id 
        AND p1.product_id != p2.product_id
    GROUP BY p1.product_id, p2.product_id
    HAVING co_purchase_count >= 5  -- Minimum threshold
),

product_popularity AS (
    SELECT 
        product_id,
        category,
        brand,
        COUNT(DISTINCT order_id) as total_orders,
        SUM(quantity) as total_quantity_sold,
        AVG(unit_price) as avg_price,
        COUNT(DISTINCT user_id) as unique_customers
    FROM iceberg.default.silver_purchase_items
    GROUP BY product_id, category, brand
),

category_preferences AS (
    SELECT 
        user_id,
        category,
        COUNT(*) as category_purchases,
        SUM(total_price) as category_spend,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) as preference_rank
    FROM iceberg.default.silver_purchase_items
    GROUP BY user_id, category
)

SELECT 
    pa.*,
    pp_a.category as product_a_category,
    pp_a.brand as product_a_brand,
    pp_b.category as product_b_category,
    pp_b.brand as product_b_brand,
    
    -- Affinity Score
    (pa.co_purchase_count * 1.0 / GREATEST(pp_a.total_orders, pp_b.total_orders)) as affinity_score,
    
    CURRENT_TIMESTAMP() as created_at
    
FROM product_affinity pa
JOIN product_popularity pp_a ON pa.product_a = pp_a.product_id
JOIN product_popularity pp_b ON pa.product_b = pp_b.product_id;

-- Real-time Anomaly Detection
CREATE OR REPLACE VIEW real_time_anomalies AS
WITH hourly_metrics AS (
    SELECT 
        DATE_TRUNC('hour', event_timestamp) as hour_window,
        event_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as unique_sessions
    FROM iceberg.default.bronze_events
    WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
    GROUP BY DATE_TRUNC('hour', event_timestamp), event_type
),

historical_patterns AS (
    SELECT 
        event_type,
        HOUR(hour_window) as hour_of_day,
        DAYOFWEEK(hour_window) as day_of_week,
        AVG(event_count) as avg_event_count,
        STDDEV(event_count) as stddev_event_count,
        MIN(event_count) as min_event_count,
        MAX(event_count) as max_event_count
    FROM hourly_metrics
    WHERE hour_window < DATE_TRUNC('day', CURRENT_TIMESTAMP())
    GROUP BY event_type, HOUR(hour_window), DAYOFWEEK(hour_window)
),

current_vs_expected AS (
    SELECT 
        hm.*,
        hp.avg_event_count as expected_count,
        hp.stddev_event_count,
        hp.min_event_count as historical_min,
        hp.max_event_count as historical_max,
        
        -- Z-score calculation
        CASE 
            WHEN hp.stddev_event_count > 0 
            THEN (hm.event_count - hp.avg_event_count) / hp.stddev_event_count
            ELSE 0 
        END as z_score
        
    FROM hourly_metrics hm
    JOIN historical_patterns hp 
        ON hm.event_type = hp.event_type
        AND HOUR(hm.hour_window) = hp.hour_of_day
        AND DAYOFWEEK(hm.hour_window) = hp.day_of_week
    WHERE hm.hour_window >= DATE_TRUNC('hour', CURRENT_TIMESTAMP()) - INTERVAL 1 HOURS
)

SELECT 
    *,
    -- Anomaly Classification
    CASE 
        WHEN ABS(z_score) >= 3 THEN 'Critical'
        WHEN ABS(z_score) >= 2 THEN 'Warning'
        WHEN event_count = 0 AND expected_count > 10 THEN 'Data Gap'
        ELSE 'Normal'
    END as anomaly_level,
    
    -- Anomaly Description
    CASE 
        WHEN z_score >= 3 THEN 'Unusually high activity'
        WHEN z_score <= -3 THEN 'Unusually low activity'
        WHEN z_score >= 2 THEN 'Higher than normal activity'
        WHEN z_score <= -2 THEN 'Lower than normal activity'
        WHEN event_count = 0 AND expected_count > 10 THEN 'No events detected'
        ELSE 'Normal activity'
    END as anomaly_description,
    
    CURRENT_TIMESTAMP() as detected_at
    
FROM current_vs_expected
WHERE ABS(z_score) >= 1.5 OR (event_count = 0 AND expected_count > 5);

-- Marketing Campaign Effectiveness
CREATE OR REPLACE VIEW campaign_effectiveness AS
WITH campaign_attribution AS (
    -- This assumes referrer data can be mapped to campaigns
    SELECT 
        user_id,
        session_id,
        CASE 
            WHEN raw_message LIKE '%utm_campaign%' THEN 
                REGEXP_EXTRACT(raw_message, 'utm_campaign=([^&"]*)', 1)
            WHEN referrer LIKE '%facebook%' THEN 'facebook_organic'
            WHEN referrer LIKE '%google%' THEN 'google_organic'
            WHEN referrer = 'email' THEN 'email_campaign'
            WHEN referrer = 'advertisement' THEN 'paid_advertising'
            ELSE 'direct'
        END as campaign_source,
        MIN(event_timestamp) as first_touchpoint,
        MAX(event_timestamp) as last_touchpoint
    FROM iceberg.default.bronze_events
    WHERE event_type = 'page_view'
    GROUP BY user_id, session_id, 
        CASE 
            WHEN raw_message LIKE '%utm_campaign%' THEN 
                REGEXP_EXTRACT(raw_message, 'utm_campaign=([^&"]*)', 1)
            WHEN referrer LIKE '%facebook%' THEN 'facebook_organic'
            WHEN referrer LIKE '%google%' THEN 'google_organic'
            WHEN referrer = 'email' THEN 'email_campaign'
            WHEN referrer = 'advertisement' THEN 'paid_advertising'
            ELSE 'direct'
        END
),

campaign_conversions AS (
    SELECT 
        ca.campaign_source,
        COUNT(DISTINCT ca.user_id) as unique_visitors,
        COUNT(DISTINCT ca.session_id) as total_sessions,
        COUNT(DISTINCT pi.order_id) as conversions,
        SUM(pi.total_price) as total_revenue,
        AVG(pi.total_price) as avg_order_value
    FROM campaign_attribution ca
    LEFT JOIN iceberg.default.silver_purchase_items pi 
        ON ca.user_id = pi.user_id
        AND pi.event_timestamp BETWEEN ca.first_touchpoint 
        AND ca.first_touchpoint + INTERVAL 7 DAYS  -- 7-day attribution window
    GROUP BY ca.campaign_source
)

SELECT 
    campaign_source,
    unique_visitors,
    total_sessions,
    COALESCE(conversions, 0) as conversions,
    COALESCE(total_revenue, 0) as total_revenue,
    COALESCE(avg_order_value, 0) as avg_order_value,
    
    -- Campaign Metrics
    ROUND(total_sessions * 1.0 / unique_visitors, 2) as sessions_per_visitor,
    ROUND(COALESCE(conversions, 0) * 100.0 / unique_visitors, 2) as conversion_rate_percent,
    ROUND(COALESCE(total_revenue, 0) / unique_visitors, 2) as revenue_per_visitor,
    
    -- Performance Ranking
    ROW_NUMBER() OVER (ORDER BY COALESCE(total_revenue, 0) DESC) as revenue_rank,
    ROW_NUMBER() OVER (ORDER BY COALESCE(conversions, 0) * 100.0 / unique_visitors DESC) as conversion_rank,
    
    CURRENT_TIMESTAMP() as analysis_timestamp
    
FROM campaign_conversions
ORDER BY total_revenue DESC;