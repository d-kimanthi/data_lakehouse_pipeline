-- models/marts/daily_sales_summary.sql
-- Daily sales metrics for business reporting

with daily_purchases as (
    select
        event_date,
        count(*) as total_purchases,
        count(distinct user_id) as unique_customers,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value,
        min(total_amount) as min_order_value,
        max(total_amount) as max_order_value,
        sum(case when purchase_size = 'Large' then 1 else 0 end) as large_orders,
        sum(case when purchase_size = 'Medium' then 1 else 0 end) as medium_orders,
        sum(case when purchase_size = 'Small' then 1 else 0 end) as small_orders
    from {{ ref('stg_purchases') }}
    group by event_date
),

daily_traffic as (
    select
        event_date,
        count(*) as total_page_views,
        count(distinct user_id) as unique_visitors,
        count(distinct session_id) as unique_sessions,
        avg(case when is_anonymous_user = 0 then 1.0 else 0.0 end) as logged_in_rate
    from {{ ref('stg_page_views') }}
    group by event_date
)

select
    coalesce(p.event_date, t.event_date) as date,
    -- Sales metrics
    coalesce(p.total_purchases, 0) as total_purchases,
    coalesce(p.unique_customers, 0) as unique_customers,
    coalesce(p.total_revenue, 0) as total_revenue,
    coalesce(p.avg_order_value, 0) as avg_order_value,
    coalesce(p.min_order_value, 0) as min_order_value,
    coalesce(p.max_order_value, 0) as max_order_value,
    coalesce(p.large_orders, 0) as large_orders,
    coalesce(p.medium_orders, 0) as medium_orders,
    coalesce(p.small_orders, 0) as small_orders,
    -- Traffic metrics
    coalesce(t.total_page_views, 0) as total_page_views,
    coalesce(t.unique_visitors, 0) as unique_visitors,
    coalesce(t.unique_sessions, 0) as unique_sessions,
    coalesce(t.logged_in_rate, 0) as logged_in_rate,
    -- Conversion metrics
    case 
        when t.unique_visitors > 0 
        then p.unique_customers::float / t.unique_visitors 
        else 0 
    end as conversion_rate,
    case 
        when t.total_page_views > 0 
        then p.total_purchases::float / t.total_page_views 
        else 0 
    end as purchase_per_pageview,
    -- Business metrics
    current_timestamp as updated_at
from daily_purchases p
full outer join daily_traffic t on p.event_date = t.event_date
order by date desc