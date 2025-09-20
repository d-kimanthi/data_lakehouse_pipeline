# dagster/dagster_project/assets/data_marts.py

from datetime import datetime, timedelta
from typing import Any, Dict

import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from dagster import AssetIn, DailyPartitionsDefinition, MetadataValue, Output, asset

# Define partitions for daily processing
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    partitions_def=daily_partitions,
    description="Raw e-commerce events from Kafka streams stored in Iceberg bronze tables",
)
def bronze_events(context) -> Dict[str, Any]:
    """
    Bronze layer - Raw events from Kafka streams
    This asset represents the streaming data that's continuously written by Spark Streaming
    """
    partition_date = context.asset_partition_key_for_output()

    # In a real implementation, this would verify the data exists
    # and return metadata about the bronze layer for the given partition

    context.log.info(f"Processing bronze events for {partition_date}")

    # Connect to Iceberg via Spark (simplified for demo)
    spark = (
        SparkSession.builder.appName("dagster-bronze-verification")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "glue")
        .getOrCreate()
    )

    # Query bronze table for the partition
    query = f"""
    SELECT 
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT event_type) as event_types,
        MIN(event_timestamp) as min_timestamp,
        MAX(event_timestamp) as max_timestamp
    FROM iceberg.default.bronze_events 
    WHERE DATE(event_timestamp) = '{partition_date}'
    """

    try:
        result = spark.sql(query).collect()[0]
        stats = {
            "event_count": result["event_count"],
            "unique_users": result["unique_users"],
            "event_types": result["event_types"],
            "min_timestamp": str(result["min_timestamp"]),
            "max_timestamp": str(result["max_timestamp"]),
            "partition_date": partition_date,
        }

        context.add_output_metadata(
            {
                "event_count": MetadataValue.int(stats["event_count"]),
                "unique_users": MetadataValue.int(stats["unique_users"]),
                "event_types": MetadataValue.int(stats["event_types"]),
                "data_freshness": MetadataValue.text(
                    f"Latest event: {stats['max_timestamp']}"
                ),
            }
        )

        return stats

    except Exception as e:
        context.log.error(f"Error querying bronze events: {e}")
        return {"error": str(e), "partition_date": partition_date}

    finally:
        spark.stop()


@asset(
    ins={"bronze_events": AssetIn()},
    partitions_def=daily_partitions,
    description="Cleaned and validated events in the silver layer",
)
def silver_events(context, bronze_events: Dict[str, Any]) -> Dict[str, Any]:
    """
    Silver layer - Cleaned and validated events with data quality checks
    """
    partition_date = context.asset_partition_key_for_output()
    context.log.info(f"Processing silver events for {partition_date}")

    spark = (
        SparkSession.builder.appName("dagster-silver-processing")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "glue")
        .getOrCreate()
    )

    # Data quality and cleaning transformations
    cleaning_query = f"""
    CREATE OR REPLACE TABLE iceberg.default.silver_events_{partition_date.replace('-', '_')}
    USING ICEBERG
    AS
    SELECT 
        event_id,
        event_type,
        event_timestamp,
        user_id,
        session_id,
        year,
        month,
        day,
        hour,
        is_weekend,
        is_business_hours,
        data_quality_score,
        processing_time,
        -- Data quality flags
        CASE 
            WHEN user_id IS NULL THEN 'missing_user_id'
            WHEN event_timestamp IS NULL THEN 'missing_timestamp'
            WHEN data_quality_score < 0.5 THEN 'low_quality'
            ELSE 'valid'
        END as data_quality_flag,
        
        -- Event categorization
        CASE 
            WHEN event_type IN ('page_view', 'add_to_cart', 'purchase') THEN 'customer_journey'
            WHEN event_type = 'user_session' THEN 'session_management'
            WHEN event_type = 'product_update' THEN 'catalog_management'
            ELSE 'other'
        END as event_category
        
    FROM iceberg.default.bronze_events
    WHERE DATE(event_timestamp) = '{partition_date}'
      AND data_quality_score >= 0.3  -- Filter out very low quality events
      AND event_timestamp IS NOT NULL
    """

    try:
        spark.sql(cleaning_query)

        # Get statistics for the cleaned data
        stats_query = f"""
        SELECT 
            COUNT(*) as cleaned_event_count,
            COUNT(DISTINCT user_id) as unique_users,
            SUM(CASE WHEN data_quality_flag = 'valid' THEN 1 ELSE 0 END) as valid_events,
            AVG(data_quality_score) as avg_quality_score,
            COUNT(DISTINCT event_category) as event_categories
        FROM iceberg.default.silver_events_{partition_date.replace('-', '_')}
        """

        result = spark.sql(stats_query).collect()[0]

        stats = {
            "cleaned_event_count": result["cleaned_event_count"],
            "unique_users": result["unique_users"],
            "valid_events": result["valid_events"],
            "avg_quality_score": float(result["avg_quality_score"]),
            "event_categories": result["event_categories"],
            "partition_date": partition_date,
            "quality_percentage": (
                (result["valid_events"] / result["cleaned_event_count"]) * 100
                if result["cleaned_event_count"] > 0
                else 0
            ),
        }

        context.add_output_metadata(
            {
                "cleaned_events": MetadataValue.int(stats["cleaned_event_count"]),
                "data_quality_percentage": MetadataValue.float(
                    stats["quality_percentage"]
                ),
                "avg_quality_score": MetadataValue.float(stats["avg_quality_score"]),
            }
        )

        return stats

    except Exception as e:
        context.log.error(f"Error processing silver events: {e}")
        return {"error": str(e), "partition_date": partition_date}

    finally:
        spark.stop()


@asset(
    ins={"silver_events": AssetIn()},
    partitions_def=daily_partitions,
    description="Daily sales summary data mart for business analytics",
)
def daily_sales_summary(context, silver_events: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gold layer - Daily sales summary data mart
    """
    partition_date = context.asset_partition_key_for_output()
    context.log.info(f"Creating daily sales summary for {partition_date}")

    spark = (
        SparkSession.builder.appName("dagster-sales-summary")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "glue")
        .getOrCreate()
    )

    # Create comprehensive daily sales summary
    sales_summary_query = f"""
    CREATE OR REPLACE TABLE iceberg.default.daily_sales_summary
    USING ICEBERG
    PARTITIONED BY (report_date)
    AS
    WITH purchase_events AS (
        SELECT *
        FROM iceberg.default.silver_purchase_items
        WHERE DATE(event_timestamp) = '{partition_date}'
    ),
    
    daily_metrics AS (
        SELECT 
            '{partition_date}' as report_date,
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(DISTINCT user_id) as unique_customers,
            COUNT(*) as total_items_sold,
            SUM(total_price) as gross_revenue,
            AVG(total_price) as avg_item_value,
            SUM(quantity) as total_quantity,
            COUNT(DISTINCT product_id) as unique_products_sold,
            COUNT(DISTINCT category) as categories_sold,
            
            -- Payment method breakdown
            SUM(CASE WHEN payment_method = 'credit_card' THEN total_price ELSE 0 END) as credit_card_revenue,
            SUM(CASE WHEN payment_method = 'paypal' THEN total_price ELSE 0 END) as paypal_revenue,
            SUM(CASE WHEN payment_method = 'debit_card' THEN total_price ELSE 0 END) as debit_card_revenue,
            
            -- Time-based metrics
            COUNT(CASE WHEN hour(event_timestamp) BETWEEN 9 AND 17 THEN 1 END) as business_hours_orders,
            COUNT(CASE WHEN hour(event_timestamp) NOT BETWEEN 9 AND 17 THEN 1 END) as after_hours_orders
            
        FROM purchase_events
    ),
    
    category_performance AS (
        SELECT 
            '{partition_date}' as report_date,
            category,
            COUNT(DISTINCT order_id) as orders_per_category,
            SUM(total_price) as revenue_per_category,
            SUM(quantity) as quantity_per_category,
            AVG(total_price) as avg_item_price_per_category,
            COUNT(DISTINCT user_id) as unique_customers_per_category
        FROM purchase_events
        GROUP BY category
    ),
    
    hourly_sales AS (
        SELECT 
            '{partition_date}' as report_date,
            hour(event_timestamp) as sales_hour,
            COUNT(DISTINCT order_id) as hourly_orders,
            SUM(total_price) as hourly_revenue,
            COUNT(DISTINCT user_id) as hourly_customers
        FROM purchase_events
        GROUP BY hour(event_timestamp)
    )
    
    SELECT 
        dm.*,
        CURRENT_TIMESTAMP() as created_at,
        'dagster_batch' as processing_method
    FROM daily_metrics dm
    """

    try:
        spark.sql(sales_summary_query)

        # Get the summary statistics
        summary_stats_query = f"""
        SELECT *
        FROM iceberg.default.daily_sales_summary
        WHERE report_date = '{partition_date}'
        """

        result = spark.sql(summary_stats_query).collect()[0]

        stats = {
            "report_date": partition_date,
            "total_orders": result["total_orders"],
            "unique_customers": result["unique_customers"],
            "gross_revenue": float(result["gross_revenue"]),
            "avg_item_value": float(result["avg_item_value"]),
            "total_items_sold": result["total_items_sold"],
            "unique_products_sold": result["unique_products_sold"],
        }

        context.add_output_metadata(
            {
                "total_orders": MetadataValue.int(stats["total_orders"]),
                "gross_revenue": MetadataValue.float(stats["gross_revenue"]),
                "unique_customers": MetadataValue.int(stats["unique_customers"]),
                "avg_order_value": MetadataValue.float(
                    stats["gross_revenue"] / stats["total_orders"]
                    if stats["total_orders"] > 0
                    else 0
                ),
            }
        )

        return stats

    except Exception as e:
        context.log.error(f"Error creating daily sales summary: {e}")
        return {"error": str(e), "partition_date": partition_date}

    finally:
        spark.stop()


@asset(
    ins={"silver_events": AssetIn()},
    partitions_def=daily_partitions,
    description="User behavior analytics and customer journey metrics",
)
def user_behavior_metrics(context, silver_events: Dict[str, Any]) -> Dict[str, Any]:
    """
    Gold layer - User behavior and customer journey analytics
    """
    partition_date = context.asset_partition_key_for_output()
    context.log.info(f"Creating user behavior metrics for {partition_date}")

    spark = (
        SparkSession.builder.appName("dagster-user-behavior")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "glue")
        .getOrCreate()
    )

    user_behavior_query = f"""
    CREATE OR REPLACE TABLE iceberg.default.user_behavior_metrics
    USING ICEBERG
    PARTITIONED BY (analysis_date)
    AS
    WITH daily_events AS (
        SELECT *
        FROM iceberg.default.silver_events_{partition_date.replace('-', '_')}
    ),
    
    user_sessions AS (
        SELECT *
        FROM iceberg.default.silver_user_sessions
        WHERE DATE(session_start) = '{partition_date}'
    ),
    
    user_activity AS (
        SELECT 
            '{partition_date}' as analysis_date,
            user_id,
            COUNT(*) as total_events,
            COUNT(DISTINCT session_id) as sessions_count,
            COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) as page_views,
            COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) as cart_additions,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
            
            -- Session quality metrics
            AVG(CASE WHEN us.session_duration_minutes IS NOT NULL 
                THEN us.session_duration_minutes ELSE 0 END) as avg_session_duration,
            MAX(us.events_in_session) as max_events_per_session,
            
            -- Conversion funnel
            CASE WHEN COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) > 0 THEN 'converter'
                 WHEN COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) > 0 THEN 'cart_abandoner'
                 WHEN COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) > 0 THEN 'browser'
                 ELSE 'inactive' END as user_segment,
                 
            -- Engagement level
            CASE WHEN COUNT(*) >= 50 THEN 'high_engagement'
                 WHEN COUNT(*) >= 10 THEN 'medium_engagement'
                 WHEN COUNT(*) >= 1 THEN 'low_engagement'
                 ELSE 'no_engagement' END as engagement_level
                 
        FROM daily_events de
        LEFT JOIN user_sessions us ON de.user_id = us.user_id AND de.session_id = us.session_id
        WHERE de.user_id IS NOT NULL
        GROUP BY user_id
    ),
    
    funnel_metrics AS (
        SELECT 
            '{partition_date}' as analysis_date,
            COUNT(DISTINCT user_id) as total_active_users,
            COUNT(DISTINCT CASE WHEN page_views > 0 THEN user_id END) as users_with_page_views,
            COUNT(DISTINCT CASE WHEN cart_additions > 0 THEN user_id END) as users_with_cart_additions,
            COUNT(DISTINCT CASE WHEN purchases > 0 THEN user_id END) as users_with_purchases,
            
            -- Conversion rates
            ROUND(COUNT(DISTINCT CASE WHEN cart_additions > 0 THEN user_id END) * 100.0 / 
                  NULLIF(COUNT(DISTINCT CASE WHEN page_views > 0 THEN user_id END), 0), 2) as page_to_cart_rate,
            ROUND(COUNT(DISTINCT CASE WHEN purchases > 0 THEN user_id END) * 100.0 / 
                  NULLIF(COUNT(DISTINCT CASE WHEN cart_additions > 0 THEN user_id END), 0), 2) as cart_to_purchase_rate,
            ROUND(COUNT(DISTINCT CASE WHEN purchases > 0 THEN user_id END) * 100.0 / 
                  NULLIF(COUNT(DISTINCT user_id), 0), 2) as overall_conversion_rate,
                  
            -- Engagement metrics
            AVG(total_events) as avg_events_per_user,
            AVG(sessions_count) as avg_sessions_per_user,
            AVG(avg_session_duration) as avg_session_duration_minutes
            
        FROM user_activity
    )
    
    SELECT 
        fm.*,
        CURRENT_TIMESTAMP() as created_at
    FROM funnel_metrics fm
    """

    try:
        spark.sql(user_behavior_query)

        # Get the metrics
        metrics_query = f"""
        SELECT *
        FROM iceberg.default.user_behavior_metrics
        WHERE analysis_date = '{partition_date}'
        """

        result = spark.sql(metrics_query).collect()[0]

        stats = {
            "analysis_date": partition_date,
            "total_active_users": result["total_active_users"],
            "overall_conversion_rate": float(result["overall_conversion_rate"]),
            "page_to_cart_rate": float(result["page_to_cart_rate"]),
            "cart_to_purchase_rate": float(result["cart_to_purchase_rate"]),
            "avg_events_per_user": float(result["avg_events_per_user"]),
            "avg_sessions_per_user": float(result["avg_sessions_per_user"]),
        }

        context.add_output_metadata(
            {
                "total_active_users": MetadataValue.int(stats["total_active_users"]),
                "conversion_rate": MetadataValue.float(
                    stats["overall_conversion_rate"]
                ),
                "funnel_efficiency": MetadataValue.float(stats["page_to_cart_rate"]),
                "user_engagement": MetadataValue.float(stats["avg_events_per_user"]),
            }
        )

        return stats

    except Exception as e:
        context.log.error(f"Error creating user behavior metrics: {e}")
        return {"error": str(e), "partition_date": partition_date}

    finally:
        spark.stop()


@asset(
    ins={
        "daily_sales_summary": AssetIn(),
        "user_behavior_metrics": AssetIn(),
    },
    partitions_def=daily_partitions,
    description="Product performance analytics combining sales and user behavior data",
)
def product_analytics(
    context, daily_sales_summary: Dict[str, Any], user_behavior_metrics: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Gold layer - Product performance analytics
    """
    partition_date = context.asset_partition_key_for_output()
    context.log.info(f"Creating product analytics for {partition_date}")

    spark = (
        SparkSession.builder.appName("dagster-product-analytics")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "glue")
        .getOrCreate()
    )

    product_analytics_query = f"""
    CREATE OR REPLACE TABLE iceberg.default.product_analytics
    USING ICEBERG
    PARTITIONED BY (analysis_date)
    AS
    WITH product_views AS (
        SELECT 
            product_id,
            COUNT(*) as total_views,
            COUNT(DISTINCT user_id) as unique_viewers,
            COUNT(DISTINCT session_id) as unique_sessions
        FROM iceberg.default.silver_events_{partition_date.replace('-', '_')}
        WHERE event_type = 'page_view'
          AND product_id IS NOT NULL
        GROUP BY product_id
    ),
    
    product_purchases AS (
        SELECT 
            product_id,
            COUNT(DISTINCT order_id) as orders_count,
            SUM(quantity) as total_quantity_sold,
            SUM(total_price) as total_revenue,
            AVG(unit_price) as avg_selling_price,
            COUNT(DISTINCT user_id) as unique_buyers,
            category,
            brand
        FROM iceberg.default.silver_purchase_items
        WHERE DATE(event_timestamp) = '{partition_date}'
        GROUP BY product_id, category, brand
    ),
    
    product_cart_adds AS (
        SELECT 
            product_id,
            COUNT(*) as cart_additions,
            COUNT(DISTINCT user_id) as unique_cart_users
        FROM iceberg.default.silver_events_{partition_date.replace('-', '_')}
        WHERE event_type = 'add_to_cart'
          AND product_id IS NOT NULL
        GROUP BY product_id
    )
    
    SELECT 
        '{partition_date}' as analysis_date,
        COALESCE(pv.product_id, pp.product_id, pca.product_id) as product_id,
        pp.category,
        pp.brand,
        
        -- View metrics
        COALESCE(pv.total_views, 0) as total_views,
        COALESCE(pv.unique_viewers, 0) as unique_viewers,
        COALESCE(pv.unique_sessions, 0) as unique_sessions,
        
        -- Cart metrics
        COALESCE(pca.cart_additions, 0) as cart_additions,
        COALESCE(pca.unique_cart_users, 0) as unique_cart_users,
        
        -- Purchase metrics
        COALESCE(pp.orders_count, 0) as orders_count,
        COALESCE(pp.total_quantity_sold, 0) as total_quantity_sold,
        COALESCE(pp.total_revenue, 0.0) as total_revenue,
        COALESCE(pp.avg_selling_price, 0.0) as avg_selling_price,
        COALESCE(pp.unique_buyers, 0) as unique_buyers,
        
        -- Conversion metrics
        ROUND(COALESCE(pca.cart_additions, 0) * 100.0 / NULLIF(pv.total_views, 0), 2) as view_to_cart_rate,
        ROUND(COALESCE(pp.orders_count, 0) * 100.0 / NULLIF(pca.cart_additions, 0), 2) as cart_to_purchase_rate,
        ROUND(COALESCE(pp.orders_count, 0) * 100.0 / NULLIF(pv.total_views, 0), 2) as view_to_purchase_rate,
        
        -- Performance ranking
        DENSE_RANK() OVER (ORDER BY COALESCE(pp.total_revenue, 0) DESC) as revenue_rank,
        DENSE_RANK() OVER (ORDER BY COALESCE(pv.total_views, 0) DESC) as views_rank,
        
        CURRENT_TIMESTAMP() as created_at
        
    FROM product_views pv
    FULL OUTER JOIN product_purchases pp ON pv.product_id = pp.product_id
    FULL OUTER JOIN product_cart_adds pca ON COALESCE(pv.product_id, pp.product_id) = pca.product_id
    """

    try:
        spark.sql(product_analytics_query)

        # Get summary statistics
        summary_query = f"""
        SELECT 
            COUNT(*) as total_products_analyzed,
            SUM(total_revenue) as total_product_revenue,
            AVG(view_to_cart_rate) as avg_view_to_cart_rate,
            AVG(cart_to_purchase_rate) as avg_cart_to_purchase_rate,
            COUNT(CASE WHEN orders_count > 0 THEN 1 END) as products_with_sales,
            MAX(total_views) as max_product_views
        FROM iceberg.default.product_analytics
        WHERE analysis_date = '{partition_date}'
        """

        result = spark.sql(summary_query).collect()[0]

        stats = {
            "analysis_date": partition_date,
            "total_products_analyzed": result["total_products_analyzed"],
            "total_product_revenue": float(result["total_product_revenue"]),
            "avg_view_to_cart_rate": float(result["avg_view_to_cart_rate"]),
            "avg_cart_to_purchase_rate": float(result["avg_cart_to_purchase_rate"]),
            "products_with_sales": result["products_with_sales"],
            "max_product_views": result["max_product_views"],
        }

        context.add_output_metadata(
            {
                "products_analyzed": MetadataValue.int(
                    stats["total_products_analyzed"]
                ),
                "avg_conversion_rate": MetadataValue.float(
                    stats["avg_view_to_cart_rate"]
                ),
                "products_sold": MetadataValue.int(stats["products_with_sales"]),
                "total_revenue": MetadataValue.float(stats["total_product_revenue"]),
            }
        )

        return stats

    except Exception as e:
        context.log.error(f"Error creating product analytics: {e}")
        return {"error": str(e), "partition_date": partition_date}

    finally:
        spark.stop()


# Data quality checks
@asset(
    ins={
        "bronze_events": AssetIn(),
        "silver_events": AssetIn(),
    },
    partitions_def=daily_partitions,
    description="Data quality monitoring and validation checks",
)
def data_quality_report(
    context, bronze_events: Dict[str, Any], silver_events: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Data quality monitoring and alerting
    """
    partition_date = context.asset_partition_key_for_output()
    context.log.info(f"Running data quality checks for {partition_date}")

    # Calculate data quality metrics
    bronze_count = bronze_events.get("event_count", 0)
    silver_count = silver_events.get("cleaned_event_count", 0)

    # Quality checks
    data_loss_rate = (
        ((bronze_count - silver_count) / bronze_count * 100) if bronze_count > 0 else 0
    )
    avg_quality_score = silver_events.get("avg_quality_score", 0)

    quality_report = {
        "partition_date": partition_date,
        "bronze_event_count": bronze_count,
        "silver_event_count": silver_count,
        "data_loss_rate_percent": data_loss_rate,
        "avg_quality_score": avg_quality_score,
        "quality_status": (
            "PASS" if data_loss_rate < 10 and avg_quality_score > 0.7 else "FAIL"
        ),
        "alerts": [],
    }

    # Generate alerts
    if data_loss_rate > 10:
        quality_report["alerts"].append(f"High data loss rate: {data_loss_rate:.2f}%")

    if avg_quality_score < 0.7:
        quality_report["alerts"].append(
            f"Low average quality score: {avg_quality_score:.2f}"
        )

    if bronze_count == 0:
        quality_report["alerts"].append("No events found in bronze layer")

    context.add_output_metadata(
        {
            "quality_status": MetadataValue.text(quality_report["quality_status"]),
            "data_loss_rate": MetadataValue.float(data_loss_rate),
            "quality_score": MetadataValue.float(avg_quality_score),
            "alerts_count": MetadataValue.int(len(quality_report["alerts"])),
        }
    )

    if quality_report["alerts"]:
        context.log.warning(f"Data quality issues detected: {quality_report['alerts']}")

    return quality_report
