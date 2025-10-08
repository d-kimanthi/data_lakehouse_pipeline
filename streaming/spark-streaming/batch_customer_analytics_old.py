#!/usr/bin/env python3
"""
Spark batch job for daily customer analytics.
Processes Silver layer Iceberg tables to create Gold layer customer insights.
"""

import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Create Spark session with Iceberg support"""
    return (
        SparkSession.builder.appName("DailyCustomerAnalytics")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://data-lake/warehouse/")
        # MinIO configuration
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def process_daily_customer_analytics(spark, target_date):
    """Process daily customer analytics for the given date"""

    print(f"Processing customer analytics for {target_date}")

    # Create Gold table if not exists
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.daily_customer_analytics (
            user_id STRING,
            date DATE,
            page_views BIGINT,
            unique_sessions BIGINT,
            unique_products_viewed BIGINT,
            total_purchases BIGINT,
            total_revenue DECIMAL(12,2),
            avg_order_value DECIMAL(10,2),
            first_visit_time TIMESTAMP,
            last_activity_time TIMESTAMP,
            customer_segment STRING,
            created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (date)
        TBLPROPERTIES (
            'write.parquet.compression-codec'='snappy'
        )
    """
    )

    # Calculate daily customer metrics
    customer_analytics = spark.sql(
        f"""
        WITH page_view_metrics AS (
            SELECT 
                user_id,
                date(timestamp) as date,
                count(*) as page_views,
                count(distinct session_id) as unique_sessions,
                count(distinct product_id) as unique_products_viewed,
                min(timestamp) as first_visit_time,
                max(timestamp) as last_activity_time
            FROM iceberg.silver.page_views
            WHERE date(timestamp) = '{target_date}'
            GROUP BY user_id, date(timestamp)
        ),
        
        purchase_metrics AS (
            SELECT 
                user_id,
                date(timestamp) as date,
                count(*) as total_purchases,
                sum(total_price) as total_revenue,
                avg(total_price) as avg_order_value
            FROM iceberg.silver.purchases
            WHERE date(timestamp) = '{target_date}'
            GROUP BY user_id, date(timestamp)
        )
        
        SELECT 
            coalesce(pv.user_id, pm.user_id) as user_id,
            coalesce(pv.date, pm.date) as date,
            coalesce(pv.page_views, 0) as page_views,
            coalesce(pv.unique_sessions, 0) as unique_sessions,
            coalesce(pv.unique_products_viewed, 0) as unique_products_viewed,
            coalesce(pm.total_purchases, 0) as total_purchases,
            coalesce(pm.total_revenue, 0.0) as total_revenue,
            coalesce(pm.avg_order_value, 0.0) as avg_order_value,
            pv.first_visit_time,
            pv.last_activity_time,
            CASE 
                WHEN pm.total_purchases > 0 AND pm.total_revenue >= 1000 THEN 'high_value'
                WHEN pm.total_purchases > 0 AND pm.total_revenue >= 100 THEN 'medium_value'
                WHEN pm.total_purchases > 0 THEN 'low_value'
                WHEN pv.page_views > 10 THEN 'engaged_browser'
                WHEN pv.page_views > 0 THEN 'casual_browser'
                ELSE 'unknown'
            END as customer_segment,
            current_timestamp() as created_at
        FROM page_view_metrics pv
        FULL OUTER JOIN purchase_metrics pm
            ON pv.user_id = pm.user_id AND pv.date = pm.date
    """
    )

    # Write to Gold layer
    customer_analytics.write.format("iceberg").mode("overwrite").option(
        "path", "iceberg.gold.daily_customer_analytics"
    ).save()

    # Get summary stats
    total_customers = customer_analytics.count()
    high_value_customers = customer_analytics.filter(
        col("customer_segment") == "high_value"
    ).count()
    total_revenue = customer_analytics.agg(sum("total_revenue")).collect()[0][0] or 0

    print(f"✅ Processed {total_customers} customers")
    print(f"📊 High value customers: {high_value_customers}")
    print(f"💰 Total revenue: ${total_revenue:,.2f}")

    return {
        "total_customers": total_customers,
        "high_value_customers": high_value_customers,
        "total_revenue": float(total_revenue),
    }


def main():
    parser = argparse.ArgumentParser(description="Daily Customer Analytics Spark Job")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        results = process_daily_customer_analytics(spark, args.date)
        print(f"🎉 Customer analytics completed successfully: {results}")

    except Exception as e:
        print(f"❌ Job failed: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
