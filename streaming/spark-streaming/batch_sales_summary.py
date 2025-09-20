#!/usr/bin/env python3
"""
Spark batch job for daily sales summary.
Processes Silver layer Iceberg tables to create Gold layer sales analytics.
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Create Spark session with Iceberg support"""
    return (
        SparkSession.builder.appName("DailySalesSummary")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config(
            "spark.sql.catalog.iceberg.warehouse", "s3a://bronze/iceberg-warehouse/"
        )
        # MinIO configuration
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def process_daily_sales_summary(spark, target_date):
    """Process daily sales summary for the given date"""

    print(f"Processing sales summary for {target_date}")

    # Create Gold table if not exists
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.daily_sales_summary (
            date DATE,
            total_orders BIGINT,
            total_revenue DECIMAL(12,2),
            avg_order_value DECIMAL(10,2),
            unique_customers BIGINT,
            total_page_views BIGINT,
            unique_visitors BIGINT,
            conversion_rate DECIMAL(5,4),
            revenue_per_visitor DECIMAL(10,2),
            top_payment_method STRING,
            created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (date)
        TBLPROPERTIES (
            'write.parquet.compression-codec'='snappy'
        )
    """
    )

    # Calculate daily sales summary
    sales_summary = spark.sql(
        f"""
        WITH sales_metrics AS (
            SELECT 
                date(timestamp) as date,
                count(*) as total_orders,
                sum(total_price) as total_revenue,
                avg(total_price) as avg_order_value,
                count(distinct user_id) as unique_customers,
                -- Get most popular payment method
                first(payment_method) OVER (
                    PARTITION BY date(timestamp) 
                    ORDER BY count(*) OVER (PARTITION BY date(timestamp), payment_method) DESC
                ) as top_payment_method
            FROM iceberg.silver.purchases
            WHERE date(timestamp) = '{target_date}'
            GROUP BY date(timestamp)
        ),
        
        traffic_metrics AS (
            SELECT 
                date(timestamp) as date,
                count(*) as total_page_views,
                count(distinct user_id) as unique_visitors
            FROM iceberg.silver.page_views
            WHERE date(timestamp) = '{target_date}'
            GROUP BY date(timestamp)
        )
        
        SELECT 
            coalesce(s.date, t.date) as date,
            coalesce(s.total_orders, 0) as total_orders,
            coalesce(s.total_revenue, 0.0) as total_revenue,
            coalesce(s.avg_order_value, 0.0) as avg_order_value,
            coalesce(s.unique_customers, 0) as unique_customers,
            coalesce(t.total_page_views, 0) as total_page_views,
            coalesce(t.unique_visitors, 0) as unique_visitors,
            CASE 
                WHEN t.unique_visitors > 0 
                THEN s.unique_customers / t.unique_visitors 
                ELSE 0.0 
            END as conversion_rate,
            CASE 
                WHEN t.unique_visitors > 0 
                THEN s.total_revenue / t.unique_visitors 
                ELSE 0.0 
            END as revenue_per_visitor,
            coalesce(s.top_payment_method, 'none') as top_payment_method,
            current_timestamp() as created_at
        FROM sales_metrics s
        FULL OUTER JOIN traffic_metrics t ON s.date = t.date
    """
    )

    # Write to Gold layer
    sales_summary.write.format("iceberg").mode("overwrite").option(
        "path", "iceberg.gold.daily_sales_summary"
    ).save()

    # Get summary stats
    summary_data = sales_summary.collect()[0]

    print(f"✅ Sales summary processed successfully")
    print(f"📊 Total orders: {summary_data['total_orders']}")
    print(f"💰 Total revenue: ${summary_data['total_revenue']:,.2f}")
    print(f"👥 Unique customers: {summary_data['unique_customers']}")
    print(f"📈 Conversion rate: {summary_data['conversion_rate']:.2%}")

    return {
        "total_orders": summary_data["total_orders"],
        "total_revenue": float(summary_data["total_revenue"]),
        "unique_customers": summary_data["unique_customers"],
        "conversion_rate": float(summary_data["conversion_rate"]),
    }


def main():
    parser = argparse.ArgumentParser(description="Daily Sales Summary Spark Job")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        results = process_daily_sales_summary(spark, args.date)
        print(f"🎉 Sales summary completed successfully: {results}")

    except Exception as e:
        print(f"❌ Job failed: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
