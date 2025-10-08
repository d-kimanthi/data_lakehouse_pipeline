#!/usr/bin/env python3
"""
Spark batch job for product performance analytics.
Processes Silver layer Iceberg tables to create Gold layer product insights.
"""

import argparse
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Create Spark session with Iceberg support"""
    return (
        SparkSession.builder.appName("ProductPerformanceAnalytics")
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


def process_product_performance_analytics(spark, target_date):
    """Process product performance analytics for the given date"""

    print(f"Processing product performance analytics for {target_date}")

    # Create Gold table if not exists
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.product_performance_analytics (
            product_id STRING,
            date DATE,
            total_views BIGINT,
            total_purchases BIGINT,
            conversion_rate DECIMAL(5,4),
            total_revenue DECIMAL(12,2),
            avg_price DECIMAL(10,2),
            unique_viewers BIGINT,
            revenue_rank INT,
            created_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (date)
        TBLPROPERTIES (
            'write.parquet.compression-codec'='snappy'
        )
    """
    )

    # Calculate product performance metrics
    spark.sql(
        f"""
        WITH product_views AS (
            SELECT 
                product_id,
                count(*) as total_views,
                count(distinct user_id) as unique_viewers
            FROM iceberg.silver.page_views
            WHERE date(timestamp) = '{target_date}' AND product_id IS NOT NULL
            GROUP BY product_id
        ),
        product_purchases AS (
            SELECT 
                p.product_id,
                count(*) as total_purchases,
                sum(p.total_price) as total_revenue,
                avg(p.total_price) as avg_price
            FROM iceberg.silver.purchases p
            WHERE date(p.timestamp) = '{target_date}'
            GROUP BY p.product_id
        ),
        product_analytics AS (
            SELECT 
                coalesce(v.product_id, p.product_id) as product_id,
                date('{target_date}') as date,
                coalesce(v.total_views, 0) as total_views,
                coalesce(p.total_purchases, 0) as total_purchases,
                CASE 
                    WHEN v.total_views > 0 THEN cast(p.total_purchases as decimal) / cast(v.total_views as decimal)
                    ELSE 0.0
                END as conversion_rate,
                coalesce(p.total_revenue, 0.0) as total_revenue,
                coalesce(p.avg_price, 0.0) as avg_price,
                coalesce(v.unique_viewers, 0) as unique_viewers,
                row_number() OVER (ORDER BY p.total_revenue DESC) as revenue_rank,
                current_timestamp() as created_at
            FROM product_views v
            FULL OUTER JOIN product_purchases p ON v.product_id = p.product_id
        )
        
        INSERT OVERWRITE iceberg.gold.product_performance_analytics
        SELECT * FROM product_analytics
        WHERE date = '{target_date}'
    """
    )

    # Get summary stats
    summary_df = spark.sql(
        f"""
        SELECT 
            count(*) as total_products,
            sum(total_purchases) as total_purchases,
            sum(total_revenue) as total_revenue,
            avg(conversion_rate) as avg_conversion_rate
        FROM iceberg.gold.product_performance_analytics
        WHERE date = '{target_date}'
    """
    )

    summary = summary_df.collect()[0]

    total_products = summary.total_products or 0
    total_purchases = summary.total_purchases or 0
    total_revenue = float(summary.total_revenue or 0.0)
    avg_conversion_rate = float(summary.avg_conversion_rate or 0.0)

    print(f"✅ Processed {total_products} products")
    print(f"📦 Total purchases: {total_purchases}")
    print(f"💰 Total revenue: ${total_revenue:,.2f}")
    print(f"📈 Average conversion rate: {avg_conversion_rate:.2%}")

    return {
        "total_products": total_products,
        "total_purchases": total_purchases,
        "total_revenue": total_revenue,
        "avg_conversion_rate": avg_conversion_rate,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Product Performance Analytics Spark Job"
    )
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = create_spark_session()

    try:
        results = process_product_performance_analytics(spark, args.date)
        print(f"DAGSTER_RESULT:{json.dumps(results)}")
        print(f"🎉 Product performance analytics completed successfully: {results}")

    except Exception as e:
        print(f"❌ Job failed: {str(e)}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
