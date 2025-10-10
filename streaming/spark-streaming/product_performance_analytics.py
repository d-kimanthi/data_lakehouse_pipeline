#!/usr/bin/env python3
"""
Spark batch job for product performance analytics with Dagster Pipes.
Processes Silver layer Iceberg tables to create Gold layer product insights.
"""

import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def process_product_performance_analytics(target_date: str):
    """Process product performance analytics with Dagster Pipes"""
    try:
        from dagster_pipes import open_dagster_pipes

        with open_dagster_pipes() as context:
            context.log.info("Starting product performance analytics")
            context.log.info(f"Processing date: {target_date}")

            spark = (
                SparkSession.builder.appName(f"ProductPerformance-{target_date}")
                .config(
                    "spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.0",
                )
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
                )
                .config(
                    "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
                )
                .config(
                    "spark.sql.catalog.iceberg.catalog-impl",
                    "org.apache.iceberg.nessie.NessieCatalog",
                )
                .config("spark.sql.catalog.iceberg.uri", "http://nessie:19120/api/v1")
                .config("spark.sql.catalog.iceberg.ref", "main")
                .config(
                    "spark.sql.catalog.iceberg.warehouse", "s3a://data-lake/warehouse/"
                )
                .config(
                    "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .getOrCreate()
            )

            context.log.info("Spark session initialized successfully")

            context.log.info("Spark session initialized successfully")

            try:
                # Calculate product performance metrics
                context.log.info("📊 Calculating product performance metrics")

                product_analytics = spark.sql(
                    f"""
                    WITH product_views AS (
                        SELECT 
                            product_id,
                            count(*) as total_views,
                            count(distinct user_id) as unique_viewers
                        FROM iceberg.silver.page_views
                        WHERE date(timestamp) >= date('{target_date}') AND product_id IS NOT NULL
                        GROUP BY product_id
                    ),
                    product_purchases AS (
                        SELECT 
                            item.product_id,
                            max(item.category) as category,
                            max(item.brand) as brand,
                            sum(item.quantity) as total_purchases,
                            sum(item.total_price) as total_revenue,
                            avg(item.unit_price) as avg_price
                        FROM iceberg.silver.purchases
                        LATERAL VIEW explode(items) AS item
                        WHERE date(timestamp) >= date('{target_date}')
                        GROUP BY item.product_id
                    )
                    SELECT 
                        coalesce(v.product_id, p.product_id) as product_id,
                        p.category,
                        p.brand,
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
                        current_timestamp() as created_at
                    FROM product_views v
                    FULL OUTER JOIN product_purchases p ON v.product_id = p.product_id
                """
                )

                total_products = product_analytics.count()
                context.log.info(f"📈 Calculated metrics for {total_products} products")

                if total_products > 0:
                    # Show sample results
                    context.log.info("📋 Sample product analytics:")
                    product_analytics.show(5, truncate=False)

                    # Write to Iceberg Gold layer
                    context.log.info(
                        "💾 Saving product analytics to Iceberg gold layer"
                    )
                    product_analytics.write.format("iceberg").mode("overwrite").option(
                        "write.parquet.compression-codec", "snappy"
                    ).partitionBy("date").saveAsTable(
                        "iceberg.gold.product_performance_analytics"
                    )

                    context.log.info(
                        "✅ Successfully saved analytics to iceberg.gold.product_performance_analytics"
                    )

                    # Calculate summary metrics
                    total_revenue = (
                        product_analytics.agg(sum("total_revenue")).collect()[0][0] or 0
                    )
                    total_purchases = (
                        product_analytics.agg(sum("total_purchases")).collect()[0][0]
                        or 0
                    )
                    avg_conversion = (
                        product_analytics.agg(avg("conversion_rate")).collect()[0][0]
                        or 0
                    )

                    # Report comprehensive metadata
                    context.report_asset_materialization(
                        metadata={
                            "processing_date": target_date,
                            "total_products": total_products,
                            "total_purchases": int(total_purchases),
                            "total_revenue": f"${total_revenue:.2f}",
                            "avg_conversion_rate": f"{avg_conversion:.2%}",
                            "table_format": "iceberg",
                            "table_location": "iceberg.gold.product_performance_analytics",
                            "spark_app_name": spark.sparkContext.appName,
                            "spark_app_id": spark.sparkContext.applicationId,
                            "processing_timestamp": datetime.now().isoformat(),
                            "status": "SUCCESS",
                        },
                        data_version=f"{target_date}-v1",
                    )
                else:
                    context.log.warning(
                        "⚠️ No product data found for the specified date"
                    )
                    context.report_asset_materialization(
                        metadata={
                            "processing_date": target_date,
                            "total_products": 0,
                            "status": "SUCCESS_NO_DATA",
                            "processing_timestamp": datetime.now().isoformat(),
                        },
                        data_version=f"{target_date}-v1",
                    )

                context.log.info("Product performance analytics completed successfully")

            except Exception as e:
                context.log.error(f"Error during data processing: {str(e)}")
                context.report_asset_materialization(
                    metadata={
                        "processing_date": target_date,
                        "status": "FAILED",
                        "error_message": str(e),
                        "processing_timestamp": datetime.now().isoformat(),
                    },
                    data_version=f"{target_date}-error",
                )
                raise

            finally:
                context.log.info("🔧 Stopping Spark session")
                spark.stop()

    except ImportError as e:
        print(f"Failed to import dagster_pipes: {e}")
        print("This script requires dagster_pipes to be available")
        sys.exit(1)
    except Exception as e:
        print(f"Error in Pipes execution: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Product Performance Analytics Spark Job"
    )
    parser.add_argument(
        "--target_date",
        default=(datetime.now().strftime("%Y-%m-%d")),
        help="Target date for processing (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    results = process_product_performance_analytics(args.target_date)
    print(f"🎉 Product performance analytics completed successfully: {results}")


if __name__ == "__main__":
    main()
