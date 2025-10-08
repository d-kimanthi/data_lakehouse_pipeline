"""
Full Pipes-enabled Spark script for customer analytics processing.
This demonstrates enhanced observability and communication with Dagster.
"""

import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

try:
    from dagster_pipes import open_dagster_pipes

    def main():
        parser = argparse.ArgumentParser(description="Process customer analytics data")
        parser.add_argument(
            "--target_date",
            default=(datetime.now().strftime("%Y-%m-%d")),
            help="Target date for processing (YYYY-MM-DD)",
        )
        args = parser.parse_args()

        with open_dagster_pipes() as context:
            context.log.info(
                "🚀 Starting Dagster Pipes-enabled Spark customer analytics"
            )
            context.log.info(f"📅 Processing date: {args.target_date}")

            # Initialize Spark Session with enhanced configuration
            context.log.info(
                "⚙️ Initializing Spark session with Iceberg and S3 configuration"
            )

            spark = (
                SparkSession.builder.appName(f"CustomerAnalytics-{args.target_date}")
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.iceberg.spark.SparkSessionCatalog",
                )
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config(
                    "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
                )
                .config("spark.sql.catalog.iceberg.type", "hadoop")
                .config(
                    "spark.sql.catalog.iceberg.warehouse", "s3a://datalake/warehouse"
                )
                .config(
                    "spark.sql.catalog.iceberg.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO",
                )
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config(
                    "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
                .getOrCreate()
            )

            context.log.info("✅ Spark session initialized successfully")

            try:
                # Check if silver tables exist
                context.log.info("🔍 Checking for available silver layer tables")

                # Try to read from silver tables
                silver_path = "s3a://datalake/silver"

                try:
                    # List available data
                    spark.sql("SHOW DATABASES").show()
                    context.log.info("📊 Available databases listed")

                    # Create sample data for demonstration
                    context.log.info("🎯 Creating sample customer analytics data")

                    sample_data = [
                        ("customer_1", args.target_date, 150.00, 3, "premium"),
                        ("customer_2", args.target_date, 89.50, 2, "standard"),
                        ("customer_3", args.target_date, 320.75, 5, "premium"),
                        ("customer_4", args.target_date, 45.20, 1, "basic"),
                        ("customer_5", args.target_date, 200.00, 4, "standard"),
                    ]

                    schema = [
                        "customer_id",
                        "date",
                        "total_revenue",
                        "order_count",
                        "tier",
                    ]
                    df = spark.createDataFrame(sample_data, schema)

                    context.log.info(
                        f"📈 Created sample dataset with {df.count()} customer records"
                    )

                    # Perform analytics
                    context.log.info("📊 Computing customer analytics metrics")

                    analytics_df = df.groupBy("tier").agg(
                        count("customer_id").alias("customer_count"),
                        sum("total_revenue").alias("total_revenue"),
                        avg("total_revenue").alias("avg_revenue"),
                        sum("order_count").alias("total_orders"),
                    )

                    context.log.info("📋 Customer analytics results:")
                    results = analytics_df.collect()

                    total_customers = 0
                    total_revenue = 0.0

                    for row in results:
                        tier = row["tier"]
                        customers = row["customer_count"]
                        revenue = row["total_revenue"]
                        avg_rev = row["avg_revenue"]
                        orders = row["total_orders"]

                        total_customers += customers
                        total_revenue += revenue

                        context.log.info(
                            f"  🎯 Tier {tier}: {customers} customers, ${revenue:.2f} revenue, ${avg_rev:.2f} avg, {orders} orders"
                        )

                    # Try to save to gold layer
                    context.log.info("💾 Attempting to save results to gold layer")

                    gold_path = f"s3a://data-lake/gold/customer_analytics/date={args.target_date}"

                    # Save as Parquet first (simpler, more reliable)
                    analytics_df.write.mode("overwrite").parquet(gold_path)

                    context.log.info(f"✅ Successfully saved analytics to {gold_path}")

                    # Try to create the Iceberg table (optional)
                    try:
                        context.log.info("🔄 Attempting to create/update Iceberg table")
                        analytics_df.write.mode("overwrite").option(
                            "path", gold_path
                        ).saveAsTable("iceberg.gold.customer_analytics_daily")
                        context.log.info("✅ Iceberg table created successfully")
                    except Exception as iceberg_error:
                        context.log.warning(
                            f"⚠️ Iceberg table creation failed: {str(iceberg_error)}"
                        )
                        context.log.info(
                            "📋 Data saved as Parquet, continuing without Iceberg table"
                        )

                    # Report comprehensive metadata
                    context.report_asset_materialization(
                        metadata={
                            "processing_date": args.target_date,
                            "total_customers_processed": total_customers,
                            "total_revenue_processed": f"${total_revenue:.2f}",
                            "avg_revenue_per_customer": f"${total_revenue/total_customers:.2f}",
                            "output_path": gold_path,
                            "spark_app_name": spark.sparkContext.appName,
                            "spark_app_id": spark.sparkContext.applicationId,
                            "processing_timestamp": datetime.now().isoformat(),
                            "records_written": total_customers,
                            "status": "SUCCESS",
                        },
                        data_version=f"{args.target_date}-v1",
                    )

                    context.log.info(
                        "🎉 Customer analytics processing completed successfully"
                    )

                except Exception as e:
                    context.log.error(f"❌ Error during data processing: {str(e)}")

                    # Still report the attempt with error metadata
                    context.report_asset_materialization(
                        metadata={
                            "processing_date": args.target_date,
                            "status": "FAILED",
                            "error_message": str(e),
                            "processing_timestamp": datetime.now().isoformat(),
                        },
                        data_version=f"{args.target_date}-error",
                    )

                    raise

            finally:
                context.log.info("🔧 Stopping Spark session")
                spark.stop()

    if __name__ == "__main__":
        main()

except ImportError as e:
    print(f"❌ Failed to import dagster_pipes: {e}")
    print("This script requires dagster_pipes to be available")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error in Pipes execution: {e}")
    sys.exit(1)
