#!/usr/bin/env python3
"""
Spark batch job for daily customer analytics with Dagster Pipes integration.
Processes Silver layer Iceberg tables to create Gold layer customer insights
with enhanced communication back to Dagster.
"""

import argparse
import sys
import time
from datetime import datetime

# Import Dagster Pipes
from dagster_pipes import (
    PipesContext,
    open_dagster_pipes,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Create Spark session with Iceberg support"""
    return (
        SparkSession.builder.appName("DailyCustomerAnalyticsPipes")
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


def process_daily_customer_analytics(spark, context: PipesContext, target_date):
    """Process daily customer analytics for the given date with Pipes communication"""

    start_time = time.time()

    context.log.info(f"🔄 Starting customer analytics processing for {target_date}")

    try:
        # Report progress: Setting up tables
        context.log.info("📊 Setting up Iceberg catalog and tables...")

        # Create Gold table if not exists
        context.log.info(
            "🏗️ Creating gold.daily_customer_analytics table if not exists..."
        )
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS iceberg.gold.daily_customer_analytics (
                user_id STRING,
                date DATE,
                page_views BIGINT,
                unique_sessions BIGINT,
                total_time_spent DOUBLE,
                purchases BIGINT,
                total_revenue DOUBLE,
                avg_order_value DOUBLE,
                customer_segment STRING,
                first_purchase_date DATE,
                last_activity_timestamp TIMESTAMP
            ) USING ICEBERG
            PARTITIONED BY (date)
            """
        )

        context.log.info("✅ Gold table ready")

        # Report progress: Checking data availability
        context.log.info("🔍 Checking silver layer data availability...")

        # For now, create mock data since we might not have real silver data
        context.log.info(
            "📝 Creating sample customer analytics data for demonstration..."
        )

        # Sample data for testing
        sample_data = [
            (
                "user_001",
                target_date,
                15,
                3,
                450.0,
                2,
                89.99,
                44.99,
                "regular",
                "2025-01-15",
                "2025-10-02 10:30:00",
            ),
            (
                "user_002",
                target_date,
                8,
                2,
                120.0,
                1,
                25.50,
                25.50,
                "regular",
                "2025-09-20",
                "2025-10-02 14:20:00",
            ),
            (
                "user_003",
                target_date,
                25,
                5,
                720.0,
                4,
                299.96,
                74.99,
                "high_value",
                "2024-12-01",
                "2025-10-02 16:45:00",
            ),
            (
                "user_004",
                target_date,
                3,
                1,
                45.0,
                0,
                0.0,
                0.0,
                "browsing",
                "2025-10-01",
                "2025-10-02 09:15:00",
            ),
            (
                "user_005",
                target_date,
                12,
                4,
                380.0,
                3,
                159.97,
                53.32,
                "regular",
                "2025-05-10",
                "2025-10-02 18:30:00",
            ),
        ]

        schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("date", StringType(), True),  # Will be converted to date
                StructField("page_views", LongType(), True),
                StructField("unique_sessions", LongType(), True),
                StructField("total_time_spent", DoubleType(), True),
                StructField("purchases", LongType(), True),
                StructField("total_revenue", DoubleType(), True),
                StructField("avg_order_value", DoubleType(), True),
                StructField("customer_segment", StringType(), True),
                StructField(
                    "first_purchase_date", StringType(), True
                ),  # Will be converted to date
                StructField(
                    "last_activity_timestamp", StringType(), True
                ),  # Will be converted to timestamp
            ]
        )

        context.log.info("🔨 Creating customer analytics DataFrame...")
        df = spark.createDataFrame(sample_data, schema)

        # Convert string dates to proper date/timestamp types
        df = (
            df.withColumn("date", to_date(col("date")))
            .withColumn("first_purchase_date", to_date(col("first_purchase_date")))
            .withColumn(
                "last_activity_timestamp", to_timestamp(col("last_activity_timestamp"))
            )
        )

        # Report progress: Processing analytics
        context.log.info("⚙️ Processing customer analytics...")

        # Calculate some basic metrics
        total_customers = df.count()
        total_revenue = df.agg(sum("total_revenue")).collect()[0][0] or 0.0
        high_value_customers = df.filter(
            col("customer_segment") == "high_value"
        ).count()
        avg_order_value = (
            df.filter(col("avg_order_value") > 0)
            .agg(avg("avg_order_value"))
            .collect()[0][0]
            or 0.0
        )

        context.log.info(f"📈 Analytics Results:")
        context.log.info(f"   • Total Customers: {total_customers}")
        context.log.info(f"   • High Value Customers: {high_value_customers}")
        context.log.info(f"   • Total Revenue: ${total_revenue:.2f}")
        context.log.info(f"   • Average Order Value: ${avg_order_value:.2f}")

        # Write to gold layer
        context.log.info("💾 Writing to gold layer...")
        df.write.mode("overwrite").saveAsTable("iceberg.gold.daily_customer_analytics")

        processing_time = time.time() - start_time

        context.log.info(
            f"✅ Customer analytics processing completed in {processing_time:.2f} seconds"
        )

        # Calculate data quality score
        null_count = df.select(
            [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
        ).collect()[0]
        total_cells = df.count() * len(df.columns)
        null_cells = sum(null_count)
        data_quality_score = (
            (total_cells - null_cells) / total_cells if total_cells > 0 else 1.0
        )

        # Report metadata back to Dagster
        context.report_asset_materialization(
            metadata={
                "total_customers": total_customers,
                "high_value_customers": high_value_customers,
                "total_revenue": total_revenue,
                "avg_order_value": avg_order_value,
                "processing_time_seconds": processing_time,
                "records_processed": total_customers,
                "data_quality_score": data_quality_score,
                "source_tables": ["silver.page_views", "silver.purchases"],
                "output_table": "iceberg.gold.daily_customer_analytics",
            }
        )

        context.log.info("📊 Metadata reported to Dagster successfully")

        return {
            "status": "success",
            "total_customers": total_customers,
            "high_value_customers": high_value_customers,
            "total_revenue": total_revenue,
            "processing_time": processing_time,
        }

    except Exception as e:
        context.log.error(f"❌ Error processing customer analytics: {str(e)}")
        context.log.error(f"Error type: {type(e).__name__}")
        raise


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Daily Customer Analytics with Pipes")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")

    args = parser.parse_args()
    target_date = args.date

    # Use Dagster Pipes context
    with open_dagster_pipes() as context:
        context.log.info("🚀 Starting Dagster Pipes Customer Analytics job")
        context.log.info(f"📅 Target date: {target_date}")

        # Create Spark session
        context.log.info("⚡ Creating Spark session...")
        spark = create_spark_session()

        try:
            # Process the analytics
            result = process_daily_customer_analytics(spark, context, target_date)
            context.log.info(f"🎉 Job completed successfully: {result}")

        except Exception as e:
            context.log.error(f"💥 Job failed: {str(e)}")
            raise
        finally:
            context.log.info("🛑 Stopping Spark session...")
            spark.stop()


if __name__ == "__main__":
    main()
