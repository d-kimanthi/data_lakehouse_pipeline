"""
Dagster assets for Spark batch processing.
These assets orchestrate Spark jobs for Silver → Gold layer transformations.
"""

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
    get_dagster_logger,
)

# Set up logging
logger = get_dagster_logger()

# Define partitions for daily processing
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

# Path to Spark jobs
SPARK_JOBS_DIR = Path(__file__).parent.parent.parent / "streaming" / "spark-streaming"


@asset(
    description="Daily customer analytics from Silver layer Iceberg tables",
    group_name="gold_layer",
    partitions_def=daily_partitions,
)
def gold_daily_customer_analytics(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run Spark job to create daily customer analytics from Silver layer tables.
    Aggregates page views, purchases, and user behavior into business metrics.
    """
    partition_date = context.partition_key
    logger.info(f"Processing daily customer analytics for {partition_date}")

    try:
        # Submit Spark job for customer analytics
        spark_submit_cmd = [
            "spark-submit",
            "--master",
            "local[*]",
            "--packages",
            (
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.565"
            ),
            str(SPARK_JOBS_DIR / "batch_customer_analytics.py"),
            "--date",
            partition_date,
        ]

        result = subprocess.run(
            spark_submit_cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=str(SPARK_JOBS_DIR),
        )

        logger.info(f"Spark job completed successfully: {result.stdout}")

        return MaterializeResult(
            metadata={
                "partition_date": partition_date,
                "spark_output": result.stdout[-1000:],  # Last 1000 chars
                "records_processed": "extracted_from_spark_logs",  # You'd parse this
                "execution_time_seconds": "extracted_from_spark_logs",
            }
        )

    except subprocess.CalledProcessError as e:
        logger.error(f"Spark job failed: {e.stderr}")
        raise Exception(f"Daily customer analytics failed: {e.stderr}")


@asset(
    description="Daily sales summary and business KPIs from Silver layer",
    group_name="gold_layer",
    partitions_def=daily_partitions,
)
def gold_daily_sales_summary(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run Spark job to create daily sales summary and business KPIs.
    """
    partition_date = context.partition_key
    logger.info(f"Processing daily sales summary for {partition_date}")

    try:
        spark_submit_cmd = [
            "spark-submit",
            "--master",
            "local[*]",
            "--packages",
            (
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.565"
            ),
            str(SPARK_JOBS_DIR / "batch_sales_summary.py"),
            "--date",
            partition_date,
        ]

        result = subprocess.run(
            spark_submit_cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=str(SPARK_JOBS_DIR),
        )

        logger.info(f"Sales summary job completed: {result.stdout}")

        return MaterializeResult(
            metadata={
                "partition_date": partition_date,
                "spark_output": result.stdout[-1000:],
                "total_revenue": "extracted_from_spark_logs",
                "total_orders": "extracted_from_spark_logs",
                "unique_customers": "extracted_from_spark_logs",
            }
        )

    except subprocess.CalledProcessError as e:
        logger.error(f"Sales summary job failed: {e.stderr}")
        raise Exception(f"Daily sales summary failed: {e.stderr}")


@asset(
    description="Product performance analytics from Silver layer data",
    group_name="gold_layer",
    partitions_def=daily_partitions,
)
def gold_product_analytics(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run Spark job to analyze product performance, views, and conversion rates.
    """
    partition_date = context.partition_key
    logger.info(f"Processing product analytics for {partition_date}")

    try:
        spark_submit_cmd = [
            "spark-submit",
            "--master",
            "local[*]",
            "--packages",
            (
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.565"
            ),
            str(SPARK_JOBS_DIR / "batch_product_analytics.py"),
            "--date",
            partition_date,
        ]

        result = subprocess.run(
            spark_submit_cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=str(SPARK_JOBS_DIR),
        )

        logger.info(f"Product analytics completed: {result.stdout}")

        return MaterializeResult(
            metadata={
                "partition_date": partition_date,
                "spark_output": result.stdout[-1000:],
                "products_analyzed": "extracted_from_spark_logs",
                "top_performing_products": "extracted_from_spark_logs",
            }
        )

    except subprocess.CalledProcessError as e:
        logger.error(f"Product analytics failed: {e.stderr}")
        raise Exception(f"Product analytics failed: {e.stderr}")


@asset(
    description="Data quality checks across all Silver layer tables",
    group_name="data_quality",
)
def silver_data_quality_report(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run comprehensive data quality checks on Silver layer Iceberg tables.
    """
    logger.info("Running data quality checks on Silver layer")

    try:
        spark_submit_cmd = [
            "spark-submit",
            "--master",
            "local[*]",
            "--packages",
            (
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.565"
            ),
            str(SPARK_JOBS_DIR / "data_quality_checks.py"),
        ]

        result = subprocess.run(
            spark_submit_cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=str(SPARK_JOBS_DIR),
        )

        logger.info(f"Data quality checks completed: {result.stdout}")

        return MaterializeResult(
            metadata={
                "timestamp": datetime.now().isoformat(),
                "spark_output": result.stdout[-1000:],
                "quality_score": "extracted_from_spark_logs",
                "issues_found": "extracted_from_spark_logs",
                "tables_checked": ["page_views", "purchases", "raw_events"],
            }
        )

    except subprocess.CalledProcessError as e:
        logger.error(f"Data quality checks failed: {e.stderr}")
        # Don't fail the pipeline for quality issues, just log them
        return MaterializeResult(
            metadata={
                "timestamp": datetime.now().isoformat(),
                "status": "failed",
                "error": str(e.stderr),
                "quality_score": 0,
            }
        )


@asset(
    description="Monitor Spark streaming job health and performance",
    group_name="monitoring",
)
def streaming_health_monitor(context: AssetExecutionContext) -> MaterializeResult:
    """
    Check the health of the Spark streaming job and report metrics.
    """
    logger.info("Monitoring Spark streaming job health")

    try:
        # Check if streaming job is running
        # This would typically check Spark UI or application status

        # For now, simulate health check
        health_metrics = {
            "streaming_job_status": "running",  # You'd check this from Spark
            "last_batch_time": datetime.now().isoformat(),
            "records_per_second": 1000,  # Mock data
            "lag_seconds": 5,
            "error_rate": 0.01,
        }

        return MaterializeResult(
            metadata={
                "timestamp": datetime.now().isoformat(),
                "health_status": "healthy",
                **health_metrics,
            }
        )

    except Exception as e:
        logger.error(f"Health monitoring failed: {str(e)}")
        return MaterializeResult(
            metadata={
                "timestamp": datetime.now().isoformat(),
                "health_status": "unhealthy",
                "error": str(e),
            }
        )
