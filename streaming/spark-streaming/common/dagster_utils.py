"""
Common utilities for Dagster Pipes integration.
Provides reusable patterns for logging, error handling, and metadata reporting.
"""

import sys
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from pyspark.sql import SparkSession


@contextmanager
def dagster_pipes_context():
    """
    Context manager for Dagster Pipes integration with proper error handling.

    Usage:
        with dagster_pipes_context() as context:
            context.log.info("Processing started")
            # Your processing logic here
    """
    try:
        from dagster_pipes import open_dagster_pipes

        with open_dagster_pipes() as context:
            yield context
    except ImportError as e:
        print(f"Failed to import dagster_pipes: {e}")
        print("This script requires dagster_pipes to be available")
        sys.exit(1)
    except Exception as e:
        print(f"Error in Pipes execution: {e}")
        sys.exit(1)


def report_success_metadata(
    context,
    target_date: str,
    spark: SparkSession,
    custom_metrics: Optional[Dict[str, Any]] = None,
    table_location: Optional[str] = None,
) -> None:
    """
    Report standardized success metadata to Dagster.

    Args:
        context: Dagster Pipes context
        target_date: Processing date
        spark: SparkSession instance
        custom_metrics: Additional metrics to include
        table_location: Iceberg table location
    """
    metadata = {
        "processing_date": target_date,
        "table_format": "iceberg",
        "spark_app_name": spark.sparkContext.appName,
        "spark_app_id": spark.sparkContext.applicationId,
        "processing_timestamp": datetime.now().isoformat(),
        "status": "SUCCESS",
    }

    if table_location:
        metadata["table_location"] = table_location

    if custom_metrics:
        metadata.update(custom_metrics)

    context.report_asset_materialization(
        metadata=metadata, data_version=f"{target_date}-v1"
    )


def report_error_metadata(
    context,
    target_date: str,
    error: Exception,
    additional_context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Report standardized error metadata to Dagster.

    Args:
        context: Dagster Pipes context
        target_date: Processing date
        error: Exception that occurred
        additional_context: Additional context information
    """
    metadata = {
        "processing_date": target_date,
        "status": "FAILED",
        "error_message": str(error),
        "error_type": type(error).__name__,
        "processing_timestamp": datetime.now().isoformat(),
    }

    if additional_context:
        metadata.update(additional_context)

    context.report_asset_materialization(
        metadata=metadata, data_version=f"{target_date}-error"
    )


def report_no_data_metadata(
    context, target_date: str, table_location: Optional[str] = None
) -> None:
    """
    Report metadata when no data is found for processing.

    Args:
        context: Dagster Pipes context
        target_date: Processing date
        table_location: Iceberg table location
    """
    metadata = {
        "processing_date": target_date,
        "status": "SUCCESS_NO_DATA",
        "processing_timestamp": datetime.now().isoformat(),
    }

    if table_location:
        metadata["table_location"] = table_location

    context.report_asset_materialization(
        metadata=metadata, data_version=f"{target_date}-v1"
    )


def execute_with_pipes(process_func: Callable, target_date: str, **kwargs) -> None:
    """
    Execute a processing function with Dagster Pipes integration and error handling.

    Args:
        process_func: Function to execute (should accept context and target_date)
        target_date: Date to process
        **kwargs: Additional arguments to pass to process_func
    """
    try:
        with dagster_pipes_context() as context:
            context.log.info(f"Starting processing for date: {target_date}")
            process_func(context, target_date, **kwargs)
            context.log.info("Processing completed successfully")
    except Exception as e:
        print(f"Fatal error in processing: {str(e)}")
        import traceback

        print(traceback.format_exc())
        sys.exit(1)


def log_dataframe_summary(context, df, description: str = "DataFrame") -> None:
    """
    Log a summary of a DataFrame including count and sample rows.

    Args:
        context: Dagster Pipes context
        df: PySpark DataFrame
        description: Description of the DataFrame
    """
    count = df.count()
    context.log.info(f" {description}: {count} rows")

    if count > 0:
        context.log.info(f"Sample {description}:")
        df.show(5, truncate=False)
    else:
        context.log.warning(f"{description} is empty")

    return count


def check_silver_tables(context, spark: SparkSession) -> None:
    """
    Check and log available silver layer tables.

    Args:
        context: Dagster Pipes context
        spark: SparkSession instance
    """
    context.log.info("🔍 Checking available silver layer data")

    try:
        databases = spark.sql("SHOW DATABASES").collect()
        context.log.info(f"Available databases: {[row[0] for row in databases]}")

        try:
            silver_tables = spark.sql("SHOW TABLES IN iceberg.silver").collect()
            context.log.info(f"Silver tables: {[row[1] for row in silver_tables]}")
        except Exception as e:
            context.log.warning(f"Could not list silver tables: {e}")
    except Exception as e:
        context.log.warning(f"Error checking catalog: {e}")
