"""
Dagster assets for silver to gold layer batch processing using Pipes.

This module contains asset definitions that use Dagster Pipes for enhanced
observability and communication with Spark jobs.
"""

import os
import subprocess
from datetime import datetime, timedelta

from dagster import (
    AssetExecutionContext,
    PipesSubprocessClient,
    asset,
    get_dagster_logger,
)


@asset(
    key_prefix=["gold", "analytics"],
    description="Daily customer analytics aggregations using Dagster Pipes",
    group_name="customer_analytics",
)
def daily_customer_analytics(
    context: AssetExecutionContext,
    pipes_subprocess_client: PipesSubprocessClient,
):
    """
    Process daily customer analytics data from silver to gold layer using Dagster Pipes.

    This asset uses Dagster Pipes to execute a Spark job with enhanced observability,
    providing real-time logging and structured metadata reporting.
    """
    logger = get_dagster_logger()

    # Calculate target date (yesterday)
    target_date = (
        "2025-09-20"  # (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    )

    logger.info(f"Starting Pipes-enabled customer analytics for date: {target_date}")

    # Enhanced Spark command with full Pipes integration
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4",
        "--conf",
        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog",
        "--conf",
        "spark.sql.catalog.spark_catalog.type=hive",
        "--conf",
        "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
        "--conf",
        "spark.sql.catalog.iceberg.type=hadoop",
        "--conf",
        f"spark.sql.catalog.iceberg.warehouse=s3a://data-lake/warehouse",
        "--conf",
        "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
        "--conf",
        "spark.hadoop.fs.s3a.access.key=minioadmin",
        "--conf",
        "spark.hadoop.fs.s3a.secret.key=minioadmin",
        "--conf",
        "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf",
        "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "/opt/dagster/app/streaming/spark-streaming/batch_customer_analytics.py",
        "--target_date",
        target_date,
    ]

    logger.info(f"Executing Pipes command: {' '.join(cmd)}")

    # Set up environment variables
    env = {
        "JAVA_HOME": "/usr/lib/jvm/java-21-openjdk-arm64",
        "SPARK_HOME": "/opt/spark",
        "PYSPARK_PYTHON": "python3",
        "PYSPARK_DRIVER_PYTHON": "python3",
    }

    # Execute the command using Dagster Pipes
    result = pipes_subprocess_client.run(
        command=cmd, context=context, env=env, cwd="/opt/dagster/app"
    )

    # Get structured results from the Pipes execution
    materialization_result = result.get_materialize_result()

    logger.info("Pipes execution completed successfully")
    logger.info(f"Asset materialization metadata: {materialization_result.metadata}")

    return materialization_result


# Export assets for use in definitions
pipes_assets = [
    daily_customer_analytics,
]
