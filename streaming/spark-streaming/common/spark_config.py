"""
Common Spark session configuration for e-commerce analytics jobs.
This module provides a DRY way to create Spark sessions with consistent
Iceberg, Nessie, and MinIO configurations.

Runs in cluster mode only using Docker service names for networking.
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkConfig:
    """Configuration manager for Spark sessions with Iceberg and Nessie"""

    # Default jar packages for all Spark jobs
    DEFAULT_PACKAGES = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.0"
    )

    KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

    def __init__(self):
        """Initialize Spark configuration for cluster mode."""
        # Cluster mode endpoints (Docker service names)
        self.kafka_bootstrap = "kafka:29092"
        self.minio_endpoint = "http://ecommerce-minio:9000"
        self.nessie_uri = "http://nessie:19120/api/v1"
        logger.info("Spark configuration initialized for cluster mode")

    def create_session(
        self,
        app_name: str,
        include_kafka: bool = False,
        additional_configs: Optional[dict] = None,
    ) -> SparkSession:
        """
        Create a configured Spark session with Iceberg and Nessie support.

        Args:
            app_name: Name for the Spark application
            include_kafka: Whether to include Kafka packages
            additional_configs: Additional Spark configurations to apply

        Returns:
            Configured SparkSession
        """
        # Build packages list
        packages = self.DEFAULT_PACKAGES
        if include_kafka:
            packages = f"{self.KAFKA_PACKAGE},{packages}"

        # Create builder with common configurations
        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.jars.packages", packages)
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
            .config("spark.sql.catalog.iceberg.uri", self.nessie_uri)
            .config("spark.sql.catalog.iceberg.ref", "main")
            .config("spark.sql.catalog.iceberg.warehouse", "s3a://data-lake/warehouse/")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

        # Apply additional configurations if provided
        if additional_configs:
            for key, value in additional_configs.items():
                builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"Spark session created: {app_name}")
        return spark


def create_spark_session(
    app_name: str,
    include_kafka: bool = False,
    additional_configs: Optional[dict] = None,
) -> SparkSession:
    """
    Convenience function to create a Spark session with standard configurations.

    Args:
        app_name: Name for the Spark application
        include_kafka: Whether to include Kafka packages
        additional_configs: Additional Spark configurations

    Returns:
        Configured SparkSession
    """
    config = SparkConfig()
    return config.create_session(
        app_name=app_name,
        include_kafka=include_kafka,
        additional_configs=additional_configs,
    )
