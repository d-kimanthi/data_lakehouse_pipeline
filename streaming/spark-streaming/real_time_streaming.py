# streaming/spark-streaming/streaming_job.py

import json
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EcommerceStreamProcessor:
    """
    Spark Streaming processor for e-commerce events
    Real-time processing: Kafka → Bronze → Silver (Iceberg tables)
    """

    def __init__(self, app_name: str = "ecommerce-streaming"):
        self.app_name = app_name

        # Detect execution environment (local vs cluster)
        self.is_cluster_mode = self._detect_cluster_mode()
        logger.info(f"Running in {'cluster' if self.is_cluster_mode else 'local'} mode")

        self.spark = self._create_spark_session()
        self._setup_schemas()
        self._create_iceberg_tables()

    def _detect_cluster_mode(self) -> bool:
        """Detect if running in cluster mode by checking for Docker environment"""
        # Check if we're running inside a Docker container
        return os.path.exists("/.dockerenv") or os.environ.get("SPARK_MODE") == "master"

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Iceberg and Kafka configurations"""

        # Configure endpoints based on execution mode
        if self.is_cluster_mode:
            # Use Docker service names for cluster mode
            kafka_bootstrap = "ecommerce-kafka:9092"
            minio_endpoint = "http://ecommerce-minio:9000"
            logger.info("Using cluster networking (Docker service names)")
        else:
            # Use localhost for local mode
            kafka_bootstrap = "localhost:9092"
            minio_endpoint = "http://localhost:9000"
            logger.info("Using local networking (localhost)")

        # Store for later use
        self.kafka_bootstrap = kafka_bootstrap
        self.minio_endpoint = minio_endpoint

        # For local development - using MinIO as S3
        spark = (
            SparkSession.builder.appName(self.app_name)
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.565",
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog"
            )
            .config("spark.sql.catalog.iceberg.type", "hadoop")
            .config(
                "spark.sql.catalog.iceberg.warehouse", "s3a://bronze/iceberg-warehouse/"
            )
            # MinIO configuration - endpoint determined by execution mode
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _setup_schemas(self):
        """Define schemas for different event types"""

        # Page view event schema
        self.page_view_schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("page_type", StringType(), False),
                StructField("referrer", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("ip_address", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

        # Purchase event schema
        self.purchase_schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("order_id", StringType(), False),
                StructField("total_price", DecimalType(10, 2), False),
                StructField("payment_method", StringType(), False),
                StructField("shipping_address", StringType(), True),
                StructField(
                    "products",
                    ArrayType(
                        StructType(
                            [
                                StructField("product_id", StringType(), False),
                                StructField("name", StringType(), False),
                                StructField("price", DecimalType(10, 2), False),
                                StructField("quantity", IntegerType(), False),
                            ]
                        )
                    ),
                    False,
                ),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

    def _create_iceberg_tables(self):
        """Create Iceberg tables for Bronze and Silver layers"""

        # Bronze layer - raw events table
        self.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS iceberg.bronze.raw_events (
                event_id STRING,
                event_type STRING,
                timestamp TIMESTAMP,
                topic STRING,
                partition INT,
                offset BIGINT,
                raw_data STRING,
                processing_time TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (days(timestamp))
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy',
                'write.metadata.delete-after-commit.enabled'='true',
                'write.metadata.previous-versions-max'='3'
            )
        """
        )

        # Silver layer - page views table
        self.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS iceberg.silver.page_views (
                event_id STRING,
                timestamp TIMESTAMP,
                user_id STRING,
                session_id STRING,
                product_id STRING,
                page_type STRING,
                referrer STRING,
                user_agent STRING,
                ip_address STRING,
                device_type STRING,
                metadata MAP<STRING, STRING>,
                processing_time TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (days(timestamp))
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy'
            )
        """
        )

        # Silver layer - purchases table
        self.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS iceberg.silver.purchases (
                event_id STRING,
                timestamp TIMESTAMP,
                user_id STRING,
                session_id STRING,
                order_id STRING,
                total_price DECIMAL(10,2),
                payment_method STRING,
                shipping_address STRING,
                products ARRAY<STRUCT<
                    product_id: STRING,
                    name: STRING,
                    price: DECIMAL(10,2),
                    quantity: INT
                >>,
                metadata MAP<STRING, STRING>,
                processing_time TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (days(timestamp))
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy'
            )
        """
        )

        logger.info("Iceberg tables created successfully")

    def start_streaming(self):
        """Start the streaming job for real-time processing"""

        logger.info("Starting Kafka streaming ingestion...")

        # Read from Kafka
        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap)
            .option("subscribe", "raw-events,page-views,purchases,user-sessions")
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Process Bronze layer - store all raw events
        bronze_df = (
            kafka_df.select(
                col("key").cast("string").alias("event_id"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                col("value").cast("string").alias("raw_data"),
                current_timestamp().alias("processing_time"),
            )
            .withColumn(
                "timestamp",
                to_timestamp(
                    get_json_object(col("raw_data"), "$.timestamp"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
                ),
            )
            .withColumn("event_type", get_json_object(col("raw_data"), "$.event_type"))
        )

        # Write to Bronze Iceberg table
        bronze_query = (
            bronze_df.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", "/tmp/checkpoints/bronze")
            .option("path", "iceberg.bronze.raw_events")
            .trigger(processingTime="30 seconds")
            .start()
        )

        # Process Page Views to Silver layer
        page_views_df = (
            kafka_df.filter(col("topic") == "page-views")
            .select(
                from_json(col("value").cast("string"), self.page_view_schema).alias(
                    "data"
                )
            )
            .select("data.*")
            .withColumn("processing_time", current_timestamp())
        )

        page_views_query = (
            page_views_df.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", "/tmp/checkpoints/silver_page_views")
            .option("path", "iceberg.silver.page_views")
            .trigger(processingTime="1 minute")
            .start()
        )

        # Process Purchases to Silver layer
        purchases_df = (
            kafka_df.filter(col("topic") == "purchases")
            .select(
                from_json(col("value").cast("string"), self.purchase_schema).alias(
                    "data"
                )
            )
            .select("data.*")
            .withColumn("processing_time", current_timestamp())
        )

        purchases_query = (
            purchases_df.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", "/tmp/checkpoints/silver_purchases")
            .option("path", "iceberg.silver.purchases")
            .trigger(processingTime="1 minute")
            .start()
        )

        logger.info("All streaming queries started successfully")

        # Return queries for monitoring
        return [bronze_query, page_views_query, purchases_query]

    def stop_streaming(self, queries):
        """Stop all streaming queries"""
        for query in queries:
            query.stop()

        self.spark.stop()
        logger.info("Streaming stopped")


def main():
    """Main function to run the streaming job"""

    processor = EcommerceStreamProcessor()

    try:
        # Start streaming
        queries = processor.start_streaming()

        # Wait for termination (in production, you'd have proper monitoring)
        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Stopping streaming job...")
        processor.stop_streaming(queries)
    except Exception as e:
        logger.error(f"Streaming job failed: {str(e)}")
        processor.stop_streaming(queries)
        raise


if __name__ == "__main__":
    main()
