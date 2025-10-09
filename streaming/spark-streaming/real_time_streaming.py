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

    def __init__(
        self, app_name: str = "ecommerce-streaming", force_cluster_mode: bool = False
    ):
        self.app_name = app_name

        # Detect execution environment (local vs cluster)
        if force_cluster_mode:
            self.is_cluster_mode = True
            logger.info("Cluster mode forced via command line argument")
        else:
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
            # Use Docker service names for cluster mode (internal port)
            kafka_bootstrap = "kafka:29092"
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

        # Determine Nessie endpoint based on execution mode
        if self.is_cluster_mode:
            nessie_uri = "http://nessie:19120/api/v1"
        else:
            nessie_uri = "http://localhost:19120/api/v1"

        # For local development - using MinIO as S3 and Nessie catalog
        spark = (
            SparkSession.builder.appName(self.app_name)
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
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
            .config("spark.sql.catalog.iceberg.uri", nessie_uri)
            .config("spark.sql.catalog.iceberg.ref", "main")
            .config("spark.sql.catalog.iceberg.warehouse", "s3a://data-lake/warehouse/")
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
                StructField("total_amount", DecimalType(10, 2), False),
                StructField("subtotal", DecimalType(10, 2), False),
                StructField("discount_percent", IntegerType(), False),
                StructField("discount_amount", DecimalType(10, 2), False),
                StructField("payment_method", StringType(), False),
                StructField("shipping_method", StringType(), False),
                StructField(
                    "shipping_address",
                    StructType(
                        [
                            StructField("street", StringType(), True),
                            StructField("city", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("zip_code", StringType(), True),
                            StructField("country", StringType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("product_id", StringType(), False),
                                StructField("quantity", IntegerType(), False),
                                StructField("unit_price", DecimalType(10, 2), False),
                                StructField("total_price", DecimalType(10, 2), False),
                                StructField("category", StringType(), False),
                                StructField("brand", StringType(), False),
                            ]
                        )
                    ),
                    False,
                ),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

        # Add to cart event schema
        self.add_to_cart_schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("price", DecimalType(10, 2), False),
                StructField("total_value", DecimalType(10, 2), False),
                StructField("cart_id", StringType(), False),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

        # User session event schema
        self.user_session_schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("session_type", StringType(), False),
                StructField("device_type", StringType(), False),
                StructField("ip_address", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

        # Product update event schema
        self.product_update_schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("product_id", StringType(), False),
                StructField("update_type", StringType(), False),
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
                total_amount DECIMAL(10,2),
                subtotal DECIMAL(10,2),
                discount_percent INT,
                discount_amount DECIMAL(10,2),
                payment_method STRING,
                shipping_method STRING,
                shipping_address STRUCT<
                    street: STRING,
                    city: STRING,
                    state: STRING,
                    zip_code: STRING,
                    country: STRING
                >,
                items ARRAY<STRUCT<
                    product_id: STRING,
                    quantity: INT,
                    unit_price: DECIMAL(10,2),
                    total_price: DECIMAL(10,2),
                    category: STRING,
                    brand: STRING
                >>,
                metadata MAP<STRING, STRING>,
                processing_time TIMESTAMP
            ) USING iceberg
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy'
            )
        """
        )

        # Silver layer - cart events table
        self.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS iceberg.silver.cart_events (
                event_id STRING,
                timestamp TIMESTAMP,
                user_id STRING,
                session_id STRING,
                product_id STRING,
                quantity INT,
                price DECIMAL(10,2),
                total_value DECIMAL(10,2),
                cart_id STRING,
                metadata MAP<STRING, STRING>,
                processing_time TIMESTAMP
            ) USING iceberg
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy'
            )
        """
        )

        # Silver layer - user sessions table
        self.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS iceberg.silver.user_sessions (
                event_id STRING,
                timestamp TIMESTAMP,
                user_id STRING,
                session_id STRING,
                session_type STRING,
                device_type STRING,
                ip_address STRING,
                user_agent STRING,
                metadata MAP<STRING, STRING>,
                processing_time TIMESTAMP
            ) USING iceberg
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy'
            )
        """
        )

        # Silver layer - product updates table
        self.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS iceberg.silver.product_updates (
                event_id STRING,
                timestamp TIMESTAMP,
                product_id STRING,
                update_type STRING,
                metadata MAP<STRING, STRING>,
                processing_time TIMESTAMP
            ) USING iceberg
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy'
            )
        """
        )

        logger.info("Iceberg tables created successfully")

    def start_kafka_to_bronze_ingestion(self):
        """Job 1: Kafka → Bronze (real-time ingestion)
        Minimal processing - just capture raw events with metadata"""

        logger.info("Starting Job 1: Kafka → Bronze ingestion...")

        # Read from Kafka
        kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_bootstrap)
            .option(
                "subscribe",
                "raw-events,page-views,purchases,order-events,user-sessions,product-updates",
            )
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Bronze processing - minimal transformation, preserve raw data
        bronze_df = kafka_df.select(
            # Extract event_id from key or JSON (for partitioning)
            coalesce(
                col("key").cast("string"),
                get_json_object(col("value").cast("string"), "$.event_id"),
                concat(lit("kafka_"), col("offset").cast("string")),
            ).alias("event_id"),
            # Extract event_type for basic categorization
            coalesce(
                get_json_object(col("value").cast("string"), "$.event_type"),
                lit("unknown"),
            ).alias("event_type"),
            # Extract timestamp from JSON, fallback to Kafka timestamp
            coalesce(
                to_timestamp(
                    get_json_object(col("value").cast("string"), "$.timestamp")
                ),
                col("timestamp"),
            ).alias("timestamp"),
            # Kafka metadata for lineage
            col("topic"),
            col("partition"),
            col("offset"),
            # Raw data - complete JSON payload
            col("value").cast("string").alias("raw_data"),
            # Processing metadata
            current_timestamp().alias("processing_time"),
        )

        logger.info("Bronze ingestion DataFrame schema:")
        bronze_df.printSchema()

        # Write to Bronze Iceberg table with better error handling
        bronze_query = (
            bronze_df.writeStream.format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", "s3a://data-lake/checkpoints/bronze")
            .option("path", "iceberg.bronze.raw_events")
            .trigger(processingTime="30 seconds")
            .start()
        )

        logger.info("Job 1: Kafka → Bronze ingestion started successfully")
        return bronze_query

    def start_bronze_to_silver_processing(self):
        """Job 2: Bronze → Silver (real-time processing)
        Parse JSON, apply business rules, and create clean silver tables"""

        logger.info("Starting Job 2: Bronze → Silver processing...")

        # Read from Bronze Iceberg table
        bronze_df = (
            self.spark.readStream.format("iceberg")
            .option("path", "iceberg.bronze.raw_events")
            .option("startingOffsets", "latest")
            .load()
        )

        logger.info("Bronze source DataFrame schema:")
        bronze_df.printSchema()

        # Parse and route events by type
        queries = []

        # Process Page Views
        page_views_df = (
            bronze_df.filter(col("event_type") == "page_view")
            .select(
                col("event_id"),
                from_json(col("raw_data"), self.page_view_schema).alias("parsed_event"),
                col("processing_time").alias("bronze_processing_time"),
                current_timestamp().alias("silver_processing_time"),
            )
            .select(
                col("event_id"),
                col("parsed_event.timestamp").alias("timestamp"),
                col("parsed_event.user_id"),
                col("parsed_event.session_id"),
                col("parsed_event.product_id"),
                col("parsed_event.page_type"),
                col("parsed_event.referrer"),
                col("parsed_event.user_agent"),
                col("parsed_event.ip_address"),
                col("parsed_event.device_type"),
                col("parsed_event.metadata"),
                col("silver_processing_time").alias("processing_time"),
            )
            .filter(col("user_id").isNotNull())  # Data quality check
        )

        page_views_query = (
            page_views_df.writeStream.format("iceberg")
            .outputMode("append")
            .option(
                "checkpointLocation", "s3a://data-lake/checkpoints/silver_page_views"
            )
            .option("path", "iceberg.silver.page_views")
            .trigger(processingTime="1 minute")
            .start()
        )
        queries.append(page_views_query)

        # Process Purchases
        purchases_df = (
            bronze_df.filter(col("event_type") == "purchase")
            .select(
                col("event_id"),
                from_json(col("raw_data"), self.purchase_schema).alias("parsed_event"),
                current_timestamp().alias("processing_time"),
            )
            .select(
                col("event_id"),
                col("parsed_event.timestamp").alias("timestamp"),
                col("parsed_event.user_id"),
                col("parsed_event.session_id"),
                col("parsed_event.order_id"),
                col("parsed_event.total_amount"),
                col("parsed_event.subtotal"),
                col("parsed_event.discount_percent"),
                col("parsed_event.discount_amount"),
                col("parsed_event.payment_method"),
                col("parsed_event.shipping_method"),
                col("parsed_event.shipping_address"),
                col("parsed_event.items"),
                col("parsed_event.metadata"),
                col("processing_time"),
            )
            .filter(
                col("user_id").isNotNull() & col("total_amount").isNotNull()
            )  # Data quality
        )

        purchases_query = (
            purchases_df.writeStream.format("iceberg")
            .outputMode("append")
            .option(
                "checkpointLocation", "s3a://data-lake/checkpoints/silver_purchases"
            )
            .option("path", "iceberg.silver.purchases")
            .trigger(processingTime="1 minute")
            .start()
        )
        queries.append(purchases_query)

        # Process Cart Events
        cart_events_df = (
            bronze_df.filter(col("event_type") == "add_to_cart")
            .select(
                col("event_id"),
                from_json(col("raw_data"), self.add_to_cart_schema).alias(
                    "parsed_event"
                ),
                current_timestamp().alias("processing_time"),
            )
            .select(
                col("event_id"),
                col("parsed_event.timestamp").alias("timestamp"),
                col("parsed_event.user_id"),
                col("parsed_event.session_id"),
                col("parsed_event.product_id"),
                col("parsed_event.quantity"),
                col("parsed_event.price"),
                col("parsed_event.total_value"),
                col("parsed_event.cart_id"),
                col("parsed_event.metadata"),
                col("processing_time"),
            )
            .filter((col("user_id").isNotNull()) & (col("quantity") > 0))
        )

        cart_events_query = (
            cart_events_df.writeStream.format("iceberg")
            .outputMode("append")
            .option(
                "checkpointLocation", "s3a://data-lake/checkpoints/silver_cart_events"
            )
            .option("path", "iceberg.silver.cart_events")
            .trigger(processingTime="1 minute")
            .start()
        )
        queries.append(cart_events_query)

        # Process User Sessions
        user_sessions_df = (
            bronze_df.filter(col("event_type") == "user_session")
            .select(
                col("event_id"),
                from_json(col("raw_data"), self.user_session_schema).alias(
                    "parsed_event"
                ),
                current_timestamp().alias("processing_time"),
            )
            .select(
                col("event_id"),
                col("parsed_event.timestamp").alias("timestamp"),
                col("parsed_event.user_id"),
                col("parsed_event.session_id"),
                col("parsed_event.session_type"),
                col("parsed_event.device_type"),
                col("parsed_event.ip_address"),
                col("parsed_event.user_agent"),
                col("parsed_event.metadata"),
                col("processing_time"),
            )
            .filter(col("user_id").isNotNull())  # Data quality
        )

        user_sessions_query = (
            user_sessions_df.writeStream.format("iceberg")
            .outputMode("append")
            .option(
                "checkpointLocation", "s3a://data-lake/checkpoints/silver_user_sessions"
            )
            .option("path", "iceberg.silver.user_sessions")
            .trigger(processingTime="1 minute")
            .start()
        )
        queries.append(user_sessions_query)

        # Process Product Updates
        product_updates_df = (
            bronze_df.filter(col("event_type") == "product_update")
            .select(
                col("event_id"),
                from_json(col("raw_data"), self.product_update_schema).alias(
                    "parsed_event"
                ),
                current_timestamp().alias("processing_time"),
            )
            .select(
                col("event_id"),
                col("parsed_event.timestamp").alias("timestamp"),
                col("parsed_event.product_id"),
                col("parsed_event.update_type"),
                col("parsed_event.metadata"),
                col("processing_time"),
            )
            .filter(col("product_id").isNotNull())  # Data quality
        )

        product_updates_query = (
            product_updates_df.writeStream.format("iceberg")
            .outputMode("append")
            .option(
                "checkpointLocation",
                "s3a://data-lake/checkpoints/silver_product_updates",
            )
            .option("path", "iceberg.silver.product_updates")
            .trigger(processingTime="1 minute")
            .start()
        )
        queries.append(product_updates_query)

        logger.info("Job 2: Bronze → Silver processing started successfully")
        return queries

    def start_streaming(self, job_type="both"):
        """Start streaming jobs based on job_type

        Args:
            job_type: 'ingestion', 'processing', or 'both'
        """

        queries = []

        if job_type in ["ingestion", "both"]:
            bronze_query = self.start_kafka_to_bronze_ingestion()
            queries.append(bronze_query)

        if job_type in ["processing", "both"]:
            silver_queries = self.start_bronze_to_silver_processing()
            queries.extend(silver_queries)

        return queries

    def stop_streaming(self, queries):
        """Stop all streaming queries"""
        for query in queries:
            query.stop()

        self.spark.stop()
        logger.info("Streaming stopped")


def main():
    """Main function to run the streaming job with job type selection"""
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="E-commerce Streaming Processor")
    parser.add_argument(
        "--cluster", action="store_true", help="Force cluster mode execution"
    )
    parser.add_argument(
        "--job-type",
        choices=["ingestion", "processing", "both"],
        default="both",
        help="Type of job to run: 'ingestion' (Kafka→Bronze), 'processing' (Bronze→Silver), or 'both'",
    )

    args = parser.parse_args()

    logger.info(f"Starting {args.job_type} job...")

    processor = EcommerceStreamProcessor(force_cluster_mode=args.cluster)

    try:
        # Start streaming based on job type
        queries = processor.start_streaming(job_type=args.job_type)

        logger.info(f"Started {len(queries)} streaming queries")

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
