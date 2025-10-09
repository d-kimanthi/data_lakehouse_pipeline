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
        self.spark = self._create_spark_session()
        self._setup_schemas()
        self._create_iceberg_tables()

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Iceberg and Kafka configurations"""

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
            .config(
                "spark.sql.catalog.iceberg.catalog-impl",
                "org.apache.iceberg.rest.RESTCatalog",
            )
            .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
            .config("spark.sql.catalog.iceberg.warehouse", "s3a://data-lake/warehouse/")
            .config(
                "spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            # MinIO configuration for local development
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
            .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
            .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin")
            .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")
        return spark

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

    def _setup_schemas(self):
        """Define schemas for different event types"""

        # Base event schema
        self.base_event_schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", StringType(), False),
                StructField("user_id", StringType(), True),
                StructField("session_id", StringType(), True),
            ]
        )

        # Page view event schema
        self.page_view_schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", StringType(), False),
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
                StructField("timestamp", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("order_id", StringType(), False),
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("product_id", StringType(), False),
                                StructField("quantity", IntegerType(), False),
                                StructField("unit_price", DoubleType(), False),
                                StructField("total_price", DoubleType(), False),
                                StructField("category", StringType(), True),
                                StructField("brand", StringType(), True),
                            ]
                        )
                    ),
                    False,
                ),
                StructField("subtotal", DoubleType(), False),
                StructField("discount_percent", DoubleType(), True),
                StructField("discount_amount", DoubleType(), True),
                StructField("total_amount", DoubleType(), False),
                StructField("payment_method", StringType(), True),
                StructField("shipping_method", StringType(), True),
            ]
        )

    def read_kafka_stream(self, kafka_bootstrap_servers: str, topic: str):
        """Read streaming data from Kafka"""

        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("kafka.consumer.group.id", f"{self.app_name}-consumer")
            .load()
        )

    def parse_events(self, kafka_df):
        """Parse JSON events from Kafka"""

        # Parse the JSON value and add metadata
        parsed_df = (
            kafka_df.select(
                col("key").cast("string").alias("message_key"),
                col("value").cast("string").alias("raw_message"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
            )
            .withColumn(
                "parsed_event",
                from_json(
                    col("raw_message"),
                    StructType(
                        [
                            StructField("event_id", StringType()),
                            StructField("event_type", StringType()),
                            StructField("timestamp", StringType()),
                            StructField("user_id", StringType()),
                            StructField("session_id", StringType()),
                        ]
                    ),  # This is a simplified schema - in practice, use union types
                ),
            )
            .select("*", col("parsed_event.*"))
            .withColumn(
                "event_timestamp",
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            )
            .withColumn("processing_time", current_timestamp())
            .withColumn("year", year(col("event_timestamp")))
            .withColumn("month", month(col("event_timestamp")))
            .withColumn("day", dayofmonth(col("event_timestamp")))
            .withColumn("hour", hour(col("event_timestamp")))
        )

        return parsed_df

    def validate_and_clean_events(self, df):
        """Validate and clean the event data"""

        # Filter out invalid events
        validated_df = df.filter(
            (col("event_id").isNotNull())
            & (col("event_type").isNotNull())
            & (col("event_timestamp").isNotNull())
            & (col("event_timestamp") > lit("2020-01-01"))  # Reasonable timestamp range
            & (col("event_timestamp") <= current_timestamp())
        )

        # Add data quality flags
        quality_df = validated_df.withColumn(
            "data_quality_score",
            when(col("user_id").isNotNull(), lit(1.0))
            .when(col("session_id").isNotNull(), lit(0.8))
            .otherwise(lit(0.5)),
        ).withColumn("is_valid_event", col("data_quality_score") >= lit(0.5))

        return quality_df

    def enrich_events(self, df):
        """Enrich events with additional computed fields"""

        enriched_df = (
            df.withColumn(
                "is_weekend",
                date_format(col("event_timestamp"), "EEEE").isin(
                    ["Saturday", "Sunday"]
                ),
            )
            .withColumn("hour_of_day", hour(col("event_timestamp")))
            .withColumn("day_of_week", date_format(col("event_timestamp"), "EEEE"))
            .withColumn(
                "is_business_hours",
                (hour(col("event_timestamp")) >= 9)
                & (hour(col("event_timestamp")) <= 17),
            )
        )

        return enriched_df

    def write_to_iceberg(self, df, table_name: str, checkpoint_location: str):
        """Write streaming data to Iceberg tables"""

        return (
            df.writeStream.format("iceberg")
            .option("table", f"iceberg.default.{table_name}")
            .option("checkpointLocation", checkpoint_location)
            .option("fanout-enabled", "true")
            .outputMode("append")
            .trigger(processingTime="30 seconds")
        )

    def create_real_time_aggregations(self, df):
        """Create real-time aggregations with watermarking"""

        # Set watermark for handling late data
        watermarked_df = df.withWatermark("event_timestamp", "5 minutes")

        # 5-minute window aggregations
        window_agg = (
            watermarked_df.filter(col("event_type") == "page_view")
            .groupBy(window(col("event_timestamp"), "5 minutes"), col("event_type"))
            .agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("event_type"),
                col("event_count"),
                col("unique_users"),
                col("unique_sessions"),
                current_timestamp().alias("processed_at"),
            )
        )

        return window_agg

    def create_user_session_analysis(self, df):
        """Analyze user sessions in real-time"""

        # Session-level metrics
        session_metrics = (
            df.withWatermark("event_timestamp", "10 minutes")
            .groupBy(
                col("user_id"),
                col("session_id"),
                window(col("event_timestamp"), "30 minutes"),
            )
            .agg(
                count("*").alias("events_in_session"),
                countDistinct("event_type").alias("unique_event_types"),
                min("event_timestamp").alias("session_start"),
                max("event_timestamp").alias("session_end"),
                collect_set("event_type").alias("event_types"),
            )
            .withColumn(
                "session_duration_minutes",
                (unix_timestamp("session_end") - unix_timestamp("session_start")) / 60,
            )
            .select(
                col("user_id"),
                col("session_id"),
                col("window.start").alias("analysis_window_start"),
                col("events_in_session"),
                col("unique_event_types"),
                col("session_start"),
                col("session_end"),
                col("session_duration_minutes"),
                col("event_types"),
                current_timestamp().alias("processed_at"),
            )
        )

        return session_metrics

    def process_purchase_events(self, df):
        """Special processing for purchase events"""

        purchase_df = df.filter(col("event_type") == "purchase")

        # Parse the complete purchase event with proper schema
        purchase_parsed = (
            purchase_df.select(
                "*",
                from_json(col("raw_message"), self.purchase_schema).alias(
                    "purchase_data"
                ),
            )
            .select(col("*"), col("purchase_data.*"))
            .drop("purchase_data")
        )

        # Explode items for item-level analysis
        purchase_items = (
            purchase_parsed.select(
                col("event_id"),
                col("user_id"),
                col("order_id"),
                col("event_timestamp"),
                explode(col("items")).alias("item"),
                col("total_amount"),
                col("payment_method"),
                current_timestamp().alias("processed_at"),
            )
            .select(
                col("*"),
                col("item.product_id"),
                col("item.quantity"),
                col("item.unit_price"),
                col("item.total_price"),
                col("item.category"),
                col("item.brand"),
            )
            .drop("item")
        )

        return purchase_items

    def run_streaming_pipeline(
        self, kafka_bootstrap_servers: str, kafka_topic: str, s3_bucket: str
    ):
        """Run the complete streaming pipeline"""

        logger.info("Starting e-commerce streaming pipeline...")

        # Read from Kafka
        kafka_df = self.read_kafka_stream(kafka_bootstrap_servers, kafka_topic)

        # Parse and process events
        parsed_df = self.parse_events(kafka_df)
        validated_df = self.validate_and_clean_events(parsed_df)
        enriched_df = self.enrich_events(validated_df)

        # Write raw events to Bronze layer
        bronze_query = self.write_to_iceberg(
            enriched_df, "bronze_events", f"s3://{s3_bucket}/checkpoints/bronze_events/"
        ).start()

        # Create real-time aggregations for Silver layer
        window_agg = self.create_real_time_aggregations(enriched_df)
        silver_agg_query = self.write_to_iceberg(
            window_agg,
            "silver_event_aggregations",
            f"s3://{s3_bucket}/checkpoints/silver_aggregations/",
        ).start()

        # User session analysis
        session_analysis = self.create_user_session_analysis(enriched_df)
        session_query = self.write_to_iceberg(
            session_analysis,
            "silver_user_sessions",
            f"s3://{s3_bucket}/checkpoints/user_sessions/",
        ).start()

        # Purchase event processing
        purchase_items = self.process_purchase_events(enriched_df)
        purchase_query = self.write_to_iceberg(
            purchase_items,
            "silver_purchase_items",
            f"s3://{s3_bucket}/checkpoints/purchase_items/",
        ).start()

        # Console output for monitoring (remove in production)
        console_query = (
            enriched_df.select(
                col("event_type"),
                col("user_id"),
                col("event_timestamp"),
                col("data_quality_score"),
            )
            .writeStream.format("console")
            .option("truncate", "false")
            .option("numRows", 5)
            .trigger(processingTime="30 seconds")
            .start()
        )

        # Wait for termination
        try:
            bronze_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping streaming queries...")
            bronze_query.stop()
            silver_agg_query.stop()
            session_query.stop()
            purchase_query.stop()
            console_query.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="E-commerce Streaming Pipeline")
    parser.add_argument(
        "--kafka-servers", required=True, help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--kafka-topic", default="raw-events", help="Kafka topic to consume from"
    )
    parser.add_argument(
        "--s3-bucket", required=True, help="S3 bucket for checkpoints and data"
    )

    args = parser.parse_args()

    processor = EcommerceStreamProcessor()
    processor.run_streaming_pipeline(
        args.kafka_servers, args.kafka_topic, args.s3_bucket
    )
