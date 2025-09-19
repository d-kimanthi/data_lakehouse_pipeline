# dagster/dagster_project/jobs/__init__.py

from dagster import (
    define_asset_job,
    job,
    op,
    schedule,
    DefaultScheduleStatus,
    build_schedule_from_partitioned_job,
    AssetSelection,
    Definitions,
)
from ..assets.data_marts import (
    bronze_events,
    silver_events,
    daily_sales_summary,
    user_behavior_metrics,
    product_analytics,
    data_quality_report,
)
from ..resources.spark_resource import spark_resource
from ..resources.iceberg_resource import iceberg_resource

# Asset selection for different processing layers
bronze_assets = AssetSelection.assets(bronze_events)
silver_assets = AssetSelection.assets(silver_events, data_quality_report)
gold_assets = AssetSelection.assets(
    daily_sales_summary, user_behavior_metrics, product_analytics
)

# Define asset jobs
bronze_processing_job = define_asset_job(
    name="bronze_processing_job",
    selection=bronze_assets,
    description="Process raw streaming events into bronze layer",
)

silver_processing_job = define_asset_job(
    name="silver_processing_job",
    selection=silver_assets,
    description="Clean and validate events, create silver layer with quality checks",
)

gold_processing_job = define_asset_job(
    name="gold_processing_job",
    selection=gold_assets,
    description="Create business-ready data marts and analytics tables",
)

# Complete daily processing job
daily_processing_job = define_asset_job(
    name="daily_processing_job",
    selection=AssetSelection.all(),
    description="Complete daily data processing pipeline from bronze to gold",
)

# Schedules for automated execution
daily_bronze_schedule = build_schedule_from_partitioned_job(
    bronze_processing_job,
    hour_of_day=1,  # Run at 1 AM
    minute_of_hour=0,
    default_status=DefaultScheduleStatus.RUNNING,
)

daily_silver_schedule = build_schedule_from_partitioned_job(
    silver_processing_job,
    hour_of_day=2,  # Run at 2 AM (after bronze)
    minute_of_hour=0,
    default_status=DefaultScheduleStatus.RUNNING,
)

daily_gold_schedule = build_schedule_from_partitioned_job(
    gold_processing_job,
    hour_of_day=3,  # Run at 3 AM (after silver)
    minute_of_hour=0,
    default_status=DefaultScheduleStatus.RUNNING,
)


# Complete pipeline schedule
@schedule(
    job=daily_processing_job,
    cron_schedule="0 4 * * *",  # Run at 4 AM daily
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_complete_pipeline_schedule(context):
    """
    Daily complete pipeline execution
    """
    return {
        "resources": {
            "spark": {
                "config": {
                    "spark_conf": {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    }
                }
            }
        }
    }


# Manual backfill job for historical data
@job(resource_defs={"spark": spark_resource, "iceberg": iceberg_resource})
def backfill_historical_data():
    """
    Manual job for backfilling historical data
    """
    backfill_bronze()
    backfill_silver()
    backfill_gold()


@op
def backfill_bronze(context):
    """Backfill bronze layer data"""
    context.log.info("Starting bronze layer backfill...")
    # Implementation would trigger bronze asset materialization
    # for specified date range
    pass


@op
def backfill_silver(context):
    """Backfill silver layer data"""
    context.log.info("Starting silver layer backfill...")
    pass


@op
def backfill_gold(context):
    """Backfill gold layer data"""
    context.log.info("Starting gold layer backfill...")
    pass


# Real-time monitoring job
@job(resource_defs={"spark": spark_resource})
def streaming_monitoring_job():
    """
    Monitor streaming pipeline health and performance
    """
    check_kafka_lag()
    check_spark_streaming_health()
    validate_data_freshness()


@op
def check_kafka_lag(context):
    """Check Kafka consumer lag"""
    # Implementation would check Kafka consumer group lag
    context.log.info("Checking Kafka consumer lag...")
    pass


@op
def check_spark_streaming_health(context):
    """Check Spark streaming application health"""
    # Implementation would check Spark streaming metrics
    context.log.info("Checking Spark streaming health...")
    pass


@op
def validate_data_freshness(context):
    """Validate that data is being processed in near real-time"""
    # Implementation would check data freshness in Iceberg tables
    context.log.info("Validating data freshness...")
    pass


# Data quality monitoring schedule
@schedule(
    job=define_asset_job(
        name="data_quality_check", selection=AssetSelection.assets(data_quality_report)
    ),
    cron_schedule="0 */2 * * *",  # Every 2 hours
    default_status=DefaultScheduleStatus.RUNNING,
)
def data_quality_monitoring_schedule(context):
    """
    Regular data quality monitoring
    """
    return {}


# Alert job for critical issues
@job
def critical_alerts_job():
    """
    Job to handle critical data pipeline alerts
    """
    check_data_pipeline_failures()
    send_alert_notifications()


@op
def check_data_pipeline_failures(context):
    """Check for critical pipeline failures"""
    context.log.info("Checking for critical pipeline failures...")
    # Implementation would check for:
    # - Failed asset materializations
    # - Data quality failures
    # - Streaming pipeline downtime
    pass


@op
def send_alert_notifications(context):
    """Send notifications for critical issues"""
    context.log.info("Sending alert notifications...")
    # Implementation would send alerts via:
    # - Slack/Teams
    # - Email
    # - PagerDuty
    pass


# ===============================
# Resources Configuration
# ===============================

# dagster/dagster_project/resources/spark_resource.py

from dagster import resource, InitResourceContext
from pyspark.sql import SparkSession
import os


@resource(config_schema={"spark_conf": dict, "app_name": str, "master": str})
def spark_resource(init_context: InitResourceContext):
    """
    Spark session resource for Dagster
    """
    config = init_context.resource_config

    spark_conf = config.get("spark_conf", {})
    app_name = config.get("app_name", "dagster-spark")
    master = config.get("master", "local[*]")

    # Base Spark configuration
    base_conf = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.type": "hive",
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "glue",
        "spark.sql.catalog.iceberg.warehouse": f"s3://{os.getenv('S3_BUCKET', 'default-bucket')}/iceberg-warehouse/",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }

    # Merge with user-provided config
    final_conf = {**base_conf, **spark_conf}

    # Create Spark session
    builder = SparkSession.builder.appName(app_name).master(master)

    for key, value in final_conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        yield spark
    finally:
        spark.stop()


# dagster/dagster_project/resources/iceberg_resource.py

from dagster import resource, InitResourceContext
import boto3
import os


@resource(config_schema={"warehouse_path": str, "catalog_type": str, "aws_region": str})
def iceberg_resource(init_context: InitResourceContext):
    """
    Iceberg catalog resource
    """
    config = init_context.resource_config

    warehouse_path = config.get(
        "warehouse_path", f"s3://{os.getenv('S3_BUCKET')}/iceberg-warehouse/"
    )
    catalog_type = config.get("catalog_type", "glue")
    aws_region = config.get("aws_region", "us-west-2")

    # Initialize AWS clients if needed
    if catalog_type == "glue":
        glue_client = boto3.client("glue", region_name=aws_region)
        s3_client = boto3.client("s3", region_name=aws_region)

        catalog_config = {
            "warehouse_path": warehouse_path,
            "catalog_type": catalog_type,
            "glue_client": glue_client,
            "s3_client": s3_client,
        }
    else:
        catalog_config = {
            "warehouse_path": warehouse_path,
            "catalog_type": catalog_type,
        }

    return catalog_config


# dagster/dagster_project/resources/kafka_resource.py

from dagster import resource, InitResourceContext
from kafka import KafkaConsumer, KafkaProducer
import json


@resource(
    config_schema={
        "bootstrap_servers": list,
        "consumer_config": dict,
        "producer_config": dict,
    }
)
def kafka_resource(init_context: InitResourceContext):
    """
    Kafka resource for consumers and producers
    """
    config = init_context.resource_config

    bootstrap_servers = config.get("bootstrap_servers", ["localhost:9092"])
    consumer_config = config.get("consumer_config", {})
    producer_config = config.get("producer_config", {})

    # Default configurations
    default_consumer_config = {
        "bootstrap_servers": bootstrap_servers,
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
    }

    default_producer_config = {
        "bootstrap_servers": bootstrap_servers,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "acks": "all",
        "retries": 3,
    }

    # Merge configs
    final_consumer_config = {**default_consumer_config, **consumer_config}
    final_producer_config = {**default_producer_config, **producer_config}

    kafka_clients = {
        "consumer": lambda topic: KafkaConsumer(topic, **final_consumer_config),
        "producer": lambda: KafkaProducer(**final_producer_config),
        "config": {
            "bootstrap_servers": bootstrap_servers,
            "consumer_config": final_consumer_config,
            "producer_config": final_producer_config,
        },
    }

    return kafka_clients


# ===============================
# Main Definitions
# ===============================

# dagster/dagster_project/__init__.py

from dagster import Definitions
from .assets.data_marts import (
    bronze_events,
    silver_events,
    daily_sales_summary,
    user_behavior_metrics,
    product_analytics,
    data_quality_report,
)
from .jobs import (
    bronze_processing_job,
    silver_processing_job,
    gold_processing_job,
    daily_processing_job,
    backfill_historical_data,
    streaming_monitoring_job,
    critical_alerts_job,
    daily_bronze_schedule,
    daily_silver_schedule,
    daily_gold_schedule,
    daily_complete_pipeline_schedule,
    data_quality_monitoring_schedule,
)
from .resources.spark_resource import spark_resource
from .resources.iceberg_resource import iceberg_resource
from .resources.kafka_resource import kafka_resource

# Define all assets, jobs, schedules, and resources
defs = Definitions(
    assets=[
        bronze_events,
        silver_events,
        daily_sales_summary,
        user_behavior_metrics,
        product_analytics,
        data_quality_report,
    ],
    jobs=[
        bronze_processing_job,
        silver_processing_job,
        gold_processing_job,
        daily_processing_job,
        backfill_historical_data,
        streaming_monitoring_job,
        critical_alerts_job,
    ],
    schedules=[
        daily_bronze_schedule,
        daily_silver_schedule,
        daily_gold_schedule,
        daily_complete_pipeline_schedule,
        data_quality_monitoring_schedule,
    ],
    resources={
        "spark": spark_resource.configured(
            {
                "spark_conf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.minPartitionSize": "128MB",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                },
                "app_name": "ecommerce-analytics-dagster",
                "master": "local[*]",  # Override in production
            }
        ),
        "iceberg": iceberg_resource.configured(
            {
                "warehouse_path": "s3://your-data-lake-bucket/iceberg-warehouse/",
                "catalog_type": "glue",
                "aws_region": "us-west-2",
            }
        ),
        "kafka": kafka_resource.configured(
            {
                "bootstrap_servers": ["your-msk-cluster:9092"],
                "consumer_config": {
                    "group_id": "dagster-consumer-group",
                    "session_timeout_ms": 30000,
                    "heartbeat_interval_ms": 3000,
                },
                "producer_config": {
                    "batch_size": 16384,
                    "linger_ms": 10,
                    "buffer_memory": 33554432,
                },
            }
        ),
    },
)
