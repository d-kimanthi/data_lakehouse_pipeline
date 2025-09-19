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
