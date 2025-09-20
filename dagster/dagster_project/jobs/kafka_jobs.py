"""
Dagster jobs for Kafka data ingestion pipeline.
"""

from dagster import DefaultSensorContext, SensorResult, job, sensor

from ..assets.kafka_ingestion import (
    gold_daily_customer_metrics,
    kafka_raw_events,
    minio_storage_setup,
    silver_page_views,
    silver_purchases,
)


@job(
    description="Kafka ingestion job that processes raw events through bronze -> silver -> gold layers"
)
def kafka_ingestion_job():
    """
    Job that runs the complete Kafka ingestion pipeline:
    1. Check MinIO storage setup
    2. Ingest raw events from Kafka
    3. Process page views and purchases (silver layer)
    4. Create daily customer metrics (gold layer)
    """
    # Infrastructure check
    storage_check = minio_storage_setup()

    # Bronze layer - ingest from Kafka
    raw_events = kafka_raw_events()

    # Silver layer - clean and structure data
    page_views = silver_page_views(raw_events)
    purchases = silver_purchases(raw_events)

    # Gold layer - business metrics
    metrics = gold_daily_customer_metrics(page_views, purchases)

    return metrics


@job(description="Quick test job for Kafka connectivity and basic ingestion")
def kafka_test_job():
    """
    Lightweight job for testing Kafka connectivity.
    """
    storage_check = minio_storage_setup()
    raw_events = kafka_raw_events()
    return raw_events


# Sensor to trigger job when new events are available (optional)
@sensor(
    job=kafka_ingestion_job,
    description="Sensor that triggers Kafka ingestion when events are available",
)
def kafka_events_sensor(context: DefaultSensorContext):
    """
    Sensor that periodically checks for new events in Kafka and triggers processing.
    This is a simple implementation - in production you'd want more sophisticated logic.
    """
    # For now, we'll just trigger every time the sensor runs
    # In practice, you'd check Kafka topic offsets or other indicators

    should_run = True  # Simplified logic

    if should_run:
        return SensorResult(
            run_requests=[kafka_ingestion_job.run_request_for_partition()]
        )

    return SensorResult(skip_reason="No new events detected")
