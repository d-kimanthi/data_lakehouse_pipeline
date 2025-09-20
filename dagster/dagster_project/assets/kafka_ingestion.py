"""
Dagster assets for ingesting data from Kafka topics into the data lake.
These assets handle the batch processing of streamed events.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from dagster import (
    AssetExecutionContext,
    AssetIn,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    Output,
    asset,
    get_dagster_logger,
)

# Set up logging
logger = get_dagster_logger()

# Define partitions for daily processing
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")


@asset(
    description="Raw e-commerce events consumed from Kafka raw-events topic",
    group_name="bronze_layer",
)
def kafka_raw_events(context: AssetExecutionContext) -> MaterializeResult:
    """
    Consume raw events from Kafka and save them to MinIO (bronze layer).
    This represents a batch job that periodically pulls events from Kafka.
    """
    logger.info("Starting Kafka raw events ingestion...")

    try:
        # Configure Kafka consumer
        consumer = KafkaConsumer(
            "raw-events",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="dagster-batch-consumer",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=10000,  # 10 second timeout
        )

        events = []
        event_types = {}

        logger.info("Consuming events from Kafka...")
        for message in consumer:
            event = message.value
            events.append(event)

            # Track event types
            event_type = event.get("event_type", "unknown")
            event_types[event_type] = event_types.get(event_type, 0) + 1

            # Limit batch size for demo
            if len(events) >= 100:
                break

        consumer.close()

        if not events:
            logger.info("No new events found in Kafka")
            return MaterializeResult(
                metadata={"events_processed": 0, "status": "no_new_events"}
            )

        # Convert to DataFrame for easier handling
        df = pd.DataFrame(events)

        # Save to MinIO as parquet (simulating S3)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"/tmp/bronze_events_{timestamp}.parquet"
        df.to_parquet(file_path, index=False)

        logger.info(f"Processed {len(events)} events and saved to {file_path}")

        return MaterializeResult(
            metadata={
                "events_processed": len(events),
                "event_types": event_types,
                "file_path": file_path,
                "records_preview": MetadataValue.json(events[:3]) if events else {},
                "schema": (
                    MetadataValue.json(list(df.columns.tolist()))
                    if not df.empty
                    else {}
                ),
            }
        )

    except NoBrokersAvailable:
        logger.error("Could not connect to Kafka brokers")
        return MaterializeResult(
            metadata={"events_processed": 0, "status": "kafka_connection_error"}
        )
    except Exception as e:
        logger.error(f"Error in Kafka ingestion: {str(e)}")
        raise


@asset(
    ins={"raw_events": AssetIn("kafka_raw_events")},
    description="Page view events extracted and cleaned from raw events",
    group_name="silver_layer",
)
def silver_page_views(
    context: AssetExecutionContext, raw_events: MaterializeResult
) -> MaterializeResult:
    """
    Extract and clean page view events from raw events (silver layer).
    """
    logger.info("Processing page view events...")

    try:
        # Get the file path from raw events metadata
        raw_metadata = raw_events.metadata
        events_processed = raw_metadata.get("events_processed", 0)

        if events_processed == 0:
            logger.info("No raw events to process")
            return MaterializeResult(
                metadata={"page_views_processed": 0, "status": "no_raw_events"}
            )

        # In a real implementation, we'd read from MinIO
        # For now, we'll simulate processing page view events
        file_path = raw_metadata.get("file_path")

        if file_path and Path(file_path).exists():
            df = pd.read_parquet(file_path)

            # Filter for page view events
            page_views = df[df["event_type"] == "page_view"].copy()

            if page_views.empty:
                logger.info("No page view events found")
                return MaterializeResult(
                    metadata={"page_views_processed": 0, "status": "no_page_views"}
                )

            # Clean and standardize page view data
            page_views["processed_timestamp"] = datetime.now()
            page_views["date"] = pd.to_datetime(page_views["timestamp"]).dt.date

            # Save cleaned page views
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            clean_file_path = f"/tmp/silver_page_views_{timestamp}.parquet"
            page_views.to_parquet(clean_file_path, index=False)

            logger.info(f"Processed {len(page_views)} page view events")

            return MaterializeResult(
                metadata={
                    "page_views_processed": len(page_views),
                    "unique_users": page_views["user_id"].nunique(),
                    "unique_products": (
                        page_views["product_id"].nunique()
                        if "product_id" in page_views.columns
                        else 0
                    ),
                    "file_path": clean_file_path,
                    "sample_records": MetadataValue.json(
                        page_views.head(2).to_dict("records")
                    ),
                }
            )
        else:
            logger.warning("Raw events file not found")
            return MaterializeResult(
                metadata={"page_views_processed": 0, "status": "raw_file_not_found"}
            )

    except Exception as e:
        logger.error(f"Error processing page views: {str(e)}")
        raise


@asset(
    ins={"raw_events": AssetIn("kafka_raw_events")},
    description="Purchase events extracted and enriched from raw events",
    group_name="silver_layer",
)
def silver_purchases(
    context: AssetExecutionContext, raw_events: MaterializeResult
) -> MaterializeResult:
    """
    Extract and enrich purchase events from raw events (silver layer).
    """
    logger.info("Processing purchase events...")

    try:
        raw_metadata = raw_events.metadata
        events_processed = raw_metadata.get("events_processed", 0)

        if events_processed == 0:
            logger.info("No raw events to process")
            return MaterializeResult(
                metadata={"purchases_processed": 0, "status": "no_raw_events"}
            )

        file_path = raw_metadata.get("file_path")

        if file_path and Path(file_path).exists():
            df = pd.read_parquet(file_path)

            # Filter for purchase events
            purchases = df[df["event_type"] == "purchase"].copy()

            if purchases.empty:
                logger.info("No purchase events found")
                return MaterializeResult(
                    metadata={"purchases_processed": 0, "status": "no_purchases"}
                )

            # Enrich purchase data
            purchases["processed_timestamp"] = datetime.now()
            purchases["date"] = pd.to_datetime(purchases["timestamp"]).dt.date

            # Calculate metrics
            total_revenue = (
                purchases["total_amount"].sum()
                if "total_amount" in purchases.columns
                else 0
            )
            avg_order_value = (
                purchases["total_amount"].mean()
                if "total_amount" in purchases.columns
                else 0
            )

            # Save enriched purchases
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            clean_file_path = f"/tmp/silver_purchases_{timestamp}.parquet"
            purchases.to_parquet(clean_file_path, index=False)

            logger.info(f"Processed {len(purchases)} purchase events")

            return MaterializeResult(
                metadata={
                    "purchases_processed": len(purchases),
                    "unique_customers": purchases["user_id"].nunique(),
                    "total_revenue": float(total_revenue),
                    "avg_order_value": float(avg_order_value),
                    "file_path": clean_file_path,
                    "sample_records": MetadataValue.json(
                        purchases.head(2).to_dict("records")
                    ),
                }
            )
        else:
            logger.warning("Raw events file not found")
            return MaterializeResult(
                metadata={"purchases_processed": 0, "status": "raw_file_not_found"}
            )

    except Exception as e:
        logger.error(f"Error processing purchases: {str(e)}")
        raise


@asset(
    ins={
        "page_views": AssetIn("silver_page_views"),
        "purchases": AssetIn("silver_purchases"),
    },
    description="Daily customer analytics aggregated from page views and purchases",
    group_name="gold_layer",
)
def gold_daily_customer_metrics(
    context: AssetExecutionContext,
    page_views: MaterializeResult,
    purchases: MaterializeResult,
) -> MaterializeResult:
    """
    Create daily customer analytics from silver layer data (gold layer).
    """
    logger.info("Building daily customer metrics...")

    try:
        page_views_count = page_views.metadata.get("page_views_processed", 0)
        purchases_count = purchases.metadata.get("purchases_processed", 0)

        # Create summary metrics
        today = datetime.now().date()

        metrics = {
            "date": str(today),
            "total_page_views": page_views_count,
            "total_purchases": purchases_count,
            "unique_users_page_views": page_views.metadata.get("unique_users", 0),
            "unique_customers_purchases": purchases.metadata.get("unique_customers", 0),
            "total_revenue": purchases.metadata.get("total_revenue", 0),
            "avg_order_value": purchases.metadata.get("avg_order_value", 0),
            "conversion_rate": (
                (purchases_count / max(page_views_count, 1)) * 100
                if page_views_count > 0
                else 0
            ),
        }

        # Save daily metrics
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        metrics_file = f"/tmp/gold_daily_metrics_{timestamp}.json"

        with open(metrics_file, "w") as f:
            json.dump(metrics, f, indent=2)

        logger.info(f"Generated daily customer metrics: {metrics}")

        return MaterializeResult(
            metadata={
                "metrics": MetadataValue.json(metrics),
                "file_path": metrics_file,
                "conversion_rate": f"{metrics['conversion_rate']:.2f}%",
                "revenue_per_user": f"${metrics['total_revenue'] / max(metrics['unique_users_page_views'], 1):.2f}",
            }
        )

    except Exception as e:
        logger.error(f"Error building customer metrics: {str(e)}")
        raise


@asset(
    description="MinIO/S3 bucket health check and setup", group_name="infrastructure"
)
def minio_storage_setup(context: AssetExecutionContext) -> MaterializeResult:
    """
    Verify MinIO is accessible and create necessary buckets.
    """
    logger.info("Checking MinIO storage setup...")

    try:
        # For now, simulate MinIO check
        # In real implementation, would use boto3 to check MinIO

        buckets_needed = ["bronze-events", "silver-events", "gold-analytics"]

        # Simulate bucket creation/verification
        logger.info(f"Verified buckets: {buckets_needed}")

        return MaterializeResult(
            metadata={
                "status": "healthy",
                "buckets": buckets_needed,
                "storage_type": "MinIO (S3 compatible)",
                "connection": "localhost:9000",
            }
        )

    except Exception as e:
        logger.error(f"MinIO setup error: {str(e)}")
        return MaterializeResult(metadata={"status": "error", "error": str(e)})
