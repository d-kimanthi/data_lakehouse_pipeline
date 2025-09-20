# Import Spark-based batch processing assets
from assets.spark_batch_assets import (
    gold_daily_customer_analytics,
    gold_daily_sales_summary,
    gold_product_analytics,
    silver_data_quality_report,
    streaming_health_monitor,
)

from dagster import Definitions

# Define all assets using the new Spark-only architecture
defs = Definitions(
    assets=[
        # Spark batch processing assets for Gold layer analytics
        gold_daily_customer_analytics,
        gold_daily_sales_summary,
        gold_product_analytics,
        # Data quality and monitoring assets
        silver_data_quality_report,
        streaming_health_monitor,
    ],
    jobs=[
        # Define jobs in future if needed for scheduling multiple assets together
    ],
    schedules=[
        # Asset-based schedules will be defined individually in the assets themselves
    ],
    resources={
        # Resources can be configured here if needed for shared configurations
    },
)
