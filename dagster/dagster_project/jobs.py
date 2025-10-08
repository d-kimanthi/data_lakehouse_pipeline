"""
Dagster jobs for orchestrating silver to gold layer batch processing.

These jobs define the execution order and dependencies for the gold layer transformations.
"""

from assets import daily_customer_analytics

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# # Define asset job for Pipes-enabled gold layer transformation
# silver_to_gold_job = define_asset_job(
#     name="silver_to_gold_job",
#     selection=AssetSelection.assets(
#         daily_customer_analytics,
#     ),
#     description="Batch job to process data from silver to gold layer using Dagster Pipes",
# )

# Define asset job for customer analytics specifically
customer_analytics_job = define_asset_job(
    name="customer_analytics_job",
    selection=AssetSelection.assets(daily_customer_analytics),
    description="Job to process daily customer analytics data using enhanced Pipes observability",
)

# Define schedules for the jobs
# silver_to_gold_schedule = ScheduleDefinition(
#     job=silver_to_gold_job,
#     cron_schedule="0 2 * * *",  # Run at 2 AM every day
#     default_status=DefaultScheduleStatus.STOPPED,
# )

customer_analytics_schedule = ScheduleDefinition(
    job=customer_analytics_job,
    cron_schedule="0 3 * * *",  # Run at 3 AM every day
    default_status=DefaultScheduleStatus.STOPPED,
)

# Export jobs and schedules
silver_to_gold_jobs = [customer_analytics_job]
silver_to_gold_schedules = [customer_analytics_schedule]
