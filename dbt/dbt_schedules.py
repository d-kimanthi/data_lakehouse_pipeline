"""
Dagster schedules for dbt model execution.
This approach runs dbt models on a schedule.
"""

from assets.dbt_assets import dbt_marts_models, dbt_staging_models

from dagster import RunRequest, ScheduleEvaluationContext, schedule


@schedule(
    cron_schedule="0 2 * * *",  # Run at 2 AM daily
    job_name="dbt_daily_transformation",
    execution_timezone="UTC",
)
def daily_dbt_schedule(context: ScheduleEvaluationContext):
    """
    Schedule dbt transformations to run daily after data ingestion.
    """
    return RunRequest(
        run_key=f"daily_dbt_{context.scheduled_execution_time.strftime('%Y%m%d')}",
        tags={
            "dagster/schedule_name": "daily_dbt_schedule",
            "dbt_run_type": "daily_transformation",
        },
    )


@schedule(
    cron_schedule="0 */6 * * *",  # Run every 6 hours
    job_name="dbt_staging_refresh",
    execution_timezone="UTC",
)
def staging_refresh_schedule(context: ScheduleEvaluationContext):
    """
    Refresh staging models more frequently for near real-time analytics.
    """
    return RunRequest(
        run_key=f"staging_refresh_{context.scheduled_execution_time.strftime('%Y%m%d_%H')}",
        tags={
            "dagster/schedule_name": "staging_refresh_schedule",
            "dbt_run_type": "staging_refresh",
        },
    )
