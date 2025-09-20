"""
Dagster assets for dbt models integration.
This module creates Dagster assets from dbt models for full orchestration.
"""

import os
import subprocess
from pathlib import Path

from dagster_dbt import DbtCliResource, dbt_assets, get_asset_key_for_model

from dagster import AssetExecutionContext, Config, MaterializeResult, asset

# Path to your dbt project
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"


class DbtConfig(Config):
    """Configuration for dbt runs"""

    profiles_dir: str = str(DBT_PROJECT_DIR / "profiles")
    project_dir: str = str(DBT_PROJECT_DIR)


# Create a dbt resource
dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=(
        str(DBT_PROJECT_DIR / "profiles")
        if (DBT_PROJECT_DIR / "profiles").exists()
        else None
    ),
)


@dbt_assets(
    manifest=(
        str(DBT_PROJECT_DIR / "target" / "manifest.json")
        if (DBT_PROJECT_DIR / "target" / "manifest.json").exists()
        else None
    ),
    select="staging",
    group_name="silver_layer",
)
def dbt_staging_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt staging models that clean and standardize bronze layer data.
    These models depend on bronze layer assets from Dagster.
    """
    yield from dbt.cli(["build", "--select", "staging"], context=context).stream()


@dbt_assets(
    manifest=(
        str(DBT_PROJECT_DIR / "target" / "manifest.json")
        if (DBT_PROJECT_DIR / "target" / "manifest.json").exists()
        else None
    ),
    select="marts",
    group_name="gold_layer",
)
def dbt_marts_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt mart models that create business-ready analytics tables.
    These models depend on staging models.
    """
    yield from dbt.cli(["build", "--select", "marts"], context=context).stream()


# Alternative: Individual model assets for finer control
@asset(
    description="Run dbt daily sales summary model",
    group_name="gold_layer",
    deps=["stg_page_views", "stg_purchases"],  # Depends on staging models
)
def daily_sales_summary_dbt(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run the daily sales summary dbt model.
    This gives you more control over individual model execution.
    """
    try:
        # Run specific dbt model
        result = subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "daily_sales_summary",
                "--project-dir",
                str(DBT_PROJECT_DIR),
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        context.log.info(f"dbt run output: {result.stdout}")

        return MaterializeResult(
            metadata={
                "dbt_output": result.stdout,
                "model": "daily_sales_summary",
                "status": "success",
            }
        )
    except subprocess.CalledProcessError as e:
        context.log.error(f"dbt run failed: {e.stderr}")
        raise


@asset(
    description="Run dbt tests for data quality validation", group_name="data_quality"
)
def dbt_data_quality_tests(context: AssetExecutionContext) -> MaterializeResult:
    """
    Run dbt tests to validate data quality across all models.
    """
    try:
        # Run dbt tests
        result = subprocess.run(
            ["dbt", "test", "--project-dir", str(DBT_PROJECT_DIR)],
            capture_output=True,
            text=True,
            check=True,
        )

        context.log.info(f"dbt test output: {result.stdout}")

        return MaterializeResult(
            metadata={
                "dbt_output": result.stdout,
                "tests_status": "passed",
                "status": "success",
            }
        )
    except subprocess.CalledProcessError as e:
        context.log.error(f"dbt tests failed: {e.stderr}")
        # You might want to continue execution even if tests fail
        return MaterializeResult(
            metadata={
                "dbt_output": e.stderr,
                "tests_status": "failed",
                "status": "failed",
            }
        )
