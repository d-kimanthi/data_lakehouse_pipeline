import os

from dagster import AssetIn, Definitions, ScheduleDefinition, asset, define_asset_job
from great_expectations.data_context import DataContext
from great_expectations.data_context.types.base import DataContextConfig


@asset
def validate_orders():
    """Run Great Expectations validation on orders table"""
    context = DataContext(
        project_config=DataContextConfig(
            config_version=1.0,
            plugins_directory=None,
            config_variables_file_path=None,
            datasources={
                "retail_orders": {
                    "class_name": "Datasource",
                    "data_connectors": {
                        "default_runtime_data_connector_name": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["default_identifier_name"],
                        }
                    },
                }
            },
        )
    )

    checkpoint = context.get_checkpoint("orders_checkpoint")
    results = checkpoint.run()
    return results.success


@asset(deps=[validate_orders])
def run_dbt_models():
    """Run dbt models to create daily sales report"""
    os.system("cd /opt/dagster/app/dbt && dbt run --models marts.daily_sales_by_store")
    return True


daily_sales_job = define_asset_job(
    name="daily_sales_job",
    selection=["validate_orders", "run_dbt_models"],
)

daily_schedule = ScheduleDefinition(
    job=daily_sales_job,
    cron_schedule="0 1 * * *",  # Run at 1 AM every day
)

defs = Definitions(assets=[validate_orders, run_dbt_models], schedules=[daily_schedule])
