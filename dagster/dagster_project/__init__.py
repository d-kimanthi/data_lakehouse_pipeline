"""
E-Commerce Streaming Analytics Dagster Project

This package contains Dagster assets and jobs for orchestrating
the silver to gold data transformations using Spark.
"""

from dagster import Definitions
from dagster.dagster_project.assets import silver_to_gold_assets
from dagster.dagster_project.data_quality import data_quality_checks
from dagster.dagster_project.jobs import silver_to_gold_jobs, silver_to_gold_schedules
from dagster.dagster_project.resources import spark_cluster_resource

# Define all assets, jobs, schedules, checks and resources
defs = Definitions(
    assets=silver_to_gold_assets,
    jobs=silver_to_gold_jobs,
    schedules=silver_to_gold_schedules,
    asset_checks=data_quality_checks,
    resources={
        "spark_cluster": spark_cluster_resource,
    },
)
