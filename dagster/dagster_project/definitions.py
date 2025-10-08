"""
E-Commerce Streaming Analytics Dagster Definitions

This file contains all Dagster definitions for the silver to gold pipeline.
It's designed to be loaded directly by Dagster without import issues.
"""

import os
import sys

# Add the current directory to Python path to enable relative imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from assets import pipes_assets
from jobs import silver_to_gold_jobs, silver_to_gold_schedules

# Import all components using the direct approach
from resources import spark_cluster_resource

from dagster import Definitions, PipesSubprocessClient

# Define all assets, jobs, schedules, checks and resources
defs = Definitions(
    assets=pipes_assets,
    jobs=silver_to_gold_jobs,
    schedules=silver_to_gold_schedules,
    resources={
        "spark_cluster": spark_cluster_resource,
        "pipes_subprocess_client": PipesSubprocessClient(),
    },
)
