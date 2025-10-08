#!/usr/bin/env python3
"""
Test Dagster asset execution after fixing error handling
"""

import subprocess
import sys


def test_asset_execution():
    """Test a single asset execution"""
    print("Testing Dagster asset execution...")

    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "dagster-daemon",
        "python3",
        "-c",
        """
import sys
sys.path.append('/opt/dagster/app/dagster/dagster_project')

from assets import daily_customer_analytics_asset
from resources import SparkClusterResource
from dagster import build_asset_context

# Create mock context and resources
context = build_asset_context()
spark_cluster = SparkClusterResource()

try:
    result = daily_customer_analytics_asset(context, spark_cluster)
    print("SUCCESS: Asset executed successfully!")
    print(f"Result metadata: {result.metadata}")
except Exception as e:
    print(f"ERROR: {e}")
    raise
""",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        print(f"Return code: {result.returncode}")
        print(f"STDOUT: {result.stdout}")
        if result.stderr:
            print(f"STDERR: {result.stderr}")
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("Test timed out")
        return False
    except Exception as e:
        print(f"Test error: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Dagster Asset Execution")
    print("=" * 60)

    success = test_asset_execution()
    sys.exit(0 if success else 1)
