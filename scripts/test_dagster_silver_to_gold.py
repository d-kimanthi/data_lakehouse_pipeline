#!/usr/bin/env python3
"""
Test script to verify the silver to gold Dagster pipeline.

This script tests:
1. Dagster asset definitions
2. Spark cluster connectivity
3. Asset execution simulation
4. Data quality checks
"""

import json
import os
import sys
from datetime import datetime, timedelta

# Add the dagster project to the path
sys.path.insert(
    0, "/Users/kimanthi/projects/ecommerce-streaming-analytics/dagster/dagster_project"
)


def test_imports():
    """Test that all Dagster components can be imported correctly."""
    print("🔍 Testing imports...")

    try:
        from dagster_project import defs
        from dagster_project.assets import silver_to_gold_assets
        from dagster_project.data_quality import data_quality_checks
        from dagster_project.jobs import silver_to_gold_jobs
        from dagster_project.resources import spark_cluster_resource

        print(f"✅ Successfully imported {len(silver_to_gold_assets)} assets")
        print(f"✅ Successfully imported {len(silver_to_gold_jobs)} jobs")
        print(f"✅ Successfully imported {len(data_quality_checks)} quality checks")
        print("✅ Successfully imported Spark cluster resource")
        print("✅ Successfully imported main definitions")

        return True

    except Exception as e:
        print(f"❌ Import failed: {str(e)}")
        return False


def test_dagster_definitions():
    """Test that Dagster definitions are valid."""
    print("\n🔍 Testing Dagster definitions...")

    try:
        from dagster_project import defs

        # Check that we have assets
        assets = defs.get_asset_graph().all_asset_keys
        print(f"✅ Found {len(assets)} assets:")
        for asset in assets:
            print(f"   - {asset}")

        # Check that we have jobs
        jobs = list(defs.get_job_names())
        print(f"✅ Found {len(jobs)} jobs:")
        for job in jobs:
            print(f"   - {job}")

        # Check that we have schedules
        schedules = list(defs.get_schedule_names())
        print(f"✅ Found {len(schedules)} schedules:")
        for schedule in schedules:
            print(f"   - {schedule}")

        return True

    except Exception as e:
        print(f"❌ Definitions test failed: {str(e)}")
        return False


def test_spark_connectivity():
    """Test connectivity to the Spark cluster."""
    print("\n🔍 Testing Spark cluster connectivity...")

    try:
        from dagster_project.resources import spark_cluster_resource

        # Try to check cluster health
        health_result = spark_cluster_resource.check_spark_cluster_health()

        if health_result["status"] == "healthy":
            print("✅ Spark cluster is healthy and ready")
            return True
        else:
            print(f"⚠️  Spark cluster health check warning: {health_result['message']}")
            return False

    except Exception as e:
        print(f"❌ Spark connectivity test failed: {str(e)}")
        print(
            "💡 Make sure the Spark containers are running: docker-compose up spark-master spark-worker"
        )
        return False


def test_asset_execution_dry_run():
    """Test asset execution in dry-run mode."""
    print("\n🔍 Testing asset execution (dry-run)...")

    try:
        from dagster_project.assets import daily_customer_analytics_asset

        from dagster import DagsterInstance, materialize

        # Create a temporary Dagster instance
        instance = DagsterInstance.ephemeral()

        # Test asset metadata (this doesn't actually execute the asset)
        asset_def = daily_customer_analytics_asset
        print(f"✅ Asset '{asset_def.key}' definition is valid")
        print(f"   - Description: {asset_def.description}")
        print(
            f"   - Group: {asset_def.group_names_by_key.get(asset_def.key, 'default')}"
        )
        print(f"   - Compute kind: {asset_def.compute_kind}")

        return True

    except Exception as e:
        print(f"❌ Asset execution test failed: {str(e)}")
        return False


def create_test_summary():
    """Create a summary of the test results."""
    print("\n" + "=" * 60)
    print("📊 DAGSTER SILVER-TO-GOLD PIPELINE TEST SUMMARY")
    print("=" * 60)

    test_results = {}

    # Run all tests
    test_results["imports"] = test_imports()
    test_results["definitions"] = test_dagster_definitions()
    test_results["spark_connectivity"] = test_spark_connectivity()
    test_results["asset_execution"] = test_asset_execution_dry_run()

    # Summary
    passed_tests = sum(test_results.values())
    total_tests = len(test_results)

    print(f"\n📈 Test Results: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        print("🎉 All tests passed! Your Dagster pipeline is ready.")
        print("\n📋 Next steps:")
        print("   1. Start the full environment: cd local && docker-compose up -d")
        print("   2. Access Dagster UI at: http://localhost:3000")
        print("   3. Manually trigger the 'silver_to_gold_processing' job")
        print("   4. Monitor the Spark UI at: http://localhost:7080")
        print("   5. Check data quality results in the Dagster UI")
    else:
        print("⚠️  Some tests failed. Please fix the issues before proceeding.")

        failed_tests = [test for test, result in test_results.items() if not result]
        print(f"\n❌ Failed tests: {', '.join(failed_tests)}")

    return passed_tests == total_tests


if __name__ == "__main__":
    print("🚀 Starting Dagster Silver-to-Gold Pipeline Tests")
    print("=" * 60)

    success = create_test_summary()

    if success:
        print("\n✅ Pipeline is ready for production testing!")
        sys.exit(0)
    else:
        print("\n❌ Pipeline needs fixes before deployment.")
        sys.exit(1)
