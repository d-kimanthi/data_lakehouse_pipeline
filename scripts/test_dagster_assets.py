#!/usr/bin/env python3
"""
Test script for Dagster assets
Tests the Kafka ingestion pipeline assets locally
"""

import os
import sys
import tempfile
from pathlib import Path

# Add the dagster project to Python path
sys.path.append(
    os.path.join(os.path.dirname(__file__), "..", "dagster", "dagster_project")
)


def test_kafka_ingestion_assets():
    """Test our Kafka ingestion assets can be imported and have correct structure"""
    print("🧪 Testing Dagster Kafka Ingestion Assets")
    print("=" * 50)

    try:
        # Test imports
        print("📦 Testing asset imports...")
        from assets.kafka_ingestion import (
            gold_daily_customer_metrics,
            kafka_raw_events,
            minio_storage_setup,
            silver_page_views,
            silver_purchases,
        )

        print("✅ All assets imported successfully!")

        # Test asset metadata
        assets = [
            ("kafka_raw_events", kafka_raw_events),
            ("silver_page_views", silver_page_views),
            ("silver_purchases", silver_purchases),
            ("gold_daily_customer_metrics", gold_daily_customer_metrics),
            ("minio_storage_setup", minio_storage_setup),
        ]

        print("\n🔍 Asset Details:")
        for name, asset_def in assets:
            # Get asset information
            description = getattr(asset_def, "_description", "No description")
            group_name = getattr(asset_def, "_group_name", "default")

            print(f"  📊 {name}")
            print(f"     Description: {description}")
            print(f"     Group: {group_name}")

        return True

    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_simple_asset_execution():
    """Test a simple asset execution (MinIO setup) without dependencies"""
    print("\n🧪 Testing Simple Asset Execution")
    print("=" * 50)

    try:
        from assets.kafka_ingestion import minio_storage_setup

        from dagster import build_asset_context

        # Create a simple context for testing
        context = build_asset_context()

        print("🔧 Testing MinIO storage setup asset...")
        result = minio_storage_setup(context)

        print("✅ MinIO setup asset executed successfully!")
        print(f"   Result metadata: {result.metadata}")

        return True

    except Exception as e:
        print(f"❌ Asset execution failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_dagster_definitions():
    """Test that our Dagster definitions load correctly"""
    print("\n🧪 Testing Dagster Definitions")
    print("=" * 50)

    try:
        # This would test loading the full Dagster definitions
        # For now, we'll just verify the structure
        print("📋 Checking project structure...")

        project_path = Path(__file__).parent.parent / "dagster" / "dagster_project"

        required_files = [
            "__init__.py",
            "assets/kafka_ingestion.py",
            "jobs/__init__.py",
            "resources/kafka_resource.py",
            "workspace.yaml",
        ]

        missing_files = []
        for file in required_files:
            if not (project_path / file).exists():
                missing_files.append(file)

        if missing_files:
            print(f"❌ Missing required files: {missing_files}")
            return False

        print("✅ All required files present")

        # Test basic imports
        print("📦 Testing basic Dagster imports...")
        from assets import kafka_ingestion

        print("✅ Dagster project structure is valid!")
        return True

    except Exception as e:
        print(f"❌ Definitions test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("🚀 Dagster Assets Test Suite")
    print("=" * 50)

    tests = [
        ("Asset Imports", test_kafka_ingestion_assets),
        ("Simple Asset Execution", test_simple_asset_execution),
        ("Dagster Definitions", test_dagster_definitions),
    ]

    results = []
    for test_name, test_func in tests:
        success = test_func()
        results.append((test_name, success))

    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    print("=" * 50)

    all_passed = True
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status:8} {test_name}")
        if not success:
            all_passed = False

    if all_passed:
        print("\n🎉 All tests passed! Dagster assets are ready.")
        print("\n💡 Next steps:")
        print("   1. Start Dagster UI: docker compose up -d dagster")
        print("   2. Access UI: http://localhost:3000")
        print("   3. Materialize assets manually or set up schedules")
        print("   4. Generate events with: python3 scripts/test_event_pipeline.py")
    else:
        print("\n⚠️  Some tests failed. Check the logs above for details.")

    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
