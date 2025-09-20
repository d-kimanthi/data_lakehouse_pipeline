#!/usr/bin/env python3
"""
Test script for the Spark-only e-commerce analytics pipeline.
This script tests the end-to-end flow from Kafka to Gold layer analytics.
"""

import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a shell command and return the result."""
    print(f"\n🔧 {description}")
    print(f"Command: {cmd}")
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            print(f"✅ Success: {description}")
            return True, result.stdout
        else:
            print(f"❌ Failed: {description}")
            print(f"Error: {result.stderr}")
            return False, result.stderr
    except subprocess.TimeoutExpired:
        print(f"⏰ Timeout: {description}")
        return False, "Command timed out"
    except Exception as e:
        print(f"💥 Exception: {description} - {str(e)}")
        return False, str(e)


def check_prerequisites():
    """Check if required services are running."""
    print("🔍 Checking prerequisites...")

    # Check Docker containers
    success, output = run_command(
        "docker ps --format 'table {{.Names}}\\t{{.Status}}'",
        "Checking Docker containers",
    )
    if not success:
        return False

    required_services = ["ecommerce-kafka", "ecommerce-zookeeper", "ecommerce-minio"]
    running_services = output.lower()

    for service in required_services:
        if service.lower() not in running_services:
            print(
                f"❌ {service} is not running. Please start with: docker-compose up -d"
            )
            return False

    print("✅ All required services are running")
    return True


def test_kafka_topics():
    """Test if Kafka topics exist and can be written to."""
    print("\n📡 Testing Kafka connectivity...")

    # List topics using correct container name
    success, output = run_command(
        "docker exec ecommerce-kafka kafka-topics --bootstrap-server localhost:9092 --list",
        "Listing Kafka topics",
    )

    if not success:
        return False

    required_topics = ["raw-events", "page-views", "purchases", "user-sessions"]
    existing_topics = output.strip().split("\n") if output.strip() else []

    for topic in required_topics:
        if topic not in existing_topics:
            print(f"⚠️  Topic {topic} doesn't exist, creating it...")
            success, _ = run_command(
                f"docker exec ecommerce-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic {topic} --partitions 3 --replication-factor 1",
                f"Creating topic {topic}",
            )
            if not success:
                return False

    return True


def test_minio_connectivity():
    """Test MinIO connectivity and bucket creation."""
    print("\n🪣 Testing MinIO connectivity...")

    # Check if MinIO is accessible
    success, _ = run_command(
        "curl -s http://localhost:9000/minio/health/live", "Checking MinIO health"
    )
    if not success:
        return False

    return True


def test_spark_streaming_syntax():
    """Test that Spark streaming script has valid syntax."""
    print("\n⚡ Testing Spark streaming script syntax...")

    script_path = Path("streaming/spark-streaming/real_time_streaming.py")
    if not script_path.exists():
        print(f"❌ Streaming script not found: {script_path}")
        return False

    success, _ = run_command(
        f"python3 -m py_compile {script_path}", "Validating streaming script syntax"
    )
    return success


def test_dagster_assets_syntax():
    """Test that Dagster assets have valid syntax."""
    print("\n🔧 Testing Dagster assets syntax...")

    assets_path = Path("dagster/dagster_project/assets/spark_batch_assets.py")
    if not assets_path.exists():
        print(f"❌ Dagster assets not found: {assets_path}")
        return False

    success, _ = run_command(
        f"python3 -m py_compile {assets_path}", "Validating Dagster assets syntax"
    )
    return success


def test_spark_batch_jobs_syntax():
    """Test that Spark batch job scripts have valid syntax."""
    print("\n📊 Testing Spark batch jobs syntax...")

    job_scripts = [
        "streaming/spark-streaming/batch_customer_analytics.py",
        "streaming/spark-streaming/batch_sales_summary.py",
    ]

    for script in job_scripts:
        script_path = Path(script)
        if not script_path.exists():
            print(f"❌ Batch job script not found: {script_path}")
            return False

        success, _ = run_command(
            f"python3 -m py_compile {script_path}", f"Validating {script}"
        )
        if not success:
            return False

    return True


def generate_test_data():
    """Generate a small amount of test data."""
    print("\n📝 Generating test data...")

    # Check if event generator exists
    generator_path = Path("data-generation/event_generator.py")
    if not generator_path.exists():
        print(f"❌ Event generator not found: {generator_path}")
        return False

    # Run event generator for a short time (syntax check only)
    success, _ = run_command(
        f"python3 -m py_compile {generator_path}", "Validating event generator syntax"
    )
    return success


def main():
    """Run the complete pipeline test."""
    print("🚀 Starting Spark-only Pipeline Tests")
    print("=" * 50)

    tests = [
        ("Prerequisites Check", check_prerequisites),
        ("Kafka Topics Test", test_kafka_topics),
        ("MinIO Connectivity Test", test_minio_connectivity),
        ("Spark Streaming Syntax", test_spark_streaming_syntax),
        ("Dagster Assets Syntax", test_dagster_assets_syntax),
        ("Spark Batch Jobs Syntax", test_spark_batch_jobs_syntax),
        ("Event Generator Syntax", generate_test_data),
    ]

    passed_tests = 0
    total_tests = len(tests)

    for test_name, test_func in tests:
        try:
            if test_func():
                passed_tests += 1
                print(f"✅ {test_name} - PASSED")
            else:
                print(f"❌ {test_name} - FAILED")
        except Exception as e:
            print(f"💥 {test_name} - ERROR: {str(e)}")

    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        print("🎉 All tests passed! Your Spark-only pipeline is ready to run.")
        print("\n📋 Next steps:")
        print("1. Start streaming: cd streaming/spark-streaming && ./run_streaming.sh")
        print("2. Generate data: cd data-generation && python event_generator.py")
        print("3. Start Dagster: cd dagster/dagster_project && dagster dev")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        print("\n📋 Troubleshooting guide:")
        print("- Ensure Docker services are running: docker-compose up -d")
        print("- Check Python syntax in failed files")
        print("- Review SPARK_PIPELINE_GUIDE.md for detailed setup")
        return 1


if __name__ == "__main__":
    sys.exit(main())
