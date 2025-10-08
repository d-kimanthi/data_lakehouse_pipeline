#!/usr/bin/env python3
"""
Test script to verify Spark job submission with Java 21 integration.
This script tests the Dagster Spark integration without using the full Dagster UI.
"""

import os
import subprocess
import sys


def test_spark_submit():
    """Test basic spark-submit functionality"""
    print("Testing Spark Submit Integration...")

    # Test environment setup
    env = os.environ.copy()
    env.update(
        {
            "JAVA_HOME": "/usr/lib/jvm/java-21-openjdk-arm64",
            "SPARK_HOME": "/opt/spark",
            "PYSPARK_PYTHON": "python3",
            "PYSPARK_DRIVER_PYTHON": "python3",
        }
    )

    # Basic spark-submit command to test connectivity
    spark_cmd = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--executor-memory",
        "512m",
        "--driver-memory",
        "512m",
        "--total-executor-cores",
        "1",
        "--conf",
        "spark.sql.adaptive.enabled=false",
        "--py-files",
        "/opt/spark/python/lib/pyspark.zip",
        "--class",
        "org.apache.spark.deploy.SparkSubmit",
    ]

    # Create a simple test PySpark script
    test_script = """
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("JavaIntegrationTest") \\
    .getOrCreate()

# Simple test - create a small DataFrame
data = [("test1", 1), ("test2", 2)]
df = spark.createDataFrame(data, ["name", "value"])
print(f"Test successful! Row count: {df.count()}")

spark.stop()
"""

    with open("/tmp/test_spark.py", "w") as f:
        f.write(test_script)

    spark_cmd.append("/tmp/test_spark.py")

    print(f"Executing: {' '.join(spark_cmd)}")
    print(f"Environment JAVA_HOME: {env.get('JAVA_HOME')}")

    try:
        result = subprocess.run(
            spark_cmd, env=env, capture_output=True, text=True, timeout=60
        )

        print(f"Return code: {result.returncode}")
        print(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            print(f"STDERR:\n{result.stderr}")

        return result.returncode == 0

    except subprocess.TimeoutExpired:
        print("Command timed out after 60 seconds")
        return False
    except Exception as e:
        print(f"Error executing spark-submit: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Spark Java Integration Test")
    print("=" * 60)

    success = test_spark_submit()

    if success:
        print("\n✅ Spark Java integration test PASSED!")
        sys.exit(0)
    else:
        print("\n❌ Spark Java integration test FAILED!")
        sys.exit(1)
