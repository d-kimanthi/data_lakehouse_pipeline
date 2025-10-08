#!/usr/bin/env python3
"""
Test script to verify MinIO connectivity from Spark
"""

from pyspark.sql import SparkSession


def test_minio_connection():
    """Test connection to MinIO from Spark"""
    print("Creating Spark session...")

    spark = (
        SparkSession.builder.appName("MinIOConnectionTest")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://data-lake/warehouse/")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    try:
        print("Testing MinIO connection...")

        # Try to list tables in the iceberg catalog
        print("Available catalogs:", spark.catalog.listCatalogs())

        # Try to create a simple test table
        test_data = [("test", 1), ("data", 2)]
        df = spark.createDataFrame(test_data, ["name", "value"])

        print(f"Created test DataFrame with {df.count()} rows")

        # Try to write to MinIO/Iceberg
        print("Attempting to write to gold layer...")
        df.write.mode("overwrite").saveAsTable("iceberg.gold.connection_test")

        print("✅ MinIO connection successful!")

        # Output result for Dagster
        print("DAGSTER_RESULT:{'status': 'success', 'test_records': 2}")

    except Exception as e:
        print(f"❌ MinIO connection failed: {e}")
        print("DAGSTER_RESULT:{'status': 'failed', 'error': '" + str(e) + "'}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    test_minio_connection()
