from dagster import resource, InitResourceContext
from pyspark.sql import SparkSession
import os


@resource(config_schema={"spark_conf": dict, "app_name": str, "master": str})
def spark_resource(init_context: InitResourceContext):
    """
    Spark session resource for Dagster
    """
    config = init_context.resource_config

    spark_conf = config.get("spark_conf", {})
    app_name = config.get("app_name", "dagster-spark")
    master = config.get("master", "local[*]")

    # Base Spark configuration
    base_conf = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.type": "hive",
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "glue",
        "spark.sql.catalog.iceberg.warehouse": f"s3://{os.getenv('S3_BUCKET', 'default-bucket')}/iceberg-warehouse/",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }

    # Merge with user-provided config
    final_conf = {**base_conf, **spark_conf}

    # Create Spark session
    builder = SparkSession.builder.appName(app_name).master(master)

    for key, value in final_conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        yield spark
    finally:
        spark.stop()
