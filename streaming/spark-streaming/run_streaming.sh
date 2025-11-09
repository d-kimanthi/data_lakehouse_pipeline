#!/bin/bash
# Script to run the Spark streaming job in cluster mode
# Submits to Docker Spark cluster with all required configurations
#
# This script runs ONLY in cluster mode using Docker service names.
# Requirements:
#   - Docker Spark cluster must be running
#   - Spark Master UI accessible at http://localhost:7080
#
# Usage: ./run_streaming.sh [--job-type TYPE]
#   Job types: ingestion, processing, both (default)

echo "Starting Spark Streaming Job for E-commerce Analytics (Cluster Mode)"
echo "====================================================================="

# Parse command line arguments
JOB_TYPE="both"


# Cluster configuration
SPARK_MASTER="spark://spark-master:7077"

# Submit job from within the cluster
echo "🚀 Submitting job to Spark cluster..."

docker exec ecommerce-spark-master /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.0" \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.iceberg.uri=http://nessie:19120/api/v1 \
    --conf spark.sql.catalog.iceberg.ref=main \
    --conf spark.sql.catalog.iceberg.warehouse=s3a://data-lake/warehouse/ \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=false \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --total-executor-cores 2 \
    --name "ECommerceStreamingAnalytics-${JOB_TYPE}" \
    /tmp/real_time_streaming.py --job-type $JOB_TYPE
