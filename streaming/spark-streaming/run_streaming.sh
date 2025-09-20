#!/bin/bash
# Script to run the Spark streaming job with all required configurations

echo "🚀 Starting Spark Streaming Job for E-commerce Analytics"
echo "================================================="

# Set SPARK_HOME if not set
if [ -z "$SPARK_HOME" ]; then
    echo "⚠️  SPARK_HOME not set. Please ensure Spark is installed and SPARK_HOME is configured."
    echo "Example: export SPARK_HOME=/path/to/spark"
    exit 1
fi

# Check if required JARs exist
JARS_DIR="../../../jars"
if [ ! -d "$JARS_DIR" ]; then
    echo "⚠️  JARs directory not found: $JARS_DIR"
    echo "Please ensure the required JAR files are available."
fi

echo "📦 Using Spark packages and JARs:"
echo "  - Iceberg Spark Runtime"
echo "  - Spark SQL Kafka"
echo "  - S3A filesystem support"

# Run the Spark streaming job
$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" \
  --jars "$JARS_DIR/aws-java-sdk-bundle-1.12.262.jar,$JARS_DIR/hadoop-aws-3.3.4.jar" \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://warehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minio \
  --conf spark.hadoop.fs.s3a.secret.key=minio123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --driver-memory 2g \
  --executor-memory 2g \
  --name "ECommerceStreamingAnalytics" \
  real_time_streaming.py

echo "✅ Spark streaming job completed or stopped."