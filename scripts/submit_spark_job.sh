#!/bin/bash

echo "Submitting Spark Streaming Job..."

# Load environment variables
source .env

SPARK_SUBMIT_ARGS="
--class org.apache.spark.sql.execution.streaming.StreamExecution
--master ${SPARK_MASTER_URL:-spark://localhost:7077}
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.565
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
--conf spark.sql.catalog.spark_catalog.type=hive
--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.iceberg.type=glue
--conf spark.sql.catalog.iceberg.warehouse=s3://${S3_BUCKET}/iceberg-warehouse/
--conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT:-http://localhost:9000}
--conf spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY:-minioadmin}
--conf spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY:-minioadmin}
--conf spark.hadoop.fs.s3a.path.style.access=true
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
"

# For local development with Docker
if [ "$1" == "local" ]; then
    docker exec spark-master spark-submit $SPARK_SUBMIT_ARGS \
        /opt/spark-apps/spark-streaming/streaming_job.py \
        --kafka-servers kafka:29092 \
        --kafka-topic raw-events \
        --s3-bucket ${MINIO_BUCKET:-ecommerce-data-lake}
else
    # For direct submission (if running Spark locally)
    spark-submit $SPARK_SUBMIT_ARGS \
        streaming/spark-streaming/streaming_job.py \
        --kafka-servers ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
        --kafka-topic ${KAFKA_TOPIC_RAW_EVENTS:-raw-events} \
        --s3-bucket ${S3_BUCKET:-ecommerce-data-lake}
fi

echo "Spark job submitted. Check Spark UI at http://localhost:8080 for status."
