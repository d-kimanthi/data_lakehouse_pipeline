#!/usr/bin/env bash
set -euo pipefail

: "${AWS_ACCESS_KEY_ID:?Set in .env}"
: "${AWS_SECRET_ACCESS_KEY:?Set in .env}"
: "${AWS_REGION:?Set in .env}"
: "${S3_BUCKET:?Set in .env}"

docker compose exec \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION \
  -e S3_BUCKET \
  spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,\
org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://${S3_BUCKET}/warehouse \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider \
  --conf spark.hadoop.fs.s3a.endpoint=s3.${AWS_REGION}.amazonaws.com \
  --conf spark.hadoop.fs.s3a.path.style.access=false \
  /opt/spark-app/stream_to_iceberg.py
echo "Spark job submitted to process Kafka data into Iceberg on S3"