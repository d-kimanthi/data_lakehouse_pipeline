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
  spark-master spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://${S3_BUCKET}/warehouse \
  -e "SELECT * FROM local.retail.orders ORDER BY event_time DESC LIMIT 20"
