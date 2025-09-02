#!/usr/bin/env bash
set -euo pipefail

: "${AWS_ACCESS_KEY_ID:?Set in .env}"
: "${AWS_SECRET_ACCESS_KEY:?Set in .env}"
: "${AWS_REGION:?Set in .env}"
: "${S3_BUCKET:?Set in .env}"

# Create DynamoDB table for Iceberg locks if it doesn't exist
aws dynamodb create-table \
    --table-name iceberg_locks \
    --attribute-definitions AttributeName=lock,AttributeType=S \
    --key-schema AttributeName=lock,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region "${AWS_REGION}" || true

# Register tables in Glue Catalog
python catalog/register_tables.py
