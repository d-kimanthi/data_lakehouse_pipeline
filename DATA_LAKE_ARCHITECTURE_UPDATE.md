# Data Lake Architecture Update

## Overview

Migrated from multiple separate buckets to a single unified bucket with organized subfolder structure. This follows industry best practices and simplifies management.

## Before vs After

### ❌ Before (Multiple Buckets)

```
MinIO:
├── warehouse/           # Separate bucket
├── bronze/             # Separate bucket
├── silver/             # Separate bucket
├── gold/               # Separate bucket
├── checkpoints/        # Separate bucket
├── logs/               # Separate bucket
└── scripts/            # Separate bucket
```

### ✅ After (Single Bucket + Subfolders)

```
MinIO:
└── data-lake/
    ├── warehouse/      # Iceberg warehouse metadata
    ├── bronze/         # Raw ingested data
    ├── silver/         # Cleaned, validated data
    ├── gold/           # Analytics-ready aggregated data
    ├── checkpoints/    # Spark streaming checkpoints
    ├── logs/           # Application and audit logs
    └── scripts/        # ETL scripts and utilities
```

## Benefits of Single Bucket Architecture

### 🎯 **Management**

- **Simplified Permissions**: Single bucket-level IAM policies
- **Unified Monitoring**: One place to track all data lake activity
- **Easier Backup/Restore**: Single bucket lifecycle management
- **Cost Tracking**: Consolidated billing and storage analytics

### 🔒 **Security**

- **Centralized Access Control**: Folder-level permissions within single bucket
- **Consistent Encryption**: Single encryption policy for entire data lake
- **Audit Trail**: Unified logging for all data lake operations

### 🚀 **Operations**

- **Cleaner URLs**: `s3a://data-lake/bronze/` vs `s3a://bronze/`
- **Industry Standard**: Matches AWS, Azure, GCP data lake patterns
- **Scalability**: Easier to add new layers (e.g., `platinum/`, `archive/`)

## Configuration Changes

### MinIO Bucket Creation

```bash
# Before: Multiple buckets
mc mb myminio/warehouse
mc mb myminio/bronze
mc mb myminio/silver
# ... etc

# After: Single bucket
mc mb myminio/data-lake
```

### Spark Configuration

```bash
# Before: Multiple warehouse paths
--conf spark.sql.catalog.local.warehouse=s3a://warehouse/
--conf spark.sql.catalog.iceberg.warehouse=s3a://bronze/iceberg-warehouse/

# After: Unified warehouse path
--conf spark.sql.catalog.local.warehouse=s3a://data-lake/warehouse/
--conf spark.sql.catalog.iceberg.warehouse=s3a://data-lake/warehouse/
```

### Checkpoint Locations

```python
# Before: Local temp directories
.option("checkpointLocation", "/tmp/checkpoints/bronze")

# After: S3 persistent storage
.option("checkpointLocation", "s3a://data-lake/checkpoints/bronze")
```

## File Organization Best Practices

### 📁 **Folder Structure Guidelines**

```
data-lake/
├── warehouse/
│   ├── bronze.db/          # Iceberg database metadata
│   ├── silver.db/          # Iceberg database metadata
│   └── gold.db/            # Iceberg database metadata
├── bronze/
│   ├── raw_events/         # Raw event data
│   ├── page_views/         # Page view events
│   └── purchases/          # Purchase events
├── silver/
│   ├── cleaned_events/     # Validated events
│   ├── user_sessions/      # Sessionized data
│   └── product_catalog/    # Clean product data
├── gold/
│   ├── daily_sales/        # Daily aggregations
│   ├── user_analytics/     # User behavior metrics
│   └── product_performance/ # Product metrics
├── checkpoints/
│   ├── bronze/             # Bronze streaming checkpoints
│   ├── silver_page_views/  # Silver page views checkpoints
│   └── silver_purchases/   # Silver purchases checkpoints
├── logs/
│   ├── spark/              # Spark application logs
│   ├── dagster/            # Dagster pipeline logs
│   └── kafka/              # Kafka consumer logs
└── scripts/
    ├── etl/                # ETL job scripts
    ├── maintenance/        # Maintenance scripts
    └── monitoring/         # Monitoring utilities
```

## Implementation Status

### ✅ **Completed Updates**

- [x] Docker Compose bucket creation
- [x] Spark run script configuration
- [x] Real-time streaming checkpoint paths
- [x] Bucket creation utility script
- [x] Documentation updates

### 🎯 **Key Paths Updated**

| Component   | Old Path                    | New Path                                |
| ----------- | --------------------------- | --------------------------------------- |
| Warehouse   | `s3a://warehouse/`          | `s3a://data-lake/warehouse/`            |
| Checkpoints | `/tmp/checkpoints/`         | `s3a://data-lake/checkpoints/`          |
| Raw Events  | `iceberg.bronze.raw_events` | `iceberg.bronze.raw_events` (unchanged) |

### 🔄 **Backward Compatibility**

- Iceberg table names remain unchanged (`iceberg.bronze.*`, `iceberg.silver.*`)
- Logical schema organization preserved
- Only physical storage paths updated

## Migration Steps

### For New Deployments

1. Start services: `docker-compose up -d`
2. Verify bucket: Access http://localhost:9001
3. Run streaming: `./run_streaming.sh --cluster`

### For Existing Deployments

1. **Backup existing data** (if any)
2. **Stop services**: `docker-compose down`
3. **Update configuration** (already done)
4. **Restart services**: `docker-compose up -d`
5. **Verify new structure**: `./scripts/create_minio_buckets.sh`

## Monitoring and Verification

### MinIO Console

- URL: http://localhost:9001
- Credentials: `minioadmin` / `minioadmin`
- Should show single `data-lake` bucket

### Expected Structure

```bash
# List bucket contents
mc ls myminio/data-lake/

# Should show folders as they're created:
# PRE warehouse/
# PRE checkpoints/
# (other folders appear as data is written)
```

## Next Steps

1. **Test streaming pipeline** with new bucket structure
2. **Update Dagster assets** to use new paths (if needed)
3. **Create monitoring dashboards** for unified bucket metrics
4. **Implement data retention policies** at folder level
5. **Add data catalog integration** for improved discoverability
