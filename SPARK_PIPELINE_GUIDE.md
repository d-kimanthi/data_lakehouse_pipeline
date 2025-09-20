# Spark-Only E-commerce Analytics Pipeline Guide

## Architecture Overview

This project uses a **Spark-only architecture** for both real-time and batch processing:

### Real-time Layer (Kafka → Bronze/Silver)

- **Spark Streaming** consumes events from Kafka topics
- **Direct processing** to Bronze and Silver Iceberg tables
- **Real-time analytics** available immediately

### Batch Layer (Silver → Gold)

- **Dagster orchestrates** daily Spark batch jobs
- **Spark SQL** for complex analytics and aggregations
- **Gold layer** for business-ready datasets

## Pipeline Components

### 1. Real-time Streaming (`streaming/spark-streaming/real_time_streaming.py`)

```bash
# Processes these Kafka topics:
- raw-events          → bronze_events (Iceberg)
- page-views         → silver_page_views (Iceberg)
- purchases          → silver_purchases (Iceberg)
- user-sessions      → silver_user_sessions (Iceberg)
```

**Key Features:**

- Auto-creates Iceberg tables with partitioning
- Real-time data enrichment and validation
- Handles late-arriving data with watermarks
- Checkpointing for fault tolerance

### 2. Batch Processing (Dagster + Spark)

#### Assets (`dagster/dagster_project/assets/spark_batch_assets.py`)

- `gold_daily_customer_analytics` - Customer segmentation and behavior analysis
- `gold_daily_sales_summary` - Sales KPIs, conversion rates, traffic analysis
- `gold_product_analytics` - Product performance and recommendations
- `silver_data_quality_report` - Data validation and quality metrics
- `streaming_health_monitor` - Pipeline monitoring and alerting

#### Spark Jobs (`streaming/spark-streaming/`)

- `batch_customer_analytics.py` - Daily customer analytics processing
- `batch_sales_summary.py` - Daily sales summary and KPIs

## Running the Pipeline

### Prerequisites

```bash
# Ensure you have the required services running
docker-compose up -d  # MinIO, Kafka, Zookeeper
```

### 1. Start Real-time Streaming

```bash
cd streaming/spark-streaming

# Submit the Spark streaming job
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
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
  real_time_streaming.py
```

### 2. Generate Sample Data

```bash
# In another terminal, start generating events
cd data-generation
python event_generator.py
```

### 3. Start Dagster for Batch Processing

```bash
cd dagster/dagster_project

# Start Dagster UI
dagster dev

# Access UI at: http://localhost:3000
```

### 4. Schedule Daily Batch Jobs

In the Dagster UI:

1. Navigate to **Assets** tab
2. Find the Gold layer assets:
   - `gold_daily_customer_analytics`
   - `gold_daily_sales_summary`
   - `gold_product_analytics`
3. Click **Materialize** to run manually or set up schedules

## Data Flow

```
Kafka Topics
    ↓
[Spark Streaming] → Bronze Tables (Iceberg)
    ↓                      ↓
Silver Tables ←────────────┘
    ↓
[Dagster + Spark Batch Jobs]
    ↓
Gold Tables (Business Analytics)
```

### Table Structure

#### Bronze Layer

- `local.bronze_events` - Raw events with minimal processing

#### Silver Layer

- `local.silver_page_views` - Cleaned page view events
- `local.silver_purchases` - Validated purchase transactions
- `local.silver_user_sessions` - Session-aggregated user behavior

#### Gold Layer

- `local.gold_daily_customer_analytics` - Customer segments, LTV, behavior metrics
- `local.gold_daily_sales_summary` - Sales KPIs, conversion rates, revenue metrics
- `local.gold_product_analytics` - Product performance, recommendations

## Monitoring and Troubleshooting

### Check Streaming Job Status

```bash
# Spark UI (if running locally)
http://localhost:4040

# Check streaming query status
# Look for: isDataAvailable, inputRowsPerSecond, processedRowsPerSecond
```

### Verify Iceberg Tables

```bash
# Connect to Spark shell with Iceberg
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://warehouse/

# Check table schemas and data
spark.sql("SHOW TABLES IN local").show()
spark.sql("SELECT COUNT(*) FROM local.bronze_events").show()
spark.sql("SELECT COUNT(*) FROM local.silver_page_views").show()
```

### Check MinIO Storage

- UI: http://localhost:9001
- Credentials: minio / minio123
- Look for `/warehouse/` bucket with Iceberg table files

### Dagster Monitoring

- UI: http://localhost:3000
- Check **Runs** tab for batch job execution history
- Use **Asset Lineage** to visualize data dependencies
- Set up **Alerts** for failed materializations

## Performance Tuning

### Streaming Optimization

- Adjust `maxOffsetsPerTrigger` for throughput vs. latency
- Tune checkpoint intervals based on data volume
- Configure Kafka consumer settings for batch size

### Batch Processing Optimization

- Partition pruning with date-based filtering
- Coalesce small files with `coalesce()` or `repartition()`
- Cache intermediate DataFrames for complex transformations
- Use broadcast joins for small lookup tables

## Troubleshooting Common Issues

### 1. Streaming Job Not Processing Data

```bash
# Check Kafka topic has data
kafka-console-consumer --bootstrap-server localhost:9092 --topic raw-events --from-beginning

# Verify MinIO connectivity
curl http://localhost:9000/

# Check Spark streaming logs for errors
```

### 2. Dagster Assets Failing

- Check Spark job logs in Dagster UI
- Verify Silver layer tables exist and have data
- Ensure Spark packages are properly configured
- Check file permissions for Iceberg warehouse

### 3. Iceberg Table Issues

- Verify catalog configuration matches between streaming and batch jobs
- Check S3A filesystem configuration
- Ensure warehouse directory exists in MinIO

## Next Steps

1. **Add Data Validation** - Implement Great Expectations with Dagster
2. **Enhance Monitoring** - Add Prometheus metrics and Grafana dashboards
3. **Scale Processing** - Move to EMR or Kubernetes for production
4. **Add ML Pipelines** - Integrate MLflow for model training and serving
5. **Implement CDC** - Add change data capture for database sources
