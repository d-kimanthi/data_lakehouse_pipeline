# Dagster Silver-to-Gold Pipeline Implementation

## Overview

This implementation provides a complete Dagster-based orchestration system for processing data from the silver layer to the gold layer using Spark batch jobs. The pipeline includes:

- **Assets**: Represent the gold layer data transformations
- **Jobs**: Orchestrate the execution of multiple assets
- **Schedules**: Automate daily processing
- **Data Quality Checks**: Validate the processed data
- **Spark Integration**: Submit jobs to the containerized Spark cluster

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Silver Layer  │───▶│  Dagster Assets  │───▶│   Gold Layer    │
│                 │    │                  │    │                 │
│ • page_views    │    │ • customer_      │    │ • customer_     │
│ • purchases     │    │   analytics      │    │   analytics     │
│ • products      │    │ • sales_summary  │    │ • sales_summary │
│                 │    │ • product_perf   │    │ • product_perf  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Data Quality   │
                       │     Checks       │
                       └──────────────────┘
```

## Components

### 1. Assets (`dagster_project/assets.py`)

#### `daily_customer_analytics_asset`

- **Purpose**: Creates comprehensive customer analytics from page views and purchases
- **Input**: Silver layer `page_views` and `purchases` tables
- **Output**: Gold layer `daily_customer_analytics` table
- **Spark Script**: `batch_customer_analytics.py`
- **Metrics**: Customer segments, revenue, engagement statistics

#### `daily_sales_summary_asset`

- **Purpose**: Aggregates daily sales metrics and trends
- **Input**: Silver layer `purchases` and `products` tables
- **Output**: Gold layer `daily_sales_summary` table
- **Spark Script**: `batch_sales_summary.py`
- **Metrics**: Revenue, order counts, product performance

#### `product_performance_analytics_asset`

- **Purpose**: Analyzes product views, purchases, and conversion rates
- **Input**: Silver layer `page_views` and `purchases` tables
- **Output**: Gold layer `product_performance_analytics` table
- **Spark Script**: `product_performance_analytics.py`
- **Metrics**: Conversion rates, revenue rankings, view-to-purchase ratios

### 2. Jobs (`dagster_project/jobs.py`)

#### `silver_to_gold_processing`

- **Purpose**: Main daily batch job for all gold layer transformations
- **Assets**: All three gold layer assets
- **Schedule**: Daily at 2 AM UTC
- **Dependencies**: Ensures proper execution order

#### `customer_analytics_processing`

- **Purpose**: Focused job for customer analytics only
- **Schedule**: Daily at 3 AM UTC (optional)

#### `sales_analytics_processing`

- **Purpose**: Sales and product analytics combined
- **Schedule**: Daily at 4 AM UTC (optional)

### 3. Resources (`dagster_project/resources.py`)

#### `SparkClusterResource`

- **Purpose**: Manages Spark job submission to the containerized cluster
- **Features**:
  - Automatic cluster health checks
  - Configurable job parameters (memory, cores)
  - Error handling and retry logic
  - Result parsing and metadata extraction

### 4. Data Quality (`dagster_project/data_quality.py`)

#### Asset Checks

- **Customer Analytics**: Validates data completeness, revenue consistency, segment logic
- **Sales Summary**: Checks revenue calculations, order consistency
- **Product Analytics**: Validates conversion rates, view/purchase relationships

## Setup and Deployment

### 1. Prerequisites

Ensure your environment has the following running:

```bash
cd /Users/kimanthi/projects/ecommerce-streaming-analytics/local
docker-compose up -d postgres kafka minio spark-master spark-worker
```

### 2. Start Dagster Services

```bash
# Start Dagster daemon and webserver
docker-compose up -d dagster-daemon dagster-webserver

# Check logs
docker-compose logs -f dagster-webserver
```

### 3. Access Dagster UI

Navigate to: http://localhost:3000

## Usage

### Manual Execution

1. **Access Dagster UI**: http://localhost:3000
2. **Navigate to Assets**: View all gold layer assets
3. **Materialize Assets**: Click "Materialize" on individual assets or asset groups
4. **Monitor Progress**: Watch job execution in real-time
5. **View Metadata**: Check asset metadata for processing statistics

### Scheduled Execution

The pipeline includes three schedules:

1. **Main Schedule**: `daily_gold_processing_schedule`

   - Runs daily at 2 AM UTC
   - Processes all gold layer assets
   - Status: Running (enabled by default)

2. **Customer Analytics**: `customer_analytics_schedule`

   - Runs daily at 3 AM UTC
   - Status: Stopped (enable manually if needed)

3. **Sales Analytics**: `sales_analytics_schedule`
   - Runs daily at 4 AM UTC
   - Status: Stopped (enable manually if needed)

### Spark Job Monitoring

Monitor Spark job execution:

- **Spark Master UI**: http://localhost:7080
- **Spark History Server**: http://localhost:18080
- **Dagster Logs**: View detailed execution logs in the Dagster UI

## Testing

### 1. Run the Test Script

```bash
cd /Users/kimanthi/projects/ecommerce-streaming-analytics
python scripts/test_dagster_silver_to_gold.py
```

### 2. Manual Testing

```bash
# Test individual Spark script
docker exec ecommerce-spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
  /opt/bitnami/spark/work-dir/batch_customer_analytics.py \
  --date 2024-01-01

# Test Dagster asset materialization
# (Use the Dagster UI or dagster CLI)
```

## Configuration

### Spark Job Configuration

Default settings in `SparkClusterResource`:

```python
executor_memory = "2g"      # Memory per executor
executor_cores = 2          # CPU cores per executor
driver_memory = "1g"        # Driver memory
max_wait_time = 3600        # Job timeout (1 hour)
```

### Iceberg Configuration

All Spark jobs use Iceberg with S3 (MinIO) storage:

```python
warehouse_path = "s3a://data-lake/warehouse/"
endpoint = "http://minio:9000"
```

### Schedule Configuration

Schedules use cron expressions:

```python
daily_schedule = "0 2 * * *"  # 2 AM UTC daily
```

## Troubleshooting

### Common Issues

1. **Spark Cluster Not Available**

   ```bash
   # Check Spark containers
   docker-compose ps spark-master spark-worker

   # Restart if needed
   docker-compose restart spark-master spark-worker
   ```

2. **Dagster Connection Issues**

   ```bash
   # Check Dagster containers
   docker-compose logs dagster-webserver
   docker-compose logs dagster-daemon

   # Restart Dagster services
   docker-compose restart dagster-daemon dagster-webserver
   ```

3. **Asset Materialization Failures**
   - Check Spark logs in Dagster UI
   - Verify silver layer data exists
   - Check S3/MinIO connectivity

### Logs and Debugging

- **Dagster Logs**: Available in the Dagster UI under "Runs"
- **Spark Logs**: Available in Spark UI and Dagster metadata
- **Container Logs**: `docker-compose logs [service-name]`

## Performance Tuning

### Spark Configuration

For larger datasets, adjust:

```python
spark_cluster.submit_spark_job(
    executor_memory="4g",
    executor_cores=4,
    driver_memory="2g"
)
```

### Iceberg Optimization

- Use appropriate partitioning strategies
- Configure compaction schedules
- Monitor table metadata size

## Next Steps

1. **Add More Assets**: Create additional gold layer transformations
2. **Implement SLAs**: Add data freshness and processing time SLAs
3. **Add Alerting**: Integrate with monitoring systems
4. **Partitioning**: Implement date-based partitioning for better performance
5. **Data Lineage**: Leverage Dagster's built-in lineage tracking

## Support

For issues and questions:

- Check the Dagster documentation: https://docs.dagster.io/
- Review Spark logs in the UI
- Examine container logs for system-level issues
