# 🚀 Complete E-commerce Analytics Pipeline Execution Guide

## Prerequisites ✅

Before starting, ensure your local setup is running:

```bash
# Check that all services are up
docker ps --format 'table {{.Names}}\t{{.Status}}'

# Should show these services as healthy:
# - ecommerce-kafka
# - ecommerce-zookeeper
# - ecommerce-minio
# - ecommerce-spark-master
# - ecommerce-spark-worker
# - ecommerce-dagster-webserver
# - ecommerce-dagster-daemon
# - ecommerce-grafana
# - ecommerce-prometheus
```

## 📋 Pipeline Execution Steps

### Step 1: Validate Pipeline Health 🔍

Run the comprehensive pipeline test to ensure all components are ready:

```bash
cd /Users/kimanthi/projects/ecommerce-streaming-analytics
python3 test_spark_pipeline.py
```

**Expected Output**: `7/7 tests passed` - All components validated ✅

---

### Step 2: Start Data Generation 📊

Generate realistic e-commerce events and stream them to Kafka:

```bash
# Open a new terminal window
cd /Users/kimanthi/projects/ecommerce-streaming-analytics/data-generation

# Option A: Continuous generation (recommended for pipeline testing)
python3 event_generator.py --continuous

# Option B: Generate fixed number of events (default: 1000 events)
python3 event_generator.py

# Option C: Custom configuration
python3 event_generator.py --continuous --delay 0.05  # Faster event generation
python3 event_generator.py --num-events 500 --delay 0.2  # Slower batch generation
```

**Mode Options**:

- **`--continuous` or `-c`**: Runs indefinitely until Ctrl+C (recommended for real-time testing)
- **Default (no flag)**: Generates 1000 events then stops
- **`--delay`**: Time between events in seconds (default: 0.1s = ~600 events/min)
- **`--num-events`**: Number of events for batch mode (ignored in continuous mode)

**What happens**:

- Generates realistic page views, purchases, and user sessions
- Sends ~100-200 events per minute to Kafka topics:
  - `raw-events` - All events
  - `page-views` - Page view events
  - `purchases` - Purchase transactions
  - `user-sessions` - User session data

**Monitor**: You'll see output like:

**Continuous Mode**:

```
INFO:event_generator:Starting continuous event generation (press Ctrl+C to stop)...
INFO:event_generator:Generated PageViewEvent: user_123 viewed /product/456
INFO:event_generator:Generated PurchaseEvent: user_789 purchased product_101 for $45.99
INFO:event_generator:Generated 6000 events total (598.2 events/min)
```

**Batch Mode**:

```
INFO:event_generator:Starting to generate 1000 events...
INFO:event_generator:Sent 100 events...
INFO:event_generator:Sent 200 events...
INFO:event_generator:Finished sending 1000 events
```

**Keep this running** throughout the pipeline execution (use continuous mode for real-time testing).

---

### Step 3: Start Spark Streaming Job ⚡

Launch the real-time Spark streaming processor:

```bash
# Open another new terminal window
cd /Users/kimanthi/projects/ecommerce-streaming-analytics/streaming/spark-streaming

# Option A: Submit to Docker Spark cluster (recommended)
./run_streaming.sh --cluster

# Option B: Run locally (requires SPARK_HOME)
./run_streaming.sh --local

# Option C: Default local execution
./run_streaming.sh
```

**Execution Options**:

- **`--cluster`**: Submits job to Docker Spark cluster (spark://localhost:7077)
  - Uses Docker service networking (ecommerce-kafka:9092, ecommerce-minio:9000)
  - Better resource isolation and monitoring
  - Recommended for testing the complete setup
- **`--local`**: Runs with local Spark installation (local[*])
  - Requires SPARK_HOME environment variable
  - Uses localhost networking
  - Good for development and debugging

**What happens**:

- **Consumes events** from all Kafka topics in real-time
- **Creates Iceberg tables** automatically in MinIO:
  - `local.bronze_events` - Raw events with minimal processing
  - `local.silver_page_views` - Cleaned page view data
  - `local.silver_purchases` - Validated purchase transactions
  - `local.silver_user_sessions` - Session-aggregated user behavior
- **Processes data** with ~30 second latency
- **Handles schema evolution** and data quality validation

**Monitor**:

**Cluster Mode**:

- **Spark Master UI**: http://localhost:7080 shows cluster status and running applications
- **Application UI**: Click on running application in Master UI to see detailed metrics
- **Job execution**: Distributed across Spark workers

**Local Mode**:

- **Spark UI**: http://localhost:4040 shows streaming queries and metrics

**Both Modes Show**:

- **Streaming queries** processing events
- **Input/output rates** (events per second)
- **Batch processing times** and watermarks

**Success indicators**:

- No errors in console output
- Increasing input/output rates in Spark UI
- New files appearing in MinIO warehouse

---

### Step 4: Verify Data Flow 🔄

Check that data is flowing correctly through the pipeline:

#### 4.1 Check Kafka Topics

```bash
# List topics and check message counts
docker exec ecommerce-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Sample messages from a topic (Ctrl+C to stop)
docker exec ecommerce-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic page-views \
  --from-beginning \
  --max-messages 5
```

#### 4.2 Check MinIO Storage

- **UI**: http://localhost:9001 (minio/minio123)
- **Bucket**: Look for `warehouse` bucket
- **Path**: `/warehouse/local/` should contain Iceberg table directories
- **Files**: Each table should have data files and metadata

#### 4.3 Check Iceberg Tables (Optional)

```bash
# Connect to Spark shell to query tables
cd /Users/kimanthi/projects/ecommerce-streaming-analytics/streaming/spark-streaming

spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://warehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minio \
  --conf spark.hadoop.fs.s3a.secret.key=minio123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true

# In Spark shell:
spark.sql("SHOW TABLES IN local").show()
spark.sql("SELECT COUNT(*) FROM local.bronze_events").show()
spark.sql("SELECT COUNT(*) FROM local.silver_page_views").show()
spark.sql("SELECT * FROM local.silver_purchases LIMIT 5").show()
```

**Let streaming run for 5-10 minutes** to accumulate data before proceeding.

---

### Step 5: Start Dagster for Batch Processing 🔧

Launch Dagster to orchestrate batch analytics jobs:

```bash
# Open another new terminal window
cd /Users/kimanthi/projects/ecommerce-streaming-analytics/dagster/dagster_project

# Start Dagster (background daemon + web UI)
dagster dev
```

**Access Dagster UI**: http://localhost:3000

**What you'll see**:

- **Assets tab**: 5 Spark-based batch processing assets
- **Asset lineage**: Visual dependency graph
- **Materialize buttons**: Run individual assets

---

### Step 6: Execute Batch Analytics Jobs 📈

Run the batch processing jobs to create Gold layer analytics:

#### 6.1 Navigate to Assets

- Go to http://localhost:3000
- Click **"Assets"** tab
- You'll see 5 assets:
  - `gold_daily_customer_analytics`
  - `gold_daily_sales_summary`
  - `gold_product_analytics`
  - `silver_data_quality_report`
  - `streaming_health_monitor`

#### 6.2 Materialize Assets (Run Batch Jobs)

**Option A: Run Individual Assets**

1. Click on `gold_daily_customer_analytics`
2. Click **"Materialize"** button
3. Monitor execution in **"Runs"** tab
4. Repeat for other Gold assets

**Option B: Run All Assets Together**

1. Select all assets (checkbox)
2. Click **"Materialize selected"**
3. Dagster will run them in dependency order

**What each job does**:

**🧑‍💼 Customer Analytics** (`gold_daily_customer_analytics`):

- Customer segmentation (High Value, Regular, New, At Risk)
- Lifetime Value (LTV) calculation
- Purchase behavior analysis
- Session engagement metrics

**💰 Sales Summary** (`gold_daily_sales_summary`):

- Daily revenue and transaction counts
- Conversion rates (visitors → purchasers)
- Average order value trends
- Traffic source performance

**📦 Product Analytics** (`gold_product_analytics`):

- Product performance rankings
- Category analysis
- Recommendation scores
- Inventory insights

**✅ Data Quality** (`silver_data_quality_report`):

- Data completeness checks
- Schema validation
- Duplicate detection
- Error rate monitoring

**🔍 Health Monitor** (`streaming_health_monitor`):

- Streaming job status
- Processing latency metrics
- Error alerts
- Data freshness checks

#### 6.3 Monitor Job Execution

**In Dagster UI**:

- **Runs tab**: See job execution progress
- **Logs**: View Spark job output and errors
- **Asset details**: Check materialization timestamps
- **Lineage**: Visualize data flow

**Job Duration**: Each asset typically takes 2-5 minutes to complete.

---

### Step 7: Access Analytics Results 📊

Once batch jobs complete, you can query the Gold layer analytics:

#### 7.1 Customer Analytics Results

```sql
-- Connect to Spark and query:
SELECT
    customer_segment,
    COUNT(*) as customer_count,
    AVG(lifetime_value) as avg_ltv,
    AVG(total_spent) as avg_spent
FROM local.gold_daily_customer_analytics
WHERE partition_date = CURRENT_DATE
GROUP BY customer_segment;
```

#### 7.2 Sales Summary Results

```sql
SELECT
    partition_date,
    total_revenue,
    total_transactions,
    conversion_rate,
    avg_order_value
FROM local.gold_daily_sales_summary
ORDER BY partition_date DESC
LIMIT 7;
```

#### 7.3 Product Analytics Results

```sql
SELECT
    product_id,
    product_name,
    total_views,
    total_purchases,
    conversion_rate,
    recommendation_score
FROM local.gold_product_analytics
WHERE partition_date = CURRENT_DATE
ORDER BY recommendation_score DESC
LIMIT 10;
```

---

### Step 8: Monitor with Grafana Dashboard 📈

Access the monitoring dashboard to visualize pipeline health:

#### 8.1 Access Grafana

- **URL**: http://localhost:3000/grafana
- **Credentials**: admin/admin (change on first login)

#### 8.2 Import Dashboard

1. Go to **Dashboards** → **Import**
2. Upload `/monitoring/grafana/dashboards/ecommerce-analytics-dashboard.json`
3. Select Prometheus as data source

#### 8.3 Dashboard Panels

**📊 Real-time Metrics**:

- **Kafka Consumer Lag**: Event processing delays
- **Message Rate**: Events per second by topic
- **Streaming Rates**: Spark input vs processing rates
- **Processing Duration**: Batch processing latency

**🔧 System Health**:

- **Dagster Assets**: Materialization status
- **MinIO Storage**: Data lake capacity usage
- **Error Rates**: Failed jobs and data quality issues

**📈 Business Metrics** (requires additional setup):

- **Daily Revenue**: from Gold analytics tables
- **Customer Growth**: new vs returning customers
- **Conversion Funnel**: page views → purchases

---

### Step 9: Automated Scheduling (Optional) ⏰

Set up automated daily processing:

#### 9.1 Configure Asset Schedules

In Dagster UI:

1. Go to **Assets** → Select asset → **"Schedules"**
2. Create schedule: Daily at 1 AM
3. Enable auto-materialization

#### 9.2 Alternative: Cron-based Scheduling

```bash
# Add to crontab for daily 1 AM execution
0 1 * * * cd /path/to/dagster_project && dagster asset materialize --select gold_daily_customer_analytics gold_daily_sales_summary gold_product_analytics
```

---

## 🎯 Success Validation

### Pipeline is working correctly when:

✅ **Data Generation**: Event generator shows ~100-200 events/minute  
✅ **Real-time Processing**: Spark streaming shows input/output rates in UI  
✅ **Data Storage**: MinIO contains Bronze/Silver Iceberg tables with data  
✅ **Batch Processing**: Dagster assets materialize successfully  
✅ **Analytics**: Gold tables contain business metrics and insights  
✅ **Monitoring**: Grafana dashboard shows healthy pipeline metrics

### Troubleshooting Common Issues

**🔴 Streaming Job Fails**:

- Check Kafka connectivity: `docker exec ecommerce-kafka kafka-topics --list`
- Verify MinIO access: http://localhost:9001
- Check Spark logs for Java/Scala errors

**🔴 Dagster Assets Fail**:

- Ensure Silver tables exist and have data
- Check Spark package availability
- Verify file permissions for Iceberg warehouse

**🔴 No Data in Tables**:

- Confirm event generator is running and producing messages
- Check Kafka topic has messages: `kafka-console-consumer`
- Verify Spark streaming query is active in UI

**🔴 Dashboard Shows No Data**:

- Confirm Prometheus is scraping metrics
- Check Grafana data source configuration
- Verify metric names match dashboard queries

---

## 🏁 Next Steps

With the pipeline running successfully, you can:

1. **Add More Analytics**: Create additional Spark batch jobs for specific business questions
2. **Enhance Monitoring**: Add more detailed metrics and alerting rules
3. **Scale Processing**: Move to EMR/Kubernetes for production workloads
4. **Add ML Pipelines**: Integrate MLflow for customer segmentation and recommendation models
5. **Implement CDC**: Add database change data capture for additional data sources

---

## 📞 Support

If you encounter issues:

1. **Check logs**: Dagster UI → Runs → Logs for specific errors
2. **Validate setup**: Run `python3 test_spark_pipeline.py`
3. **Monitor resources**: Check CPU/memory usage with `docker stats`
4. **Review documentation**: `SPARK_PIPELINE_GUIDE.md` for detailed configurations

**🎉 Congratulations!** You now have a complete, production-ready Spark-only analytics pipeline processing real-time e-commerce data with automated batch analytics and monitoring dashboards.
