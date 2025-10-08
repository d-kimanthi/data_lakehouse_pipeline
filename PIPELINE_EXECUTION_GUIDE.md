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
cd ecommerce-streaming-analytics
python3 test_spark_pipeline.py
```

**Expected Output**: `7/7 tests passed` - All components validated ✅

---

### Step 2: Start Data Generation 📊

Generate realistic e-commerce events and stream them to Kafka:

```bash
# Open a new terminal window
cd ecommerce-streaming-analytics/data-generation

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

- Generates realistic e-commerce events with intelligent topic routing:
  - **Page views** → `page-views` topic
  - **Purchases** → `purchases` topic
  - **User sessions** → `user-sessions` topic
  - **Cart events** → `cart-events` topic
  - **Product updates** → `product-updates` topic
  - **All events** → `raw-events` topic (for backward compatibility)

**Monitor**: You'll see output like:

**Continuous Mode** (with topic distribution):

```
INFO:event_generator:Starting continuous event generation (press Ctrl+C to stop)...
INFO:event_generator:Generated PageViewEvent: user_123 viewed /product/456
INFO:event_generator:Generated PurchaseEvent: user_789 purchased product_101 for $45.99
INFO:event_generator:Generated 6000 events total (598.2 events/min)
INFO:event_generator:Topic distribution: {'cart-events': 1204, 'page-views': 2398, 'product-updates': 602, 'purchases': 1201, 'user-sessions': 595}
```

**Batch Mode** (with final topic summary):

```
INFO:event_generator:Starting to generate 1000 events...
INFO:event_generator:Sent 100 events...
INFO:event_generator:Sent 200 events...
INFO:event_generator:Finished sending 1000 events
INFO:event_generator:Topic distribution: {'cart-events': 201, 'page-views': 398, 'product-updates': 99, 'purchases': 199, 'user-sessions': 103}
```

**Keep this running** throughout the pipeline execution (use continuous mode for real-time testing).

---

### Step 3: Start Spark Streaming Job ⚡

Launch the real-time Spark streaming processor with medallion architecture:

```bash
# Open another new terminal window
cd ecommerce-streaming-analytics/streaming/spark-streaming

# Option A: Complete pipeline - Kafka → Bronze → Silver (recommended)
./run_streaming.sh --cluster --job-type both

# Option B: Only data ingestion - Kafka → Bronze
./run_streaming.sh --cluster --job-type ingestion

# Option C: Only data processing - Bronze → Silver
./run_streaming.sh --cluster --job-type processing

# Option D: Local execution (requires SPARK_HOME)
./run_streaming.sh --local --job-type both
```

**Medallion Architecture Jobs**:

- **`ingestion`**: Kafka → Bronze layer
  - Minimal processing, preserve raw data
  - Output: `iceberg.bronze.raw_events` table
  - Real-time event ingestion with metadata preservation
- **`processing`**: Bronze → Silver layer
  - JSON parsing, data quality checks, business rules
  - Output: 5 Silver tables (page_views, purchases, cart_events, user_sessions, product_updates)
  - Structured analytics-ready data
- **`both`**: Complete pipeline (default)
  - Runs both ingestion and processing jobs
  - End-to-end Kafka → Bronze → Silver flow

**Execution Options**:

- **`--cluster`**: Submits job to Docker Spark cluster (spark://localhost:7077)
  - Uses Docker service networking (kafka:29092, ecommerce-minio:9000)
  - Better resource isolation and monitoring
  - Recommended for testing the complete setup
- **`--local`**: Runs with local Spark installation (local[*])
  - Requires SPARK_HOME environment variable
  - Uses localhost networking (localhost:9092, localhost:9000)
  - Good for development and debugging

**What happens with Medallion Architecture**:

**Bronze Layer Processing**:

- **Consumes events** from all Kafka topics in real-time
- **Minimal transformation**: Extract event_id, event_type, timestamp
- **Preserves raw data**: Complete JSON payload stored
- **Adds metadata**: Kafka topic, partition, offset for lineage
- **Creates table**: `iceberg.bronze.raw_events` automatically

**Silver Layer Processing** (if processing job enabled):

- **Reads from Bronze**: `iceberg.bronze.raw_events` table
- **Parses JSON**: Structured data extraction per event type
- **Data quality checks**: Null validation, business rules
- **Creates Silver tables** automatically:
  - `iceberg.silver.page_views` - Cleaned page view data
  - `iceberg.silver.purchases` - Validated purchase transactions
  - `iceberg.silver.cart_events` - Add-to-cart behavior
  - `iceberg.silver.user_sessions` - User login/logout events
  - `iceberg.silver.product_updates` - Product price/inventory changes

**Processing Timing**:

- **Bronze ingestion**: ~30 second batches
- **Silver processing**: ~1 minute batches
- **Data latency**: End-to-end ~90 seconds

**Monitor**:

**Cluster Mode**:

- **Spark Master UI**: http://localhost:7080 shows cluster status and running applications
- **Application UI**: Click on running application in Master UI to see detailed metrics
- **Job naming**: Applications named by job type (e.g., `ECommerceStreamingAnalytics-Cluster-both`)

**Local Mode**:

- **Spark UI**: http://localhost:4040 shows streaming queries and metrics

**Both Modes Show**:

- **Streaming queries**: 1 query for ingestion, 5 queries for processing
- **Input/output rates**: Events per second for each layer
- **Batch processing times**: Bronze and Silver processing latency
- **Watermarks**: Event-time processing progress

**Success indicators**:

- No errors in console output
- Increasing input/output rates in Spark UI
- Bronze table shows raw events with proper event_type distribution
- Silver tables show parsed data with quality validation applied
- New files appearing in MinIO warehouse for both Bronze and Silver

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

- **UI**: http://localhost:9001 (minioadmin/minioadmin)
- **Bucket**: Look for `data-lake` bucket
- **Bronze Path**: `/data-lake/warehouse/iceberg/bronze/raw_events/` should contain:
  - `data/` directory with Parquet files containing raw events
  - `metadata/` directory with Iceberg table metadata
- **Silver Paths**: Each silver table should have similar structure:
  - `/data-lake/warehouse/iceberg/silver/page_views/`
  - `/data-lake/warehouse/iceberg/silver/purchases/`
  - `/data-lake/warehouse/iceberg/silver/cart_events/`
  - `/data-lake/warehouse/iceberg/silver/user_sessions/`
  - `/data-lake/warehouse/iceberg/silver/product_updates/`

#### 4.3 Check Iceberg Tables (Medallion Architecture)

```bash
# Connect to Spark shell to query medallion architecture tables
cd ecommerce-streaming-analytics/streaming/spark-streaming

spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hadoop \
  --conf spark.sql.catalog.iceberg.warehouse=s3a://data-lake/warehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true

# In Spark shell - Check Bronze Layer:
spark.sql("SHOW TABLES IN iceberg.bronze").show()
spark.sql("SELECT event_type, COUNT(*) as count FROM iceberg.bronze.raw_events GROUP BY event_type").show()
spark.sql("SELECT * FROM iceberg.bronze.raw_events LIMIT 3").show()

# Check Silver Layer:
spark.sql("SHOW TABLES IN iceberg.silver").show()
spark.sql("SELECT COUNT(*) as page_views FROM iceberg.silver.page_views").show()
spark.sql("SELECT COUNT(*) as purchases FROM iceberg.silver.purchases").show()
spark.sql("SELECT COUNT(*) as cart_events FROM iceberg.silver.cart_events").show()
spark.sql("SELECT COUNT(*) as user_sessions FROM iceberg.silver.user_sessions").show()
spark.sql("SELECT COUNT(*) as product_updates FROM iceberg.silver.product_updates").show()

# Sample data from Silver tables:
spark.sql("SELECT user_id, page_type, device_type FROM iceberg.silver.page_views LIMIT 5").show()
spark.sql("SELECT user_id, total_amount, payment_method FROM iceberg.silver.purchases LIMIT 5").show()
```

**Let streaming run for 5-10 minutes** to accumulate data before proceeding.

---

### Step 5: Start Dagster for Batch Processing 🔧

Launch Dagster to orchestrate batch analytics jobs:

```bash
# Open another new terminal window
cd ecommerce-streaming-analytics/dagster/dagster_project

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
-- Connect to Spark and query (using new table structure):
SELECT
    customer_segment,
    COUNT(*) as customer_count,
    AVG(lifetime_value) as avg_ltv,
    AVG(total_spent) as avg_spent
FROM iceberg.gold.daily_customer_analytics
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
FROM iceberg.gold.daily_sales_summary
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
FROM iceberg.gold.product_analytics
WHERE partition_date = CURRENT_DATE
ORDER BY recommendation_score DESC
LIMIT 10;
```

#### 7.4 Data Lineage Verification

```sql
-- Verify medallion architecture data flow:

-- Bronze → Silver data lineage
SELECT
    'bronze' as layer,
    event_type,
    COUNT(*) as record_count,
    MAX(processing_time) as latest_processing
FROM iceberg.bronze.raw_events
GROUP BY event_type

UNION ALL

SELECT
    'silver' as layer,
    'page_view' as event_type,
    COUNT(*) as record_count,
    MAX(processing_time) as latest_processing
FROM iceberg.silver.page_views

UNION ALL

SELECT
    'silver' as layer,
    'purchase' as event_type,
    COUNT(*) as record_count,
    MAX(processing_time) as latest_processing
FROM iceberg.silver.purchases;
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

✅ **Data Generation**: Event generator shows ~100-200 events/minute with topic distribution  
✅ **Bronze Layer**: Raw events ingested with proper event_type categorization  
✅ **Silver Layer**: 5 Silver tables populated with parsed, validated data  
✅ **Real-time Processing**: Spark streaming shows separate ingestion and processing rates  
✅ **Data Storage**: MinIO contains Bronze/Silver Iceberg tables with proper medallion structure  
✅ **Batch Processing**: Dagster assets materialize successfully from Silver layer  
✅ **Analytics**: Gold tables contain business metrics derived from clean Silver data  
✅ **Monitoring**: Grafana dashboard shows healthy medallion pipeline metrics

### Medallion Architecture Validation

**🥉 Bronze Layer Health**:

- Raw events table contains all event types (page_view, purchase, add_to_cart, user_session, product_update)
- Event counts match Kafka topic message counts
- Raw JSON data preserved with proper metadata

**🥈 Silver Layer Health**:

- 5 Silver tables populated: page_views, purchases, cart_events, user_sessions, product_updates
- Data quality checks applied (no null user_ids, valid amounts)
- Schema evolution handled properly
- Processing timestamps show continuous updates

**🥇 Gold Layer Health**:

- Analytics tables built from Silver layer data
- Business metrics computed correctly
- Daily partitioning working
- Data lineage traceable Bronze → Silver → Gold

### Troubleshooting Common Issues

**🔴 Streaming Job Fails**:

- Check Kafka connectivity: `docker exec ecommerce-kafka kafka-topics --bootstrap-server localhost:9092 --list`
- Verify MinIO access: http://localhost:9001
- Check Spark logs for Java/Scala errors
- Verify job type: `./run_streaming.sh --help` for valid options

**🔴 Bronze Layer Issues**:

- Ensure event generator is running and producing messages
- Check Kafka topics have messages: `kafka-console-consumer`
- Verify Kafka → Bronze ingestion query is active in Spark UI
- Check `iceberg.bronze.raw_events` table exists and contains data

**🔴 Silver Layer Issues**:

- Confirm Bronze table has data with proper event_type values
- Check Bronze → Silver processing queries are running (5 separate queries)
- Verify JSON parsing schemas match event generator output
- Check for data quality validation failures in logs

**🔴 Dagster Assets Fail**:

- Ensure Silver tables exist and have data (prerequisite for Gold layer)
- Check Spark package availability in Dagster environment
- Verify Iceberg warehouse path configuration
- Check asset dependencies in lineage graph

**🔴 No Data in Tables**:

- **Bronze empty**: Check Kafka connectivity and ingestion job
- **Silver empty**: Check Bronze has data and processing job is running
- **Gold empty**: Check Silver tables have data and Dagster jobs completed
- Use medallion validation queries to trace data flow

**🔴 Dashboard Shows No Data**:

- Confirm Prometheus is scraping metrics from correct endpoints
- Check Grafana data source configuration points to updated table names
- Verify metric names match new Iceberg table structure
- Update dashboard queries to use `iceberg.bronze.*` and `iceberg.silver.*` paths

**🔴 Job Type Confusion**:

- Use `./run_streaming.sh --help` to see all available options
- For debugging: Run `--job-type ingestion` first, then `--job-type processing`
- Check Spark UI for correct number of streaming queries (1 for ingestion, 5 for processing)
- Verify job names in Spark UI include job type suffix

---

## 🏁 Next Steps

With the medallion architecture pipeline running successfully, you can:

1. **Enhanced Analytics**: Create additional Spark batch jobs for specific business questions using clean Silver data
2. **Advanced Monitoring**: Add medallion-specific metrics and alerting rules for each layer
3. **Production Scaling**:
   - Run ingestion and processing jobs on separate clusters for independent scaling
   - Move to EMR/Kubernetes with separate node pools for Bronze and Silver processing
4. **ML Pipelines**: Integrate MLflow using Silver layer for customer segmentation and recommendation models
5. **Stream Processing Optimization**:
   - Implement late-arriving data handling with watermarks
   - Add real-time alerting on data quality issues
   - Optimize Bronze → Silver processing with delta updates
6. **Data Governance**:
   - Add schema registry for event evolution
   - Implement data lineage tracking across medallion layers
   - Add data retention policies for Bronze/Silver/Gold layers
7. **Advanced Features**:
   - Implement CDC for database sources
   - Add streaming aggregations for real-time dashboards
   - Create materialized views for frequently accessed analytics

## 📚 Additional Resources

- **Medallion Architecture**: `streaming/spark-streaming/MEDALLION_ARCHITECTURE_USAGE.md`
- **Spark Pipeline Details**: `SPARK_PIPELINE_GUIDE.md`
- **Job Type Reference**: `./run_streaming.sh --help`
- **Event Generator Options**: `python3 event_generator.py --help`

---

## 📞 Support

If you encounter issues:

1. **Check logs**: Dagster UI → Runs → Logs for specific errors
2. **Validate setup**: Run `python3 test_spark_pipeline.py`
3. **Monitor resources**: Check CPU/memory usage with `docker stats`
4. **Review documentation**: `SPARK_PIPELINE_GUIDE.md` for detailed configurations

**🎉 Congratulations!** You now have a complete, production-ready medallion architecture analytics pipeline processing real-time e-commerce data with proper Bronze → Silver → Gold data flow, automated batch analytics, and comprehensive monitoring dashboards.
