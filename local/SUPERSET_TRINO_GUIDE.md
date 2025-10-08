# Superset and Trino Setup Guide

This guide explains how to use Trino and Superset to visualize your Iceberg data lake analytics.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Visualization Layer                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Apache Superset (Port 8088)                              │  │
│  │  - Dashboards and Charts                                  │  │
│  │  - SQL Lab for ad-hoc queries                             │  │
│  └──────────────────────┬───────────────────────────────────┘  │
│                         │                                        │
│                         ▼                                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Trino Query Engine (Port 8080)                           │  │
│  │  - Distributed SQL queries                                │  │
│  │  - Iceberg connector                                      │  │
│  └──────────────────────┬───────────────────────────────────┘  │
│                         │                                        │
│                         ▼                                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  MinIO S3 Storage (Port 9000)                             │  │
│  │  - Iceberg Tables (Silver & Gold)                         │  │
│  │  - Parquet Data Files                                     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Services

### Trino (Port 8080)

- **Purpose**: Distributed SQL query engine for querying Iceberg tables
- **Catalog**: `iceberg` - connects to your data lake in MinIO
- **Access**: http://localhost:8080
- **User**: No authentication by default

### Superset (Port 8088)

- **Purpose**: Modern data exploration and visualization platform
- **Access**: http://localhost:8088
- **Default Credentials**:
  - Username: `admin`
  - Password: `admin`

### Supporting Services

- **Redis (Port 6379)**: Caching for Superset
- **Superset DB (Port 5433)**: PostgreSQL database for Superset metadata

## Quick Start

### 1. Start the Services

```bash
cd /Users/kimanthi/projects/ecommerce-streaming-analytics/local
docker compose up -d trino superset redis superset-db
```

Wait for services to be healthy:

```bash
docker compose ps
```

### 2. Access Superset

1. Open browser: http://localhost:8088
2. Login with:
   - Username: `admin`
   - Password: `admin`

### 3. Connect Trino to Superset

#### Add Trino Database Connection

1. In Superset, click **Settings** → **Database Connections** → **+ Database**
2. Select **Trino** from the list
3. Configure connection:
   ```
   Display Name: Iceberg Data Lake
   SQLAlchemy URI: trino://trino:8080/iceberg
   ```
4. Click **Test Connection**
5. Click **Connect**

**Alternative manual connection string:**

```
trino://trino:8080/iceberg/silver
```

### 4. Query Your Data

#### Using SQL Lab

1. Go to **SQL** → **SQL Lab**
2. Select database: **Iceberg Data Lake**
3. Select schema: **silver** or **gold**
4. Run queries:

**Query Silver Layer Tables:**

```sql
-- Page views
SELECT * FROM iceberg.silver.page_views LIMIT 100;

-- Purchases
SELECT * FROM iceberg.silver.purchases LIMIT 100;

-- User sessions
SELECT * FROM iceberg.silver.user_sessions LIMIT 100;
```

**Query Gold Layer Analytics:**

```sql
-- Daily customer analytics
SELECT * FROM iceberg.gold.daily_customer_analytics
ORDER BY date DESC
LIMIT 100;

-- Customer segments distribution
SELECT
    customer_segment,
    COUNT(*) as customer_count,
    SUM(total_revenue) as total_revenue,
    AVG(page_views) as avg_page_views
FROM iceberg.gold.daily_customer_analytics
GROUP BY customer_segment
ORDER BY total_revenue DESC;

-- Top customers by revenue
SELECT
    user_id,
    date,
    total_revenue,
    total_purchases,
    customer_segment
FROM iceberg.gold.daily_customer_analytics
ORDER BY total_revenue DESC
LIMIT 20;
```

### 5. Create Visualizations

#### Create a Dataset

1. Go to **Data** → **Datasets** → **+ Dataset**
2. Select:
   - **Database**: Iceberg Data Lake
   - **Schema**: gold
   - **Table**: daily_customer_analytics
3. Click **Create Dataset and Create Chart**

#### Example Charts

**1. Customer Segments Pie Chart**

- Chart Type: Pie Chart
- Metrics: COUNT(\*)
- Group by: customer_segment

**2. Revenue Trend Line Chart**

- Chart Type: Line Chart
- X-Axis: date
- Metrics: SUM(total_revenue)
- Time Grain: Day

**3. Top Customers Table**

- Chart Type: Table
- Columns: user_id, total_revenue, total_purchases, customer_segment
- Sort: total_revenue DESC
- Row limit: 20

**4. Customer Activity Heatmap**

- Chart Type: Heatmap
- X-Axis: date
- Y-Axis: customer_segment
- Metrics: COUNT(\*)

### 6. Create a Dashboard

1. Go to **Dashboards** → **+ Dashboard**
2. Name it: "E-commerce Customer Analytics"
3. Drag and drop your charts
4. Save the dashboard

## Trino CLI Access

You can also query directly using Trino CLI:

```bash
# Access Trino CLI
docker compose exec trino trino

# In Trino CLI
trino> SHOW CATALOGS;
trino> SHOW SCHEMAS IN iceberg;
trino> SHOW TABLES IN iceberg.silver;
trino> SELECT COUNT(*) FROM iceberg.silver.page_views;
```

## Useful Trino Queries

### Table Information

```sql
-- Show table columns
DESCRIBE iceberg.gold.daily_customer_analytics;

-- Show table properties
SHOW CREATE TABLE iceberg.gold.daily_customer_analytics;

-- Table statistics
SHOW STATS FOR iceberg.gold.daily_customer_analytics;
```

### Data Exploration

```sql
-- Check data freshness
SELECT
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    COUNT(*) as total_records
FROM iceberg.gold.daily_customer_analytics;

-- Customer segment summary
SELECT
    customer_segment,
    COUNT(DISTINCT user_id) as unique_customers,
    SUM(total_revenue) as total_revenue,
    AVG(page_views) as avg_page_views,
    AVG(total_purchases) as avg_purchases
FROM iceberg.gold.daily_customer_analytics
GROUP BY customer_segment;
```

## Troubleshooting

### Trino Cannot Connect to MinIO

Check Trino logs:

```bash
docker compose logs trino
```

Verify MinIO is accessible:

```bash
docker compose exec trino curl http://minio:9000
```

### Superset Cannot Connect to Trino

1. Check if Trino is running:

   ```bash
   docker compose ps trino
   curl http://localhost:8080
   ```

2. Verify network connectivity:

   ```bash
   docker compose exec superset ping trino
   ```

3. Check Superset logs:
   ```bash
   docker compose logs superset
   ```

### Iceberg Tables Not Visible

1. Check if tables exist in MinIO:

   - Open http://localhost:9001 (MinIO Console)
   - Browse to `data-lake/warehouse/silver/` and `data-lake/warehouse/gold/`

2. Verify Trino catalog configuration:

   ```bash
   docker compose exec trino cat /etc/trino/catalog/iceberg.properties
   ```

3. Try refreshing metadata in Trino:
   ```sql
   CALL iceberg.system.refresh_metadata('silver', 'page_views');
   ```

## Performance Tuning

### Trino Memory Configuration

Edit `trino/config.properties`:

```properties
query.max-memory=2GB
query.max-memory-per-node=1GB
```

### Superset Caching

The setup includes Redis caching. To adjust cache timeout, edit `superset/superset_config.py`:

```python
CACHE_DEFAULT_TIMEOUT = 300  # seconds
```

## Monitoring

### Trino Web UI

- URL: http://localhost:8080
- Shows running queries, cluster status, and performance metrics

### Superset Activity Log

- Go to **Settings** → **List Users** → **Activity Log**
- View query history and user actions

## Security Notes

**For Production:**

1. Change default passwords in docker-compose.yml
2. Enable HTTPS/TLS
3. Configure proper authentication (OAuth, LDAP, etc.)
4. Set up proper RBAC in Superset
5. Enable Trino authentication and authorization

## Next Steps

1. **Create More Dashboards**: Build comprehensive dashboards for different business metrics
2. **Set Up Alerts**: Configure Superset alerts for important metrics
3. **Optimize Queries**: Use Trino EXPLAIN to optimize slow queries
4. **Add More Data Sources**: Connect Trino to other data sources if needed
5. **Schedule Reports**: Set up scheduled email reports in Superset

## Additional Resources

- [Trino Documentation](https://trino.io/docs/current/)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [Superset Documentation](https://superset.apache.org/docs/intro)
- [Apache Iceberg](https://iceberg.apache.org/)
