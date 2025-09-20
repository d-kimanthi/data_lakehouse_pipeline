# E-commerce Streaming Analytics - Local Setup Guide

This guide provides step-by-step instructions to run the e-commerce streaming analytics platform locally using Docker Compose.

## 🏗️ Architecture Overview

The local environment includes:

- **Apache Kafka** - Event streaming platform
- **Apache Spark** - Stream processing engine
- **Dagster** - Data orchestration and pipeline management
- **MinIO** - S3-compatible object storage (simulating data lake)
- **PostgreSQL** - Metadata storage for Dagster
- **Prometheus & Grafana** - Monitoring and observability
- **Schema Registry** - Schema management for Kafka
- **Kafka UI** - Web interface for Kafka management

## 📋 Prerequisites

### Required Software

- **Docker Desktop** (version 4.0+)
- **Docker Compose V2** (included with Docker Desktop)
- **Git** (for cloning the repository)
- **Terminal/Command Line** access

### System Requirements

- **RAM**: Minimum 8GB, Recommended 16GB+
- **Storage**: At least 10GB free space
- **OS**: macOS, Linux, or Windows with WSL2

### Verify Prerequisites

```bash
# Check Docker version
docker --version

# Check Docker Compose version
docker compose version

# Verify Docker is running
docker info
```

## 🚀 Quick Start

### 1. Clone and Navigate to Project

```bash
git clone <repository-url>
cd ecommerce-streaming-analytics
```

### 2. Start the Local Environment

```bash
# Make scripts executable
chmod +x scripts/*.sh

# Start all services
./scripts/start-local.sh
```

### 3. Verify Setup

```bash
# Test all services
./scripts/test-local-setup.sh
```

### 4. Access Web Interfaces

- **Kafka UI**: http://localhost:8080
- **Dagster UI**: http://localhost:3000
- **Grafana**: http://localhost:3001 (admin/admin)
- **MinIO Console**: http://localhost:9001 (admin/password)
- **Spark Master UI**: http://localhost:8081

## 📝 Detailed Setup Steps

### Step 1: Environment Preparation

#### Create Required Directories

The setup script automatically creates these, but you can create them manually:

```bash
mkdir -p data/{bronze,silver,gold,checkpoints,logs}
mkdir -p dagster/dagster_home
mkdir -p monitoring/{prometheus,grafana}/data
```

#### Check Docker Resources

Ensure Docker has sufficient resources allocated:

- **Memory**: 8GB minimum (16GB recommended)
- **CPUs**: 4+ cores recommended
- **Disk Space**: 10GB+ available

### Step 2: Service Startup

#### Start Services in Order

```bash
# Navigate to project root
cd /path/to/ecommerce-streaming-analytics

# Start the environment
./scripts/start-local.sh
```

The startup script will:

1. ✅ Verify Docker is running
2. 📁 Create necessary directories
3. 🐳 Start services via Docker Compose
4. ⏱️ Wait for services to be healthy
5. 🌐 Display access URLs

#### Manual Startup (Alternative)

```bash
# Navigate to local environment
cd local

# Start services
docker compose up -d

# Check service status
docker compose ps
```

### Step 3: Service Verification

#### Run Automated Tests

```bash
./scripts/test-local-setup.sh
```

#### Manual Health Checks

```bash
# Check all containers are running
docker ps

# Check specific service logs
docker logs ecommerce-kafka
docker logs ecommerce-spark-master
docker logs ecommerce-dagster

# Test Kafka connectivity
docker exec ecommerce-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Step 4: Generate Test Data

#### Run Event Generator

```bash
# Install Python dependencies (if running locally)
pip install -r requirements.txt

# Generate sample events
python data-generation/event_generator.py

# Or use the sample data script
./scripts/generate_sample_data.sh
```

#### Verify Data Flow

```bash
# Check Kafka topics
curl http://localhost:8080/api/clusters

# Check MinIO buckets
curl http://localhost:9000/minio/health/live

# Check Dagster runs
curl http://localhost:3000/graphql
```

## 🔧 Configuration

### Environment Variables

Key configuration files:

- `local/.env` - Environment-specific settings
- `local/docker-compose.yml` - Service definitions
- `local/monitoring/prometheus/prometheus.yml` - Monitoring configuration

### Resource Allocation

To adjust resource allocation, modify `local/docker-compose.yml`:

```yaml
services:
  spark-master:
    environment:
      - SPARK_MASTER_MEMORY=2g
      - SPARK_MASTER_CORES=2
```

### Port Configuration

Default ports (can be modified in local/docker-compose.yml):

```
PostgreSQL: 5432
Zookeeper: 2181
Kafka: 9092, 9093
Schema Registry: 8081
Kafka UI: 8080
Spark Master: 7077, 8081
Spark Worker: 8082
Dagster: 3000
Grafana: 3001
Prometheus: 9090
MinIO API: 9000
MinIO Console: 9001
```

## 🧪 Testing the Setup

### Test Scenarios

#### 1. Basic Connectivity Test

```bash
# Test all services are responding
./scripts/test-local-setup.sh
```

#### 2. Data Pipeline Test

```bash
# Generate events
python data-generation/event_generator.py --events 100

# Submit Spark job
./scripts/submit_spark_job.sh

# Check Dagster pipeline
# Navigate to http://localhost:3000 and trigger a run
```

#### 3. End-to-End Test

```bash
# Run comprehensive test
python -m pytest tests/integration/ -v
```

### Expected Outputs

- ✅ All containers running (11 services)
- ✅ Kafka topics created and accessible
- ✅ MinIO buckets created
- ✅ Dagster UI accessible with assets visible
- ✅ Monitoring dashboards showing metrics

## 🔍 Troubleshooting

### Common Issues

#### Services Not Starting

```bash
# Check Docker resources
docker system df
docker system prune # if needed

# Check logs
cd local
docker compose logs

# Restart specific service
docker compose restart kafka
```

#### Port Conflicts

```bash
# Check port usage
lsof -i :9092  # Kafka
lsof -i :3000  # Dagster

# Stop conflicting services
sudo lsof -ti:3000 | xargs kill -9
```

#### Memory Issues

```bash
# Check Docker memory usage
docker stats

# Increase Docker memory in Docker Desktop settings
# Recommended: 8GB minimum, 16GB preferred
```

#### Network Issues

```bash
# Recreate network
docker network rm ecommerce-network
cd local
docker compose up -d
```

### Service-Specific Troubleshooting

#### Kafka Issues

```bash
# Check Kafka broker
docker exec ecommerce-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# List topics
docker exec ecommerce-kafka kafka-topics --list --bootstrap-server localhost:9092

# Check consumer groups
docker exec ecommerce-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

#### Dagster Issues

```bash
# Check Dagster logs
docker logs ecommerce-dagster

# Verify database connection
docker exec ecommerce-postgres psql -U dagster -d dagster -c "\dt"

# Reset Dagster database (if needed)
docker exec ecommerce-postgres psql -U dagster -d dagster -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
```

#### Spark Issues

```bash
# Check Spark master logs
docker logs ecommerce-spark-master

# Check worker logs
docker logs ecommerce-spark-worker

# Test Spark connectivity
curl http://localhost:8081/api/v1/applications
```

### Log Locations

- **Container logs**: `docker logs <container-name>`
- **Application logs**: `data/logs/`
- **Dagster logs**: `dagster/dagster_home/logs/`
- **Spark logs**: Available via Spark UI (http://localhost:8081)

## 🛑 Shutdown

### Graceful Shutdown

```bash
./scripts/stop-local.sh
```

### Manual Shutdown

```bash
cd local
docker compose down

# With volume cleanup
docker compose down -v
```

### Clean Shutdown (Remove Everything)

```bash
./scripts/cleanup.sh
```

## 📊 Monitoring and Observability

### Prometheus Metrics

Access: http://localhost:9090

- Query examples:
  - `kafka_server_brokertopicmetrics_messagesin_total`
  - `spark_streaming_lastReceivedBatch_processingTime`

### Grafana Dashboards

Access: http://localhost:3001 (admin/admin)

- Pre-configured dashboards for Kafka, Spark, and system metrics
- Custom dashboards in `monitoring/grafana/dashboards/`

### Dagster Observability

Access: http://localhost:3000

- Pipeline runs and asset materialization history
- Data lineage and dependency graphs
- Real-time execution monitoring

## 🔄 Next Steps

Once the local setup is running successfully:

1. **Explore Dagster UI** - Familiarize yourself with the data pipeline interface
2. **Generate Test Data** - Use the event generator to create realistic e-commerce events
3. **Monitor Data Flow** - Watch data move through Kafka → Spark → MinIO
4. **Experiment with Queries** - Use the notebooks to analyze processed data
5. **Customize Pipelines** - Modify Dagster assets for your specific use cases

## 📚 Additional Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [MinIO Documentation](https://docs.min.io/)

---

**Note**: This local setup is designed for development and testing. For staging and production deployments, refer to the AWS setup guide (coming soon).
