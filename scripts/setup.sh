
#!/bin/bash

echo "Setting up E-commerce Streaming Analytics Platform..."

# Check if required tools are installed
command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed. Aborting." >&2; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed. Aborting." >&2; exit 1; }

# Create necessary directories
echo "Creating project directories..."
mkdir -p data-generation/sample_data
mkdir -p streaming/spark-streaming/{utils,configs}
mkdir -p dagster/dagster_project/{assets,resources,jobs,schedules}
mkdir -p analytics/{sql/data_marts,sql/quality_checks,notebooks}
mkdir -p monitoring/{grafana/dashboards,prometheus,alerts}
mkdir -p terraform/{aws,modules/{kafka,storage,compute}}
mkdir -p scripts
mkdir -p jars

# Copy environment file
echo "Creating environment configuration..."
if [ ! -f .env ]; then
    cat > .env << EOF
# AWS Configuration (for production deployment)
AWS_REGION=us-west-2
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key

# S3 Configuration
S3_BUCKET=ecommerce-data-lake-bucket
S3_CHECKPOINT_BUCKET=ecommerce-checkpoints

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_RAW_EVENTS=raw-events
KAFKA_TOPIC_PROCESSED_EVENTS=processed-events

# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_APP_NAME=ecommerce-streaming-analytics

# MinIO Configuration (local development)
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=ecommerce-data-lake

# Dagster Configuration
DAGSTER_HOME=/opt/dagster/dagster_home
DAGSTER_POSTGRES_DB=dagster_db
DAGSTER_POSTGRES_USER=dagster_user
DAGSTER_POSTGRES_PASSWORD=dagster_password

# Monitoring
GRAFANA_ADMIN_PASSWORD=admin
PROMETHEUS_PORT=9090
EOF
    echo "Environment file created. Please update .env with your actual configurations."
fi

# Pull Docker images
echo "Pulling Docker images..."
docker compose pull

# Start infrastructure services
echo "Starting infrastructure services..."
docker compose up -d zookeeper kafka schema-registry minio postgres

# Wait for services to be ready
echo "Waiting for services to initialize..."
sleep 30

# Create Kafka topics
echo "Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic raw-events --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec kafka kafka-topics --create --topic processed-events --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists
docker exec kafka kafka-topics --create --topic alerts --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists

# Start remaining services
echo "Starting all services..."
docker compose up -d

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Setup complete!"
echo ""
echo "Services available at:"
echo "  - Dagster UI: http://localhost:3000"
echo "  - Kafka UI: http://localhost:8080"
echo "  - Jupyter: http://localhost:8888 (token: ecommerce-analytics)"
echo "  - Spark UI: http://localhost:8080"
echo "  - MinIO: http://localhost:9001 (admin/minioadmin)"
echo "  - Grafana: http://localhost:3001 (admin/admin)"
echo "  - Prometheus: http://localhost:9090"
echo ""
echo "Next steps:"
echo "1. Run 'python data-generation/event_generator.py' to start generating events"
echo "2. Submit Spark streaming job: 'scripts/submit_spark_job.sh'"
echo "3. Access Dagster UI to monitor and schedule pipelines"
