---
# scripts/setup.sh

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
docker-compose pull

# Start infrastructure services
echo "Starting infrastructure services..."
docker-compose up -d zookeeper kafka schema-registry minio postgres

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
docker-compose up -d

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

---

# scripts/submit_spark_job.sh

#!/bin/bash

echo "Submitting Spark Streaming Job..."

# Load environment variables

source .env

SPARK_SUBMIT_ARGS="
--class org.apache.spark.sql.execution.streaming.StreamExecution
--master ${SPARK_MASTER_URL:-spark://localhost:7077}
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.565
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
--conf spark.sql.catalog.spark_catalog.type=hive
--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.iceberg.type=glue
--conf spark.sql.catalog.iceberg.warehouse=s3://${S3_BUCKET}/iceberg-warehouse/
--conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT:-http://localhost:9000}
--conf spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY:-minioadmin}
--conf spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY:-minioadmin}
--conf spark.hadoop.fs.s3a.path.style.access=true
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
"

# For local development with Docker

if [ "$1" == "local" ]; then
docker exec spark-master spark-submit $SPARK_SUBMIT_ARGS \
 /opt/spark-apps/spark-streaming/streaming_job.py \
 --kafka-servers kafka:29092 \
 --kafka-topic raw-events \
 --s3-bucket ${MINIO_BUCKET:-ecommerce-data-lake}
else # For direct submission (if running Spark locally)
spark-submit $SPARK_SUBMIT_ARGS \
 streaming/spark-streaming/streaming_job.py \
 --kafka-servers ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
 --kafka-topic ${KAFKA_TOPIC_RAW_EVENTS:-raw-events} \
 --s3-bucket ${S3_BUCKET:-ecommerce-data-lake}
fi

echo "Spark job submitted. Check Spark UI at http://localhost:8080 for status."

---

# scripts/generate_sample_data.sh

#!/bin/bash

echo "Generating sample e-commerce data..."

# Load environment variables

source .env

# Parameters

NUM_EVENTS=${1:-5000}
DELAY=${2:-0.1}

echo "Generating $NUM_EVENTS events with $DELAY second delay between events..."

python data-generation/event_generator.py \
 --bootstrap-servers ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092} \
 --topic ${KAFKA_TOPIC_RAW_EVENTS:-raw-events} \
 --num-events $NUM_EVENTS \
 --delay $DELAY

echo "Data generation complete!"

---

# scripts/deploy.sh

#!/bin/bash

echo "Deploying E-commerce Analytics Platform to AWS..."

# Check if Terraform is installed

command -v terraform >/dev/null 2>&1 || { echo "Terraform is required but not installed. Aborting." >&2; exit 1; }

# Check if AWS CLI is configured

aws sts get-caller-identity > /dev/null 2>&1 || { echo "AWS CLI is not configured. Please run 'aws configure' first." >&2; exit 1; }

# Load environment variables

source .env

# Validate required environment variables

if [ -z "$AWS_REGION" ] || [ -z "$S3_BUCKET" ]; then
echo "Please set AWS_REGION and S3_BUCKET in your .env file"
exit 1
fi

# Initialize Terraform

echo "Initializing Terraform..."
cd terraform/aws
terraform init

# Plan the deployment

echo "Planning Terraform deployment..."
terraform plan \
 -var="aws_region=${AWS_REGION}" \
    -var="project_name=ecommerce-streaming-analytics" \
    -var="environment=production" \
    -var="s3_bucket_name=${S3_BUCKET}" \
 -out=tfplan

# Apply the plan

read -p "Do you want to apply this plan? (y/N): " -n 1 -r
echo
if [[$REPLY =~ ^[Yy]$]]; then
echo "Applying Terraform plan..."
terraform apply tfplan

    # Get outputs
    echo "Deployment complete! Getting resource information..."
    terraform output

    # Save important outputs to environment file
    echo "Updating environment file with deployment outputs..."
    MSK_BOOTSTRAP_SERVERS=$(terraform output -raw msk_bootstrap_servers)
    EMR_CLUSTER_ID=$(terraform output -raw emr_cluster_id)
    GLUE_DATABASE_NAME=$(terraform output -raw glue_database_name)

    cat >> ../../.env << EOF

# AWS Deployment Outputs

MSK_BOOTSTRAP_SERVERS=$MSK_BOOTSTRAP_SERVERS
EMR_CLUSTER_ID=$EMR_CLUSTER_ID
GLUE_DATABASE_NAME=$GLUE_DATABASE_NAME
EOF

    echo "Deployment complete! Check the AWS console for your resources."
    echo "MSK Bootstrap Servers: $MSK_BOOTSTRAP_SERVERS"
    echo "EMR Cluster ID: $EMR_CLUSTER_ID"
    echo "Glue Database: $GLUE_DATABASE_NAME"

else
echo "Deployment cancelled."
fi

cd ../..

---

# scripts/cleanup.sh

#!/bin/bash

echo "Cleaning up E-commerce Analytics Platform..."

# Stop and remove Docker containers

echo "Stopping Docker containers..."
docker-compose down -v

# Remove Docker images (optional)

read -p "Do you want to remove Docker images as well? (y/N): " -n 1 -r
echo
if [[$REPLY =~ ^[Yy]$]]; then
echo "Removing Docker images..."
docker-compose down --rmi all
fi

# Clean up AWS resources (if deployed)

read -p "Do you want to destroy AWS resources? This is IRREVERSIBLE! (y/N): " -n 1 -r
echo
if [[$REPLY =~ ^[Yy]$]]; then
echo "Destroying AWS resources..."
cd terraform/aws
terraform destroy -auto-approve
cd ../..
fi

# Clean up local directories (optional)

read -p "Do you want to clean up generated data and logs? (y/N): " -n 1 -r
echo
if [[$REPLY =~ ^[Yy]$]]; then
echo "Cleaning up local data..."
rm -rf data/
rm -rf logs/
rm -rf checkpoints/
echo "Local cleanup complete!"
fi

echo "Cleanup finished!"

---

# monitoring/prometheus/prometheus.yml

global:
scrape_interval: 15s
evaluation_interval: 15s

scrape_configs:

- job_name: 'prometheus'
  static_configs:

  - targets: ['localhost:9090']

- job_name: 'spark-master'
  static_configs:

  - targets: ['spark-master:8080']
    metrics_path: '/metrics'
    scrape_interval: 30s

- job_name: 'spark-worker'
  static_configs:

  - targets: ['spark-worker:8081']
    metrics_path: '/metrics'
    scrape_interval: 30s

- job_name: 'kafka'
  static_configs:

  - targets: ['kafka:9101']
    metrics_path: '/metrics'
    scrape_interval: 30s

- job_name: 'dagster'
  static_configs:
  - targets: ['dagster-webserver:3000']
    metrics_path: '/metrics'
    scrape_interval: 60s

---

# .env.example

# AWS Configuration (for production deployment)

AWS_REGION=us-west-2
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here

# S3 Configuration

S3_BUCKET=your-ecommerce-data-lake-bucket
S3_CHECKPOINT_BUCKET=your-checkpoints-bucket

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

# Project Settings

PROJECT_NAME=ecommerce-streaming-analytics
ENVIRONMENT=development
