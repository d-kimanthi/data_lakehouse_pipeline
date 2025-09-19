#!/bin/bash

# Local Environment Setup Script
echo "🚀 Starting E-commerce Streaming Analytics - Local Environment"
echo "=============================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi

# Check if Docker Compose is available
if ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not available. Please install Docker and try again."
    exit 1
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p jars
mkdir -p notebooks
mkdir -p data/{bronze,silver,gold,logs,checkpoints}
mkdir -p dagster/dagster_home
mkdir -p monitoring/grafana/{dashboards,datasources}

# Copy environment file if it doesn't exist
if [ ! -f .env ]; then
    echo "📋 Creating environment file..."
    cp terraform/environments/local/.env.local .env
fi

# Start services
echo "🐳 Starting Docker services..."
cd terraform/environments/local
docker compose -f docker-compose.local.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check service health
echo "🔍 Checking service status..."
docker compose -f docker-compose.local.yml ps

# Create Kafka topics
echo "📊 Creating Kafka topics..."
docker exec ecommerce-kafka kafka-topics --create --if-not-exists --topic raw-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec ecommerce-kafka kafka-topics --create --if-not-exists --topic processed-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec ecommerce-kafka kafka-topics --create --if-not-exists --topic customer-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec ecommerce-kafka kafka-topics --create --if-not-exists --topic product-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec ecommerce-kafka kafka-topics --create --if-not-exists --topic order-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo ""
echo "✅ Local environment is ready!"
echo ""
echo "🌐 Service URLs:"
echo "  - Dagster UI:     http://localhost:3000"
echo "  - Kafka UI:       http://localhost:8080"
echo "  - Jupyter:        http://localhost:8888 (token: ecommerce-analytics)"
echo "  - Spark Master:   http://localhost:8081"
echo "  - MinIO Console:  http://localhost:9001 (admin/minioadmin)"
echo "  - Grafana:        http://localhost:3001 (admin/admin)"
echo "  - Prometheus:     http://localhost:9090"
echo ""
echo "📚 Next steps:"
echo "  1. Open Dagster UI to see the orchestration pipeline"
echo "  2. Run 'python data-generation/event_generator.py' to start generating events"
echo "  3. Check Kafka UI to see the streaming events"
echo "  4. Monitor the data flow in Grafana dashboards"
echo ""
echo "🛑 To stop: docker-compose -f terraform/environments/local/docker-compose.local.yml down"
echo "🗑️  To reset: docker-compose -f terraform/environments/local/docker-compose.local.yml down -v"