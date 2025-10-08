#!/bin/bash
# Create required MinIO buckets for the data pipeline

echo "🪣 Creating MinIO data lake bucket..."

# Check if MinIO is running
if ! curl -s --connect-timeout 5 http://localhost:9000/minio/health/live > /dev/null; then
    echo "❌ MinIO is not accessible at http://localhost:9000"
    echo "Please start MinIO first:"
    echo "  docker-compose up minio -d"
    exit 1
fi

echo "✅ MinIO is accessible"

# Use docker exec to run mc commands inside a MinIO container
echo "🔧 Setting up MinIO client alias..."
docker run --rm --network ecommerce-streaming-analytics_default \
    minio/mc:latest /bin/sh -c "
    mc alias set myminio http://minio:9000 minioadmin minioadmin
    
    echo '📦 Creating data lake bucket with organized structure...'
    mc mb myminio/data-lake 2>/dev/null || echo 'Bucket data-lake already exists'
    
    echo '📁 Creating folder structure (folders created automatically on first write):'
    echo '  - data-lake/warehouse/     # Iceberg warehouse metadata'
    echo '  - data-lake/bronze/        # Raw ingested data'
    echo '  - data-lake/silver/        # Cleaned, validated data'
    echo '  - data-lake/gold/          # Analytics-ready aggregated data'
    echo '  - data-lake/checkpoints/   # Spark streaming checkpoints'
    echo '  - data-lake/logs/          # Application and audit logs'
    echo '  - data-lake/scripts/       # ETL scripts and utilities'
    
    echo '📋 Available buckets:'
    mc ls myminio/
    
    echo '✅ Data lake setup complete!'
    "

echo ""
echo "🌐 MinIO Console: http://localhost:9001"
echo "🔑 Credentials: minioadmin / minioadmin"
echo ""
echo "✅ All required paths are now available in the single data-lake bucket for the Spark streaming job"