#!/bin/bash

echo "Starting E-commerce Analytics Platform..."

# Start core services first
echo "Starting PostgreSQL, Zookeeper, and Kafka..."
docker compose up -d postgres zookeeper kafka

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 30

# Start MinIO and other storage services
echo "Starting MinIO..."
docker compose up -d minio minio-init schema-registry

# Wait a bit more
sleep 15

# Start Spark services
echo "Starting Spark cluster..."
docker compose up -d spark-master

# Wait for Spark master to be ready
echo "Waiting for Spark master..."
sleep 20

# Start Spark worker
docker compose up -d spark-worker spark-history

# Start remaining services
echo "Starting remaining services..."
docker compose up -d schema-registry kafka-ui
docker compose up -d dagster-daemon dagster-webserver
docker compose up -d jupyter
docker compose up -d prometheus grafana

echo "All services started!"
echo ""
echo "Access URLs:"
echo "- Spark Master UI: http://localhost:7080"
echo "- Spark Worker UI: http://localhost:8081"
echo "- Spark History: http://localhost:18080"
echo "- Kafka UI: http://localhost:8080"
echo "- MinIO: http://localhost:9001"
echo "- Dagster: http://localhost:3000"
echo "- Jupyter: http://localhost:8888 (token: ecommerce-analytics)"
echo "- Grafana: http://localhost:3001 (admin/admin)"
echo ""