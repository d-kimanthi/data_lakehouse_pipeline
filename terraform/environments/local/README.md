# Local Environment Configuration

This directory contains the configuration for the local development environment using Docker Compose.

## Services Included

- **Kafka**: Single broker setup for development
- **Zookeeper**: Required for Kafka
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Database for Dagster metadata
- **Spark**: Standalone cluster (1 master + 1 worker)
- **Dagster**: Orchestration platform
- **Jupyter**: For data exploration
- **Grafana**: Monitoring dashboards
- **Prometheus**: Metrics collection

## Quick Start

```bash
# Start all services
docker-compose -f docker-compose.local.yml up -d

# Check services status
docker-compose -f docker-compose.local.yml ps

# Stop all services
docker-compose -f docker-compose.local.yml down

# Stop and remove volumes (reset everything)
docker-compose -f docker-compose.local.yml down -v
```

## Service URLs

- **Dagster UI**: http://localhost:3000
- **Kafka UI**: http://localhost:8080
- **Jupyter**: http://localhost:8888 (token: ecommerce-analytics)
- **Spark Master UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001 (admin/minioadmin)
- **Grafana**: http://localhost:3001 (admin/admin)
- **Prometheus**: http://localhost:9090

## Environment Variables

See `.env.local` for local development configuration.
