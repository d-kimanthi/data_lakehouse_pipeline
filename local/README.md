# Local Development Environment

This directory contains all the files needed to run the e-commerce streaming analytics platform locally using Docker Compose.

## Quick Start

```bash
# From project root directory
cd local
docker compose up -d
```

## Directory Structure

```
local/
├── docker-compose.yml          # Main Docker Compose configuration
├── .env                        # Environment variables
├── data/                       # Local data storage
├── dagster/                    # Dagster configuration
├── jars/                       # Spark JAR files
├── monitoring/                 # Prometheus and Grafana configs
├── notebooks/                  # Jupyter notebooks for analysis
├── streaming/                  # Spark streaming jobs
└── requirements.txt/           # Python dependencies
```

## Services Included

- Apache Kafka & Zookeeper
- Apache Spark (Master & Worker)
- Dagster (Data Orchestration)
- PostgreSQL (Metadata)
- MinIO (S3-compatible storage)
- Prometheus & Grafana (Monitoring)
- Schema Registry & Kafka UI

## For detailed setup instructions, see the main [LOCAL_SETUP.md](../LOCAL_SETUP.md) file.
