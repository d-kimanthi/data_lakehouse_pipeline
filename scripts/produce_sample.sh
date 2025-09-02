#!/usr/bin/env bash
set -euo pipefail

# Run the Python data generator in the container
docker compose exec -T spark-master python /opt/spark-app/generate_retail_data.py