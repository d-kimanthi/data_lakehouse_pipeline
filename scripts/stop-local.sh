#!/bin/bash

echo "🛑 Stopping E-commerce Streaming Analytics - Local Environment"
echo "=============================================================="

# Change to the correct directory
cd terraform/environments/local

# Check if the compose file exists
if [ ! -f "docker-compose.local.yml" ]; then
    echo "❌ docker-compose.local.yml not found. Make sure you're in the right directory."
    exit 1
fi

# Stop and remove containers
echo "🐳 Stopping Docker services..."
docker compose -f docker-compose.local.yml down

echo ""
echo "✅ Local environment stopped!"
echo ""
echo "💡 Options:"
echo "  - To restart: ./scripts/start-local.sh"
echo "  - To reset all data: docker compose -f terraform/environments/local/docker-compose.local.yml down -v"
echo "  - To clean up Docker: docker system prune"