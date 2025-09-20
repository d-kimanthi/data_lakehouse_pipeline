#!/bin/bash

echo "Cleaning up E-commerce Analytics Platform..."

# Stop and remove Docker containers
echo "Stopping Docker containers..."
cd local
docker compose down -v
cd ..

# Remove Docker images (optional)
read -p "Do you want to remove Docker images as well? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Removing Docker images..."
    cd local
    docker compose down --rmi all
    cd ..
fi

# Clean up AWS resources (if deployed)
read -p "Do you want to destroy AWS resources? This is IRREVERSIBLE! (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Destroying AWS resources..."
    cd terraform/aws
    terraform destroy -auto-approve
    cd ../..
fi

# Clean up local directories (optional)
read -p "Do you want to clean up generated data and logs? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Cleaning up local data..."
    rm -rf data/
    rm -rf logs/
    rm -rf checkpoints/
    echo "Local cleanup complete!"
fi

echo "Cleanup finished!"
