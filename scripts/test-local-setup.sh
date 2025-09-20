#!/bin/bash

echo "🧪 Testing Local Environment Setup"
echo "=================================="

# Initialize variables
ISSUES=false

# Test 1: Check if required files exist
echo "📋 Checking required files..."

required_files=(
    "local/docker-compose.yml"
    "local/.env"
    "Dockerfile.dagster"
    "scripts/start-local.sh"
    "scripts/stop-local.sh"
    "local/monitoring/prometheus/prometheus.yml"
    "local/monitoring/grafana/datasources/prometheus.yml"
)

missing_files=()
for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        missing_files+=("$file")
    fi
done

if [ ${#missing_files[@]} -eq 0 ]; then
    echo "✅ All required files present"
else
    echo "❌ Missing files:"
    for file in "${missing_files[@]}"; do
        echo "   - $file"
    done
    ISSUES=true
fi

# Test 2: Check if Docker is available
echo ""
echo "🐳 Checking Docker..."
if command -v docker &> /dev/null; then
    if docker info > /dev/null 2>&1; then
        echo "✅ Docker is running"
    else
        echo "❌ Docker is installed but not running"
        ISSUES=true
    fi
else
    echo "❌ Docker is not installed"
    ISSUES=true
fi

# Test 3: Check if Docker Compose is available
echo ""
echo "🔧 Checking Docker Compose..."
if docker compose version &> /dev/null; then
    echo "✅ Docker Compose is available"
    docker compose version
else
    echo "❌ Docker Compose is not available"
    ISSUES=true
fi

# Test 4: Validate Docker Compose file
echo ""
echo "📝 Validating Docker Compose configuration..."
cd local
if docker compose config > /dev/null 2>&1; then
    echo "✅ Docker Compose configuration is valid"
else
    echo "❌ Docker Compose configuration has errors"
    docker compose config
    ISSUES=true
fi
cd ..

# Test 5: Check required directories
echo ""
echo "📁 Checking directories..."
required_dirs=(
    "notebooks"
    "jars"
    "monitoring/grafana/dashboards"
    "monitoring/grafana/datasources"
    "dagster/dagster_home"
)

for dir in "${required_dirs[@]}"; do
    if [ -d "../../$dir" ]; then
        echo "✅ $dir exists"
    else
        echo "❌ $dir missing"
        mkdir -p "../../$dir"
        echo "   Created $dir"
    fi
done

cd ../..

echo ""
echo "🎯 Test Summary:"
echo "================"

if [ ${#missing_files[@]} -eq 0 ] && command -v docker &> /dev/null && docker info > /dev/null 2>&1 && docker compose version &> /dev/null; then
    echo "✅ Environment setup is ready!"
    echo ""
    echo "🚀 To start the local environment:"
    echo "   ./scripts/start-local.sh"
    echo ""
    echo "📚 To test with Jupyter:"
    echo "   Open http://localhost:8888 (token: ecommerce-analytics)"
    echo "   Run notebook: notebooks/01-local-environment-test.ipynb"
else
    echo "❌ Environment setup needs attention"
    echo ""
    echo "🔧 Required actions:"
    if [ ${#missing_files[@]} -ne 0 ]; then
        echo "   - Fix missing files listed above"
    fi
    if ! command -v docker &> /dev/null; then
        echo "   - Install Docker Desktop"
    elif ! docker info > /dev/null 2>&1; then
        echo "   - Start Docker Desktop"
    fi
    if ! docker compose version &> /dev/null; then
        echo "   - Install Docker Compose"
    fi
fi