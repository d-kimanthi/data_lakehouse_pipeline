#!/bin/bash
# Test script to demonstrate both local and cluster execution modes

echo "🧪 Testing Spark Streaming Execution Modes"
echo "=========================================="

cd /Users/kimanthi/projects/ecommerce-streaming-analytics/streaming/spark-streaming

echo ""
echo "📋 Available options:"
./run_streaming.sh --help

echo ""
echo "🔍 Checking Spark cluster status..."
if curl -s --connect-timeout 5 http://localhost:7080 > /dev/null; then
    echo "✅ Spark cluster is accessible at http://localhost:7080"
    echo "📊 Cluster info:"
    curl -s http://localhost:7080 | grep -E "(Workers|Memory|Cores)" | head -3 || echo "   (Could not parse cluster details)"
else
    echo "❌ Spark cluster not accessible"
fi

echo ""
echo "🔍 Checking SPARK_HOME for local execution..."
if [ -n "$SPARK_HOME" ]; then
    echo "✅ SPARK_HOME is set: $SPARK_HOME"
    if [ -f "$SPARK_HOME/bin/spark-submit" ]; then
        echo "✅ spark-submit found"
    else
        echo "❌ spark-submit not found in SPARK_HOME"
    fi
else
    echo "⚠️  SPARK_HOME not set (required for --local mode)"
fi

echo ""
echo "📝 Usage recommendations:"
echo "  - Use './run_streaming.sh --cluster' to leverage Docker Spark cluster"
echo "  - Use './run_streaming.sh --local' if you have local Spark installation"
echo "  - Cluster mode is recommended for testing the complete pipeline"

echo ""
echo "🚀 To start streaming:"
echo "  ./run_streaming.sh --cluster    # Submit to Docker cluster"
echo "  ./run_streaming.sh --local      # Run locally"