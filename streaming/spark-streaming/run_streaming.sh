#!/bin/bash
# Script to run the Spark streaming job with all required configurations

echo "🚀 Starting Spark Streaming Job for E-commerce Analytics"
echo "================================================="

# Parse command line arguments
USE_CLUSTER=false
HELP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --cluster)
            USE_CLUSTER=true
            shift
            ;;
        --local)
            USE_CLUSTER=false
            shift
            ;;
        --help|-h)
            HELP=true
            shift
            ;;
        *)
            echo "Unknown option $1"
            HELP=true
            shift
            ;;
    esac
done

if [ "$HELP" = true ]; then
    echo "Usage: $0 [--cluster|--local] [--help]"
    echo ""
    echo "Options:"
    echo "  --cluster    Submit job to Docker Spark cluster (spark://localhost:7077)"
    echo "  --local      Run job locally with local[*] master (default)"
    echo "  --help, -h   Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                # Run locally (default)"
    echo "  $0 --local       # Run locally (explicit)"
    echo "  $0 --cluster     # Submit to Docker Spark cluster"
    exit 0
fi

# Determine Spark master URL
if [ "$USE_CLUSTER" = true ]; then
    SPARK_MASTER="spark://localhost:7077"
    echo "📡 Using Docker Spark cluster: $SPARK_MASTER"
    
    # Check if cluster is accessible
    if ! curl -s --connect-timeout 5 http://localhost:7080 > /dev/null; then
        echo "❌ Cannot connect to Spark cluster web UI at http://localhost:7080"
        echo "   Please ensure Docker Spark cluster is running:"
        echo "   docker ps | grep spark"
        exit 1
    fi
    
    echo "✅ Spark cluster is accessible"
    echo "📊 Spark Master UI: http://localhost:7080"
else
    SPARK_MASTER="local[*]"
    echo "💻 Using local Spark execution: $SPARK_MASTER"
    
    # Check if SPARK_HOME is set for local execution
    if [ -z "$SPARK_HOME" ]; then
        echo "⚠️  SPARK_HOME not set. Please ensure Spark is installed and SPARK_HOME is configured."
        echo "Example: export SPARK_HOME=/path/to/spark"
        echo "Or use --cluster to submit to Docker Spark cluster instead"
        exit 1
    fi
fi

# Check if required JARs exist
JARS_DIR="../jars"
if [ ! -d "$JARS_DIR" ]; then
    echo "⚠️  JARs directory not found: $JARS_DIR"
    echo "Please ensure the required JAR files are available."
fi

echo "📦 Using Spark packages and JARs:"
echo "  - Iceberg Spark Runtime"
echo "  - Spark SQL Kafka"
echo "  - S3A filesystem support"

# Determine spark-submit command based on execution mode
if [ "$USE_CLUSTER" = true ]; then
    # For cluster mode, we'll submit via Docker exec to the Spark master
    echo "🚀 Submitting job to Spark cluster..."
    
    # Copy the Python script to the master container
    docker cp real_time_streaming.py ecommerce-spark-master:/tmp/real_time_streaming.py
    
    # Submit job from within the cluster
    docker exec ecommerce-spark-master /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hive \
        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.local.type=hadoop \
        --conf spark.sql.catalog.local.warehouse=s3a://warehouse/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://ecommerce-minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minio \
        --conf spark.hadoop.fs.s3a.secret.key=minio123 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=false \
        --driver-memory 1g \
        --executor-memory 1g \
        --executor-cores 1 \
        --total-executor-cores 2 \
        --name "ECommerceStreamingAnalytics-Cluster" \
        /tmp/real_time_streaming.py
else
    # Local execution using SPARK_HOME
    echo "🚀 Running job locally..."
    $SPARK_HOME/bin/spark-submit \
        --master $SPARK_MASTER \
        --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" \
        --jars "$JARS_DIR/aws-java-sdk-bundle-1.12.262.jar,$JARS_DIR/hadoop-aws-3.3.4.jar" \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
        --conf spark.sql.catalog.spark_catalog.type=hive \
        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.local.type=hadoop \
        --conf spark.sql.catalog.local.warehouse=s3a://warehouse/ \
        --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minio \
        --conf spark.hadoop.fs.s3a.secret.key=minio123 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        --driver-memory 2g \
        --executor-memory 2g \
        --name "ECommerceStreamingAnalytics-Local" \
        real_time_streaming.py
fi

fi

echo "✅ Spark streaming job completed or stopped."

if [ "$USE_CLUSTER" = true ]; then
    echo "📊 Monitor job execution at: http://localhost:7080"
    echo "🧹 Cleaning up temporary files..."
    docker exec ecommerce-spark-master rm -f /tmp/real_time_streaming.py
fi