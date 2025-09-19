# JAR Files Directory

This directory contains JAR files needed for Spark jobs, particularly for Kafka and Iceberg integration.

## Required JARs for Local Development

Download these JARs and place them in this directory:

### Kafka Integration

- `kafka-clients-3.5.1.jar`
- `spark-sql-kafka-0-10_2.12-3.5.0.jar`
- `spark-streaming-kafka-0-10_2.12-3.5.0.jar`

### Iceberg Integration

- `iceberg-spark-runtime-3.5_2.12-1.4.2.jar`

### S3/MinIO Integration

- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.565.jar`

## Download Script

You can run this script to download the required JARs:

```bash
#!/bin/bash
cd jars

# Kafka JARs
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

# Iceberg JAR
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar

# AWS/S3 JARs
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar

echo "✅ JAR files downloaded successfully!"
```

## Usage

These JARs are automatically mounted into the Spark containers and will be available for Spark jobs that need Kafka or Iceberg connectivity.
