from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    explode,
    from_json,
    to_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------- Config ----------
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "orders"
CATALOG_NAME = "local"  # Spark SQL catalog name we configure at submit
NAMESPACE = "retail"  # DB/schema in Iceberg
TABLE = "orders"  # Table name
CHECKPOINT = (
    "s3a://dk-retail-analytics-lake/iceberg_checkpoints/orders"  # or s3a://minio/...
)
TARGET_TABLE = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE}"

# Define schemas for nested structures
item_schema = StructType(
    [
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("quantity", StringType()),
        StructField("unit_price", DoubleType()),
    ]
)

# Main order schema
order_schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("store_id", StringType()),
        StructField("store_state", StringType()),
        StructField("store_city", StringType()),
        StructField("items", ArrayType(item_schema)),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("payment_method", StringType()),
        StructField("event_time", StringType()),
    ]
)

spark = SparkSession.builder.appName("KafkaToIceberg").getOrCreate()

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")

# Create orders table if not exists (Iceberg)
spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    order_id string,
    customer_id string,
    store_id string,
    store_state string,
    store_city string,
    product_id string,
    product_name string,
    category string,
    quantity int,
    unit_price double,
    amount double,
    currency string,
    payment_method string,
    event_time timestamp,
    processing_time timestamp
)
USING ICEBERG
PARTITIONED BY (days(event_time))
"""
)

# Read from Kafka
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Parse value as JSON -> columns and explode items array
parsed_df = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), order_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("event_time")))
)

# Explode the items array and select final columns
exploded_df = (
    parsed_df.withColumn("item", explode("items"))
    .select(
        "order_id",
        "customer_id",
        "store_id",
        "store_state",
        "store_city",
        col("item.product_id"),
        col("item.product_name"),
        col("item.category"),
        col("item.quantity"),
        col("item.unit_price"),
        "amount",
        "currency",
        "payment_method",
        "event_time",
    )
    .withColumn("processing_time", current_timestamp())
)

# Stream append into Iceberg
query = (
    exploded_df.writeStream.format("iceberg")
    .option("checkpointLocation", CHECKPOINT)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()
