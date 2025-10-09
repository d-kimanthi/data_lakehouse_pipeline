"""
Recreate silver layer tables with proper schemas.
This script drops the empty table shells and creates them with correct Iceberg schemas.
"""

from pyspark.sql import SparkSession


def recreate_silver_tables():
    """Drop and recreate silver tables with proper schemas"""

    print("🔧 Initializing Spark session with Nessie catalog...")
    spark = (
        SparkSession.builder.appName("RecreateSilverTables")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.0",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.iceberg.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.iceberg.uri", "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.iceberg.ref", "main")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://data-lake/warehouse/")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    print("✅ Spark session initialized")

    tables_to_recreate = [
        (
            "page_views",
            """
            CREATE TABLE iceberg.silver.page_views (
                event_id STRING,
                timestamp TIMESTAMP,
                user_id STRING,
                session_id STRING,
                product_id STRING,
                page_type STRING,
                referrer STRING,
                user_agent STRING,
                ip_address STRING,
                device_type STRING,
                metadata MAP<STRING, STRING>,
                processing_time TIMESTAMP
            ) USING iceberg
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy'
            )
        """,
        ),
        (
            "purchases",
            """
            CREATE TABLE iceberg.silver.purchases (
                event_id STRING,
                timestamp TIMESTAMP,
                user_id STRING,
                session_id STRING,
                order_id STRING,
                total_amount DECIMAL(10,2),
                subtotal DECIMAL(10,2),
                discount_percent INT,
                discount_amount DECIMAL(10,2),
                payment_method STRING,
                shipping_method STRING,
                shipping_address STRUCT<
                    street: STRING,
                    city: STRING,
                    state: STRING,
                    zip_code: STRING,
                    country: STRING
                >,
                items ARRAY<STRUCT<
                    product_id: STRING,
                    quantity: INT,
                    price: DECIMAL(10,2)
                >>,
                processing_time TIMESTAMP
            ) USING iceberg
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy'
            )
        """,
        ),
        (
            "cart_events",
            """
            CREATE TABLE iceberg.silver.cart_events (
                event_id STRING,
                timestamp TIMESTAMP,
                user_id STRING,
                session_id STRING,
                product_id STRING,
                action STRING,
                quantity INT,
                processing_time TIMESTAMP
            ) USING iceberg
            TBLPROPERTIES (
                'write.parquet.compression-codec'='snappy'
            )
        """,
        ),
    ]

    for table_name, create_sql in tables_to_recreate:
        try:
            print(f"\n📋 Recreating table: iceberg.silver.{table_name}")

            # Drop the table if it exists
            try:
                spark.sql(f"DROP TABLE IF EXISTS iceberg.silver.{table_name}")
                print("   ✅ Dropped old table")
            except Exception as e:
                print(f"   ⚠️  Could not drop table: {e}")

            # Create the table with proper schema
            spark.sql(create_sql)
            print("   ✅ Created table with proper schema")

            # Verify the schema
            schema_df = spark.sql(f"DESCRIBE iceberg.silver.{table_name}")
            col_count = schema_df.count()
            print(f"   ✅ Verified: {col_count} columns defined")

        except Exception as e:
            print(f"   ❌ Error recreating {table_name}: {e}")

    print("\n" + "=" * 60)
    print("📊 Final Table Status:")
    print("=" * 60)

    for table_name, _ in tables_to_recreate:
        try:
            schema_df = spark.sql(f"DESCRIBE iceberg.silver.{table_name}")
            col_count = schema_df.count()
            print(f"  ✅ {table_name}: {col_count} columns")
        except Exception as e:
            print(f"  ❌ {table_name}: Failed - {e}")

    print("\n✅ Silver table recreation complete!")
    spark.stop()


if __name__ == "__main__":
    recreate_silver_tables()
