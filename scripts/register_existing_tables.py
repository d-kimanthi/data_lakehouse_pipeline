"""
Register existing Iceberg tables in Nessie catalog.

This script registers pre-existing Iceberg tables (bronze/silver layers) that were
created with Hadoop catalog into the Nessie catalog so they can be queried by Trino and Spark.
"""

from pyspark.sql import SparkSession


def register_existing_tables():
    """Register existing Iceberg tables in Nessie catalog"""

    print("🔧 Initializing Spark session with Nessie catalog...")

    spark = (
        SparkSession.builder.appName("RegisterExistingTables")
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

    # Tables to register (adjust based on your actual tables)
    tables_to_register = [
        ("silver", "product_updates"),
        ("silver", "user_sessions"),
    ]

    registered_count = 0
    skipped_count = 0

    for schema, table in tables_to_register:
        table_path = f"s3a://data-lake/warehouse/{schema}/{table}"
        full_table_name = f"iceberg.{schema}.{table}"

        try:
            # Check if table already exists in Nessie
            try:
                spark.sql(f"DESCRIBE TABLE {full_table_name}")
                print(
                    f"⏭️  Table {full_table_name} already exists in Nessie, skipping..."
                )
                skipped_count += 1
                continue
            except Exception:
                pass  # Table doesn't exist, proceed with registration

            print(f"📋 Registering table: {full_table_name}")
            print(f"   Location: {table_path}")

            # Register the table by creating it with the existing location
            # This tells Nessie about the table and its metadata
            spark.sql(
                f"""
                CALL iceberg.system.register_table(
                    table => '{full_table_name}',
                    metadata_file => '{table_path}/metadata/v*.metadata.json'
                )
            """
            )

            print(f"✅ Successfully registered {full_table_name}")
            registered_count += 1

            # Verify table is readable
            count = spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]
            print(f"   Table has {count:,} rows")

        except Exception as e:
            print(f"❌ Failed to register {full_table_name}: {e}")
            print("   Trying alternative method...")

            try:
                # Alternative: Create table pointing to existing metadata
                spark.sql(
                    f"""
                    CREATE TABLE IF NOT EXISTS {full_table_name}
                    USING iceberg
                    LOCATION '{table_path}'
                """
                )
                print(
                    f"✅ Successfully registered {full_table_name} using CREATE TABLE"
                )
                registered_count += 1
            except Exception as e2:
                print(f"❌ Alternative method also failed: {e2}")

    print("\n" + "=" * 60)
    print("📊 Registration Summary:")
    print(f"   ✅ Successfully registered: {registered_count}")
    print(f"   ⏭️  Already existed: {skipped_count}")
    print(f"   ❌ Failed: {len(tables_to_register) - registered_count - skipped_count}")
    print("=" * 60)

    # List all tables in Nessie
    print("\n📋 Current tables in Nessie catalog:")
    for schema in ["bronze", "silver", "gold"]:
        try:
            tables = spark.sql(f"SHOW TABLES IN iceberg.{schema}").collect()
            if tables:
                print(f"\n  {schema.upper()} schema:")
                for row in tables:
                    print(f"    - {row[1]}")
            else:
                print(f"\n  {schema.upper()} schema: (empty)")
        except Exception as e:
            print(f"\n  {schema.upper()} schema: Error - {e}")

    spark.stop()
    print("\n✅ Table registration complete!")


if __name__ == "__main__":
    register_existing_tables()
