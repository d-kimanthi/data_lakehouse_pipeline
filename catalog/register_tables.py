#!/usr/bin/env python3
import boto3
import time
from botocore.exceptions import ClientError

def create_database(glue_client, database_name, description):
    """Create a Glue Database if it doesn't exist"""
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': description,
                'LocationUri': f's3://${S3_BUCKET}/warehouse/{database_name}'
            }
        )
        print(f"Created database: {database_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            print(f"Database {database_name} already exists")
        else:
            raise

def register_table(glue_client, database_name, table_name, table_spec):
    """Register or update a table in the Glue Catalog"""
    try:
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_spec
        )
        print(f"Created table: {database_name}.{table_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            glue_client.update_table(
                DatabaseName=database_name,
                TableInput=table_spec
            )
            print(f"Updated table: {database_name}.{table_name}")
        else:
            raise

def main():
    glue_client = boto3.client('glue')
    
    # Create retail database
    create_database(
        glue_client,
        'retail',
        'Retail analytics data warehouse'
    )

    # Register tables
    tables = {
        'orders': {
            'Name': 'orders',
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'table_type': 'ICEBERG'
            },
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'order_id', 'Type': 'string'},
                    {'Name': 'customer_id', 'Type': 'string'},
                    {'Name': 'store_id', 'Type': 'string'},
                    {'Name': 'store_state', 'Type': 'string'},
                    {'Name': 'store_city', 'Type': 'string'},
                    {'Name': 'product_id', 'Type': 'string'},
                    {'Name': 'product_name', 'Type': 'string'},
                    {'Name': 'category', 'Type': 'string'},
                    {'Name': 'quantity', 'Type': 'int'},
                    {'Name': 'unit_price', 'Type': 'double'},
                    {'Name': 'amount', 'Type': 'double'},
                    {'Name': 'currency', 'Type': 'string'},
                    {'Name': 'payment_method', 'Type': 'string'},
                    {'Name': 'event_time', 'Type': 'timestamp'},
                    {'Name': 'processing_time', 'Type': 'timestamp'}
                ],
                'Location': f's3://${S3_BUCKET}/warehouse/retail/orders',
                'InputFormat': 'org.apache.iceberg.mr.InputFormat',
                'OutputFormat': 'org.apache.iceberg.mr.OutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.iceberg.mr.hive.HiveIcebergSerDe'
                }
            },
            'PartitionKeys': [
                {'Name': 'event_time_day', 'Type': 'date'}
            ]
        },
        # Add more table definitions for our mart tables
        'dim_stores': {
            'Name': 'dim_stores',
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'table_type': 'ICEBERG'
            },
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'store_key', 'Type': 'string'},
                    {'Name': 'store_id', 'Type': 'string'},
                    {'Name': 'store_state', 'Type': 'string'},
                    {'Name': 'store_city', 'Type': 'string'},
                    {'Name': 'total_customers', 'Type': 'bigint'},
                    {'Name': 'store_city_rank', 'Type': 'int'},
                    {'Name': 'dbt_loaded_at', 'Type': 'timestamp'}
                ],
                'Location': f's3://${S3_BUCKET}/warehouse/retail/dim_stores',
                'InputFormat': 'org.apache.iceberg.mr.InputFormat',
                'OutputFormat': 'org.apache.iceberg.mr.OutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.iceberg.mr.hive.HiveIcebergSerDe'
                }
            }
        },
        'dim_products': {
            'Name': 'dim_products',
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'table_type': 'ICEBERG'
            },
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'product_key', 'Type': 'string'},
                    {'Name': 'product_id', 'Type': 'string'},
                    {'Name': 'product_name', 'Type': 'string'},
                    {'Name': 'product_category', 'Type': 'string'},
                    {'Name': 'category_id', 'Type': 'int'},
                    {'Name': 'dbt_loaded_at', 'Type': 'timestamp'}
                ],
                'Location': f's3://${S3_BUCKET}/warehouse/retail/dim_products',
                'InputFormat': 'org.apache.iceberg.mr.InputFormat',
                'OutputFormat': 'org.apache.iceberg.mr.OutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.iceberg.mr.hive.HiveIcebergSerDe'
                }
            }
        }
    }

    # Register all tables
    for table_name, table_spec in tables.items():
        register_table(glue_client, 'retail', table_name, table_spec)

if __name__ == '__main__':
    main()
