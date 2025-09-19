from dagster import resource, InitResourceContext
import boto3
import os


@resource(config_schema={"warehouse_path": str, "catalog_type": str, "aws_region": str})
def iceberg_resource(init_context: InitResourceContext):
    """
    Iceberg catalog resource
    """
    config = init_context.resource_config

    warehouse_path = config.get(
        "warehouse_path", f"s3://{os.getenv('S3_BUCKET')}/iceberg-warehouse/"
    )
    catalog_type = config.get("catalog_type", "glue")
    aws_region = config.get("aws_region", "us-west-2")

    # Initialize AWS clients if needed
    if catalog_type == "glue":
        glue_client = boto3.client("glue", region_name=aws_region)
        s3_client = boto3.client("s3", region_name=aws_region)

        catalog_config = {
            "warehouse_path": warehouse_path,
            "catalog_type": catalog_type,
            "glue_client": glue_client,
            "s3_client": s3_client,
        }
    else:
        catalog_config = {
            "warehouse_path": warehouse_path,
            "catalog_type": catalog_type,
        }

    return catalog_config
