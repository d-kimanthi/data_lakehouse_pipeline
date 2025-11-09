"""
Full Pipes-enabled Spark script for customer analytics processing.
This demonstrates enhanced observability and communication with Dagster.
"""

import argparse
import sys
from datetime import datetime

from common.dagster_utils import (
    check_silver_tables,
    dagster_pipes_context,
    report_error_metadata,
    report_no_data_metadata,
    report_success_metadata,
)

# Import common utilities
from common.spark_config import create_spark_session
from pyspark.sql.functions import *


def process_daily_customer_analytics(target_date: str):
    """Process daily customer analytics for the given date with Dagster Pipes"""
    try:
        with dagster_pipes_context() as context:
            context.log.info("Starting Spark customer analytics job")
            context.log.info(f"Processing date: {target_date}")

            # Create Spark session using common configuration
            spark = create_spark_session(
                app_name=f"CustomerAnalytics-{target_date}", include_kafka=False
            )

            context.log.info("Spark session initialized successfully")

            try:
                # Check available silver layer data
                check_silver_tables(context, spark)

                # Calculate daily customer metrics using Iceberg tables
                customer_analytics = spark.sql(
                    f"""
                    WITH page_view_metrics AS (
                        SELECT 
                            user_id,
                            date(timestamp) as date,
                            count(*) as page_views,
                            count(distinct session_id) as unique_sessions,
                            count(distinct CASE WHEN product_id IS NOT NULL THEN product_id END) as unique_products_viewed,
                            min(timestamp) as first_visit_time,
                            max(timestamp) as last_activity_time
                        FROM iceberg.silver.page_views
                        WHERE date(timestamp) >= date('{target_date}')
                        GROUP BY 1,2
                    ),
                    
                    purchase_metrics AS (
                        SELECT 
                            user_id,
                            date(timestamp) as date,
                            count(*) as total_purchases,
                            sum(total_amount) as total_revenue,
                            avg(total_amount) as avg_order_value
                        FROM iceberg.silver.purchases
                        WHERE date(timestamp) >= date('{target_date}')
                        GROUP BY 1,2
                    )
                    
                    SELECT 
                        coalesce(pv.user_id, pm.user_id) as user_id,
                        coalesce(pv.date, pm.date) as date,
                        coalesce(pv.page_views, 0) as page_views,
                        coalesce(pv.unique_sessions, 0) as unique_sessions,
                        coalesce(pv.unique_products_viewed, 0) as unique_products_viewed,
                        coalesce(pm.total_purchases, 0) as total_purchases,
                        coalesce(pm.total_revenue, 0.0) as total_revenue,
                        coalesce(pm.avg_order_value, 0.0) as avg_order_value,
                        pv.first_visit_time,
                        pv.last_activity_time,
                        CASE 
                            WHEN pm.total_purchases > 0 AND pm.total_revenue >= 1000 THEN 'high_value'
                            WHEN pm.total_purchases > 0 AND pm.total_revenue >= 100 THEN 'medium_value'
                            WHEN pm.total_purchases > 0 THEN 'low_value'
                            WHEN pv.page_views > 10 THEN 'engaged_browser'
                            WHEN pv.page_views > 0 THEN 'casual_browser'
                            ELSE 'unknown'
                        END as customer_segment,
                        current_timestamp() as created_at
                    FROM page_view_metrics pv
                    FULL OUTER JOIN purchase_metrics pm
                        ON pv.user_id = pm.user_id
                """
                )

                context.log.info(
                    "Computing customer analytics from Iceberg silver layer tables"
                )

                # Count the results first
                total_customers = customer_analytics.count()
                context.log.info(
                    f"Generated customer analytics for {total_customers} customers"
                )

                if total_customers > 0:
                    # Show sample results
                    context.log.info("Sample customer analytics results:")
                    customer_analytics.show(5, truncate=False)

                    # Write to Iceberg Gold layer using table name directly
                    context.log.info("Saving customer analytics to Iceberg gold layer")
                    customer_analytics.write.format("iceberg").mode("overwrite").option(
                        "write.parquet.compression-codec", "snappy"
                    ).partitionBy("date").saveAsTable(
                        "iceberg.gold.daily_customer_analytics"
                    )

                    context.log.info(
                        "Successfully saved analytics to iceberg.gold.daily_customer_analytics"
                    )

                    # Calculate metrics
                    total_revenue = (
                        customer_analytics.agg(sum("total_revenue")).collect()[0][0]
                        or 0
                    )

                    # Report comprehensive metadata using utility function
                    report_success_metadata(
                        context=context,
                        target_date=target_date,
                        spark=spark,
                        table_location="iceberg.gold.daily_customer_analytics",
                        custom_metrics={
                            "total_customers_processed": total_customers,
                            "total_revenue_processed": f"${total_revenue:.2f}",
                        },
                    )
                else:
                    context.log.warning("No customer data found for the specified date")
                    report_no_data_metadata(
                        context=context,
                        target_date=target_date,
                        table_location="iceberg.gold.daily_customer_analytics",
                    )

                context.log.info("Customer analytics processing completed successfully")

            except Exception as e:
                context.log.error(f"Error during data processing: {str(e)}")

                # Report error using utility function
                report_error_metadata(context=context, target_date=target_date, error=e)
                raise

            finally:
                context.log.info("🔧 Stopping Spark session")
                spark.stop()
    except ImportError as e:
        print(f"Failed to import dagster_pipes: {e}")
        print("This script requires dagster_pipes to be available")
        sys.exit(1)
    except Exception as e:
        print(f"Error in Pipes execution: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Process customer analytics data")
    parser.add_argument(
        "--target_date",
        default=(datetime.now().strftime("%Y-%m-%d")),
        help="Target date for processing (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    results = process_daily_customer_analytics(args.target_date)
    print(f"Customer analytics completed successfully: {results}")


if __name__ == "__main__":
    main()
