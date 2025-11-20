"""
Full Pipes-enabled Spark script for daily sales summary processing.
This demonstrates enhanced observability and communication with Dagster.
"""

import argparse
import sys

from common.dagster_utils import (
    check_silver_tables,
    dagster_pipes_context,
    report_no_data_metadata,
    report_success_metadata,
)

# Import common utilities
from common.spark_config import create_spark_session
from pyspark.sql.functions import *


def process_daily_sales_summary(target_date: str):
    """Process daily sales summary for the given date with Dagster Pipes"""
    try:
        with dagster_pipes_context() as context:
            context.log.info("Starting Spark daily sales summary job")
            context.log.info(f"Processing date: {target_date}")

            # Create Spark session using common configuration
            spark = create_spark_session(
                app_name=f"DailySalesSummary-{target_date}", include_kafka=False
            )

            context.log.info("Spark session initialized successfully")

            try:
                # Check available silver layer data
                check_silver_tables(context, spark)

                sales_summary = spark.sql(
                    f"""
                    WITH sales_metrics AS (
                        SELECT 
                            date(timestamp) as date,
                            count(*) as total_orders,
                            sum(total_amount) as total_revenue,
                            avg(total_amount) as avg_order_value,
                            count(distinct user_id) as unique_customers,
                            mode(payment_method) as top_payment_method
                        FROM iceberg.silver.purchases
                        WHERE date(timestamp) >= date('{target_date}')
                        GROUP BY 1
                    ),
                    
                    traffic_metrics AS (
                        SELECT 
                            date(timestamp) as date,
                            count(*) as total_page_views,
                            count(distinct user_id) as unique_visitors,
                            count(distinct session_id) as total_sessions
                        FROM iceberg.silver.page_views
                        WHERE date(timestamp) >= date('{target_date}')
                        GROUP BY 1
                    )
                    
                    SELECT 
                        coalesce(sm.date, tm.date) as date,
                        coalesce(sm.total_orders, 0) as total_orders,
                        coalesce(sm.total_revenue, 0.0) as total_revenue,
                        coalesce(sm.avg_order_value, 0.0) as avg_order_value,
                        coalesce(sm.unique_customers, 0) as unique_customers,
                        sm.top_payment_method,
                        coalesce(tm.total_page_views, 0) as total_page_views,
                        coalesce(tm.unique_visitors, 0) as unique_visitors,
                        coalesce(tm.total_sessions, 0) as total_sessions,
                        CASE 
                            WHEN tm.unique_visitors > 0 
                            THEN cast(sm.unique_customers as double) / cast(tm.unique_visitors as double)
                            ELSE 0.0
                        END as conversion_rate,
                        CASE 
                            WHEN tm.unique_visitors > 0 
                            THEN sm.total_revenue / cast(tm.unique_visitors as double)
                            ELSE 0.0
                        END as revenue_per_visitor,
                        current_timestamp() as created_at
                    FROM sales_metrics sm
                    FULL OUTER JOIN traffic_metrics tm
                        ON sm.date = tm.date
                """
                )

                context.log.info(
                    "Computing daily sales summary from Iceberg silver layer tables"
                )

                total_days = sales_summary.count()
                context.log.info(f"Generated sales summary for {total_days} day(s)")

                if total_days > 0:
                    context.log.info("Sample sales summary results:")
                    sales_summary.show(5, truncate=False)

                    metrics = sales_summary.agg(
                        sum("total_orders").alias("total_orders"),
                        sum("total_revenue").alias("total_revenue"),
                        avg("avg_order_value").alias("avg_order_value"),
                        sum("unique_customers").alias("unique_customers"),
                        avg("conversion_rate").alias("avg_conversion_rate"),
                    ).collect()[0]

                    context.log.info("Saving sales summary to Iceberg gold layer")
                    sales_summary.write.format("iceberg").mode("overwrite").option(
                        "write.parquet.compression-codec", "snappy"
                    ).partitionBy("date").saveAsTable(
                        "iceberg.gold.daily_sales_summary"
                    )

                    context.log.info(
                        "Successfully saved sales summary to iceberg.gold.daily_sales_summary"
                    )

                    # Report success with standardized metadata
                    report_success_metadata(
                        context=context,
                        target_date=target_date,
                        spark=spark,
                        table_location="iceberg.gold.daily_sales_summary",
                        custom_metrics={
                            "total_days_processed": total_days,
                            "total_orders": int(metrics["total_orders"] or 0),
                            "total_revenue": f"${float(metrics['total_revenue'] or 0):.2f}",
                            "avg_order_value": f"${float(metrics['avg_order_value'] or 0):.2f}",
                            "unique_customers": int(metrics["unique_customers"] or 0),
                            "avg_conversion_rate": f"{float(metrics['avg_conversion_rate'] or 0) * 100:.2f}%",
                        },
                    )
                else:
                    context.log.warning(" No sales data found for the specified date")
                    report_no_data_metadata(
                        context=context,
                        target_date=target_date,
                        table_location="iceberg.gold.daily_sales_summary",
                    )

            except Exception as e:
                context.log.error(f"Error during processing: {str(e)}")
                context.log.error(f"Error type: {type(e).__name__}")
                import traceback

                context.log.error(f"Traceback: {traceback.format_exc()}")
                raise
            finally:
                spark.stop()
                context.log.info("Spark session stopped")

    except Exception as e:
        print(f"Fatal error in process_daily_sales_summary: {str(e)}")
        import traceback

        print(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process daily sales summary analytics"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Target date in YYYY-MM-DD format",
    )

    args = parser.parse_args()

    print(f"Starting daily sales summary processing for {args.date}")
    process_daily_sales_summary(args.date)
    print("Daily sales summary processing completed successfully")
