"""
Simple Pipes-enabled Spark script to test enhanced observability.
"""

import sys
from datetime import datetime

try:
    from dagster_pipes import open_dagster_pipes

    print("✓ dagster_pipes import successful")

    with open_dagster_pipes() as context:
        context.log.info("🚀 Starting simple Pipes test")
        context.log.info(f"📅 Processing date: {datetime.now().strftime('%Y-%m-%d')}")
        context.log.info(f"🐍 Python version: {sys.version}")

        # Simple processing simulation
        data_count = 1000
        processed_count = 950

        context.log.info(f"📊 Simulated processing: {data_count} records")
        context.log.info(f"✅ Successfully processed: {processed_count} records")

        # Report asset materialization with metadata
        context.report_asset_materialization(
            metadata={
                "records_processed": processed_count,
                "success_rate": f"{(processed_count/data_count)*100:.1f}%",
                "processing_time": "5.2 seconds",
                "test_status": "SUCCESS",
                "timestamp": datetime.now().isoformat(),
            },
            data_version="v1.0-test",
        )

        context.log.info("🎉 Simple Pipes test completed successfully!")

except ImportError as e:
    print(f"❌ Failed to import dagster_pipes: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error in Pipes execution: {e}")
    sys.exit(1)
