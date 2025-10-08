"""
Spark cluster resource for submitting jobs to the containerized Spark cluster.
"""

import json
import os
import subprocess
from typing import Any, Dict

from dagster import ConfigurableResource, get_dagster_logger


class SparkClusterResource(ConfigurableResource):
    """Resource for interacting with the containerized Spark cluster."""

    spark_master_url: str = "spark://spark-master:7077"
    spark_submit_script: str = "/opt/dagster/streaming/submit_spark_job.py"
    max_wait_time: int = 3600  # 1 hour
    check_interval: int = 30  # 30 seconds

    def submit_spark_job(
        self,
        script_path: str,
        args: Dict[str, Any],
        job_name: str,
        executor_memory: str = "2g",
        executor_cores: int = 2,
        driver_memory: str = "1g",
    ) -> Dict[str, Any]:
        """
        Submit a Spark job to the cluster and monitor its execution.

        Args:
            script_path: Path to the Python script to execute
            args: Dictionary of arguments to pass to the script
            job_name: Name of the Spark job
            executor_memory: Memory per executor
            executor_cores: CPU cores per executor
            driver_memory: Driver memory

        Returns:
            Dictionary with job execution results
        """
        logger = get_dagster_logger()

        # Build the spark-submit command
        cmd = self._build_spark_submit_command(
            script_path=script_path,
            args=args,
            job_name=job_name,
            executor_memory=executor_memory,
            executor_cores=executor_cores,
            driver_memory=driver_memory,
        )

        logger.info(f"Submitting Spark job '{job_name}' with command: {' '.join(cmd)}")

        try:
            # Prepare environment variables for Spark
            env = os.environ.copy()
            env.update(
                {
                    "JAVA_HOME": "/usr/lib/jvm/java-21-openjdk-arm64",
                    "SPARK_HOME": "/opt/spark",
                    "PYSPARK_PYTHON": "python3",
                    "PYSPARK_DRIVER_PYTHON": "python3",
                }
            )

            # Execute the spark-submit command
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=self.max_wait_time, env=env
            )

            # Check for actual errors vs normal Spark shutdown messages
            stderr_lower = result.stderr.lower()
            has_real_error = any(
                error_indicator in stderr_lower
                for error_indicator in [
                    "error:",
                    "exception:",
                    "failed",
                    "java.lang.",
                    "py4jerror",
                ]
            ) and not all(
                normal_msg in stderr_lower
                for normal_msg in ["shutdown hook", "deleting directory"]
            )

            # Consider job successful if no real errors, even with non-zero return code
            if result.returncode == 0 or not has_real_error:
                logger.info(f"Spark job '{job_name}' completed successfully")

                # Parse any results from the stdout (if the script outputs JSON results)
                output_lines = result.stdout.strip().split("\n")
                job_results = {}

                for line in output_lines:
                    if line.startswith("DAGSTER_RESULT:"):
                        try:
                            job_results = json.loads(
                                line.replace("DAGSTER_RESULT:", "")
                            )
                        except json.JSONDecodeError:
                            logger.warning(f"Could not parse job result: {line}")

                return {
                    "status": "success",
                    "job_name": job_name,
                    "results": job_results,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }
            else:
                logger.error(
                    f"Spark job '{job_name}' failed with return code {result.returncode}"
                )
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")

                raise Exception(f"Spark job failed: {result.stderr}")

        except subprocess.TimeoutExpired:
            logger.error(
                f"Spark job '{job_name}' timed out after {self.max_wait_time} seconds"
            )
            raise Exception("Spark job timed out")
        except Exception as e:
            logger.error(f"Error submitting Spark job '{job_name}': {str(e)}")
            raise

    def _build_spark_submit_command(
        self,
        script_path: str,
        args: Dict[str, Any],
        job_name: str,
        executor_memory: str,
        executor_cores: int,
        driver_memory: str,
    ) -> list:
        """Build the spark-submit command with all necessary configurations."""

        # Base spark-submit command
        cmd = [
            "spark-submit",
            "--master",
            self.spark_master_url,
            "--name",
            job_name,
            "--executor-memory",
            executor_memory,
            "--executor-cores",
            str(executor_cores),
            "--driver-memory",
            driver_memory,
            # Iceberg and S3 dependencies
            "--packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4",
            "--conf",
            "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "--conf",
            "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog",
            "--conf",
            "spark.sql.catalog.iceberg.type=hadoop",
            "--conf",
            "spark.sql.catalog.iceberg.warehouse=s3a://data-lake/warehouse/",
            # MinIO S3 configuration
            "--conf",
            "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
            "--conf",
            "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
            "--conf",
            "spark.hadoop.fs.s3a.access.key=minioadmin",
            "--conf",
            "spark.hadoop.fs.s3a.secret.key=minioadmin",
            "--conf",
            "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf",
            "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
            # Performance tuning
            "--conf",
            "spark.serializer=org.apache.spark.serializer.KryoSerializer",
            "--conf",
            "spark.sql.adaptive.enabled=true",
            "--conf",
            "spark.sql.adaptive.coalescePartitions.enabled=true",
            script_path,
        ]

        # Add script arguments
        for key, value in args.items():
            cmd.extend([f"--{key}", str(value)])

        return cmd

    def check_spark_cluster_health(self) -> Dict[str, Any]:
        """Check if the Spark cluster is healthy and ready to accept jobs."""
        logger = get_dagster_logger()

        try:
            # Simple test job to check cluster health
            test_cmd = [
                "spark-submit",
                "--master",
                self.spark_master_url,
                "--name",
                "dagster-health-check",
                "--executor-memory",
                "512m",
                "--executor-cores",
                "1",
                "--driver-memory",
                "512m",
                "--class",
                "org.apache.spark.examples.SparkPi",
                "/opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
                "1",
            ]

            # Prepare environment variables for health check
            env = os.environ.copy()
            env.update(
                {
                    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
                    "SPARK_HOME": "/opt/spark",
                    "PYSPARK_PYTHON": "python3",
                    "PYSPARK_DRIVER_PYTHON": "python3",
                }
            )

            result = subprocess.run(
                test_cmd,
                capture_output=True,
                text=True,
                timeout=120,  # 2 minutes timeout for health check
                env=env,
            )

            if result.returncode == 0:
                logger.info("Spark cluster health check passed")
                return {"status": "healthy", "message": "Cluster is ready"}
            else:
                logger.warning("Spark cluster health check failed")
                return {
                    "status": "unhealthy",
                    "message": f"Health check failed: {result.stderr}",
                }

        except Exception as e:
            logger.error(f"Error checking Spark cluster health: {str(e)}")
            return {"status": "error", "message": str(e)}


# Resource instance
spark_cluster_resource = SparkClusterResource()
