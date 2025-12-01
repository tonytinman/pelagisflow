from datetime import datetime
from pyspark.sql import Row, SparkSession
from typing import Dict


class PipelineMetrics:
    """Static telemetry logger using Databricks' native `spark`."""

    @classmethod
    def log_metrics(cls, process_queue_id: str, metrics_dict: Dict[str, int], env: str = "dev"):
        timestamp = datetime.now()
        catalog = f"cluk_{env}_nova"
        table = f"{catalog}.nova_framework.pipeline_metrics"

        try:
            # Get or create Spark session (for jobs or modules where spark isn't defined)
            spark = globals().get("spark") or SparkSession.builder.getOrCreate()

            df = spark.createDataFrame(
                [Row(process_queue_id=process_queue_id, metrics_dict=metrics_dict, timestamp=timestamp)]
            )
            df.write.mode("append").saveAsTable(table)
        except Exception as e:
            print(f"[Warning] Could not write pipeline_metrics Table: {e}")
