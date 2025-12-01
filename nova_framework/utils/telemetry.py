from datetime import datetime
from pyspark.sql import Row, SparkSession


class Telemetry:
    """Static telemetry logger using Databricks' native `spark`."""

    @classmethod
    def log(cls, origin: str, message: str, env: str = "dev"):
        timestamp = datetime.now()
        catalog = f"cluk_{env}_nova"
        table = f"{catalog}.nova_framework.telemetry"

        print(f"[{timestamp}] [{origin}] {message}")

        try:
            # Get or create Spark session (for jobs or modules where spark isn't defined)
            spark = globals().get("spark") or SparkSession.builder.getOrCreate()

            df = spark.createDataFrame(
                [Row(timestamp=timestamp, origin=origin, message=message)]
            )
            df.write.mode("append").saveAsTable(table)
        except Exception as e:
            print(f"[Telemetry Warning] Could not write telemetry: {e}")