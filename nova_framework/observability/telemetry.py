"""
Telemetry and distributed tracing for Nova Framework.

Provides lightweight telemetry for tracking pipeline execution flow.
"""

import json
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType
from nova_framework.core.config import get_config
from nova_framework.observability.logging import get_logger

logger = get_logger("telemetry")


class TelemetryEmitter:
    """
    Emits telemetry events for pipeline execution.
    """
    
    @staticmethod
    def emit(
        origin: str,
        message: str,
        process_queue_id: Optional[int] = None,
        **metadata
    ):
        """
        Emit a telemetry event.
        
        Args:
            origin: Source of the event (e.g., "IngestionPipeline.execute")
            message: Event message
            process_queue_id: Optional process queue ID
            **metadata: Additional metadata
        """
        config = get_config()
        
        # Log to console/file
        logger.info(f"[{origin}] {message}", extra=metadata)
        
        # Persist to Delta if enabled
        if config.observability.telemetry_enabled:
            TelemetryEmitter._persist(origin, message, process_queue_id, metadata)
    
    @staticmethod
    def _persist(
        origin: str,
        message: str,
        process_queue_id: Optional[int],
        metadata: dict
    ):
        """Persist telemetry to Delta table."""
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                return

            config = get_config()
            catalog = config.get_catalog_name()
            table = f"{catalog}.{config.observability.telemetry_table}"

            # Define explicit schema to avoid type inference issues
            schema = StructType([
                StructField("timestamp", TimestampType(), False),
                StructField("origin", StringType(), False),
                StructField("message", StringType(), False),
                StructField("process_queue_id", IntegerType(), True),
                StructField("metadata", StringType(), True)
            ])

            # Convert metadata dict to JSON string
            metadata_json = json.dumps(metadata) if metadata else None

            # Create DataFrame with explicit schema
            data = [(
                datetime.now(),
                origin,
                message,
                process_queue_id,
                metadata_json
            )]

            df = spark.createDataFrame(data, schema)
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)

        except Exception as e:
            logger.error(f"Failed to persist telemetry: {e}")


# Backward compatibility
class Telemetry:
    """Legacy Telemetry class for backward compatibility."""
    
    @classmethod
    def log(cls, origin: str, message: str, env: str = "dev"):
        """Log telemetry event (legacy interface)."""
        TelemetryEmitter.emit(origin, message)