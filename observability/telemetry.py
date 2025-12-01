"""
Telemetry and distributed tracing for Nova Framework.

Provides lightweight telemetry for tracking pipeline execution flow.
"""

from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, Row
from pelagisflow.core.config import get_config
from pelagisflow.observability.logging import get_logger

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
            
            telemetry_row = Row(
                timestamp=datetime.now(),
                origin=origin,
                message=message,
                process_queue_id=process_queue_id,
                metadata=metadata
            )
            
            df = spark.createDataFrame([telemetry_row])
            df.write.format("delta").mode("append").saveAsTable(table)
            
        except Exception as e:
            logger.error(f"Failed to persist telemetry: {e}")


# Backward compatibility
class Telemetry:
    """Legacy Telemetry class for backward compatibility."""
    
    @classmethod
    def log(cls, origin: str, message: str, env: str = "dev"):
        """Log telemetry event (legacy interface)."""
        TelemetryEmitter.emit(origin, message)