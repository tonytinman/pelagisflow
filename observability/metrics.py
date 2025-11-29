"""
Metrics collection for Nova Framework.

Provides structured metrics collection with automatic aggregation
and persistence.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, Row
from nova_framework.core.config import get_config


@dataclass
class MetricsCollector:
    """
    Collects and tracks metrics for a pipeline execution.
    """
    
    process_queue_id: int
    metrics: Dict[str, Any] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    def record(self, key: str, value: Any):
        """Record a metric value."""
        self.metrics[key] = value
    
    def increment(self, key: str, amount: int = 1):
        """Increment a counter metric."""
        current = self.metrics.get(key, 0)
        self.metrics[key] = current + amount
    
    def timer_start(self, key: str):
        """Start a timer metric."""
        self.metrics[f"_{key}_start"] = datetime.now()
    
    def timer_end(self, key: str):
        """End a timer metric and record duration."""
        start = self.metrics.get(f"_{key}_start")
        if start:
            duration = (datetime.now() - start).total_seconds()
            self.metrics[key] = duration
            del self.metrics[f"_{key}_start"]
    
    def finalize(self):
        """Finalize metrics collection."""
        self.end_time = datetime.now()
        self._persist()
    
    def _persist(self):
        """Persist metrics to Delta table."""
        config = get_config()
        
        if not config.observability.metrics_enabled:
            return
        
        try:
            spark = SparkSession.getActiveSession()
            if not spark:
                return
            
            catalog = config.get_catalog_name()
            table = f"{catalog}.{config.observability.metrics_table}"
            
            metrics_row = Row(
                process_queue_id=self.process_queue_id,
                timestamp=self.start_time,
                duration_seconds=self.duration_seconds,
                metrics=self.metrics
            )
            
            df = spark.createDataFrame([metrics_row])
            df.write.format("delta").mode("append").saveAsTable(table)
            
        except Exception as e:
            print(f"[MetricsCollector] Failed to persist metrics: {e}")
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Get total execution duration."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        return {
            "process_queue_id": self.process_queue_id,
            "duration_seconds": self.duration_seconds,
            "metrics": self.metrics
        }