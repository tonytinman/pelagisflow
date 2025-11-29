"""
Metrics collection for Nova Framework.

Clean, simple metrics collection without legacy dependencies.
This module is optional - PipelineStats is the primary statistics interface.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional


@dataclass
class MetricsCollector:
    """
    Simple metrics collector for general-purpose metric tracking.
    
    Note: For pipeline execution, use PipelineStats instead.
    This class is maintained for backward compatibility and
    for non-pipeline metric collection.
    
    Example:
        collector = MetricsCollector(process_queue_id=1)
        collector.record("custom_metric", 100)
        collector.increment("counter")
        collector.finalize()
    """
    
    process_queue_id: int
    metrics: Dict[str, Any] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    def record(self, key: str, value: Any):
        """
        Record a metric value.
        
        Args:
            key: Metric name
            value: Metric value
        """
        self.metrics[key] = value
    
    def increment(self, key: str, amount: int = 1):
        """
        Increment a counter metric.
        
        Args:
            key: Metric name
            amount: Amount to increment (default: 1)
        """
        current = self.metrics.get(key, 0)
        self.metrics[key] = current + amount
    
    def timer_start(self, key: str):
        """
        Start a timer metric.
        
        Args:
            key: Timer name
        """
        self.metrics[f"_{key}_start"] = datetime.now()
    
    def timer_end(self, key: str):
        """
        End a timer metric and record duration.
        
        Args:
            key: Timer name
        """
        start_key = f"_{key}_start"
        start = self.metrics.get(start_key)
        
        if start:
            duration = (datetime.now() - start).total_seconds()
            self.metrics[key] = duration
            del self.metrics[start_key]
    
    def finalize(self):
        """Finalize metrics collection."""
        self.end_time = datetime.now()
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """
        Get total execution duration in seconds.
        
        Returns:
            Duration in seconds, or None if not finalized
        """
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def summary(self) -> Dict[str, Any]:
        """
        Get metrics summary.
        
        Returns:
            Dictionary with all metrics
        """
        return {
            "process_queue_id": self.process_queue_id,
            "duration_seconds": self.duration_seconds,
            "metrics": self.metrics
        }