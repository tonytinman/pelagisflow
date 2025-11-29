"""
Pipeline statistics tracking.

Tracks operational statistics like row counts, errors, etc.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional
from nova_framework.observability.metrics import MetricsCollector


@dataclass
class PipelineStats:
    """
    Tracks pipeline execution statistics.
    
    This is a convenience wrapper around MetricsCollector
    with pipeline-specific methods.
    """
    
    process_queue_id: int
    _collector: Optional[MetricsCollector] = None
    
    def __post_init__(self):
        """Initialize metrics collector."""
        if self._collector is None:
            self._collector = MetricsCollector(self.process_queue_id)
    
    # Row count tracking
    def log_rows_read(self, count: int):
        """Log number of rows read."""
        self._collector.record("rows_read", count)
    
    def log_rows_written(self, count: int):
        """Log number of rows written."""
        self._collector.record("rows_written", count)
    
    def log_rows_invalid(self, count: int):
        """Log number of invalid rows."""
        self._collector.record("rows_invalid", count)
    
    def log_rows_deduped(self, count: int):
        """Log number of rows deduplicated."""
        self._collector.record("rows_deduped", count)
    
    # Generic stat logging
    def log_stat(self, key: str, value: Any):
        """Log a custom statistic."""
        self._collector.record(key, value)
    
    def increment_stat(self, key: str, amount: int = 1):
        """Increment a counter statistic."""
        self._collector.increment(key, amount)
    
    # Timer methods
    def start_timer(self, key: str):
        """Start a timer."""
        self._collector.timer_start(key)
    
    def end_timer(self, key: str):
        """End a timer and record duration."""
        self._collector.timer_end(key)
    
    # Finalization
    def finalize(self):
        """Finalize and persist statistics."""
        self._collector.finalize()
    
    def summary(self) -> Dict:
        """Get statistics summary."""
        return self._collector.summary()