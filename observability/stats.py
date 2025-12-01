"""
Pipeline statistics tracking for Nova Framework.

Provides clean, consistent statistics tracking during pipeline execution.
All legacy code removed - clean implementation.
"""

from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, Row


class PipelineStats:
    """
    Clean statistics tracker for pipeline execution.
    
    Tracks:
    - Row counts (read, written, invalid, deduped)
    - Custom statistics
    - Execution time
    
    Example:
        stats = PipelineStats(process_queue_id=1)
        stats.log_rows_read(100)
        stats.log_rows_written(95)
        stats.log_stat("deduped", 5)
        stats.finalize()
    """
    
    def __init__(self, process_queue_id: int):
        """
        Initialize statistics tracker.
        
        Args:
            process_queue_id: Process queue ID for this execution
        """
        self.process_queue_id = process_queue_id
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None
        
        # Core row counts
        self.rows_read = 0
        self.rows_written = 0
        self.rows_invalid = 0
        
        # Custom statistics dictionary
        self.custom_stats: Dict[str, Any] = {}
    
    # ========================================================================
    # Row Count Methods
    # ========================================================================
    
    def log_rows_read(self, count: int):
        """
        Log number of rows read from source.
        
        Args:
            count: Number of rows read
        """
        self.rows_read = count
    
    def log_rows_written(self, count: int):
        """
        Log number of rows written to target.
        
        Args:
            count: Number of rows written
        """
        self.rows_written = count
    
    def log_rows_invalid(self, count: int):
        """
        Log number of invalid rows.
        
        Args:
            count: Number of invalid rows
        """
        self.rows_invalid = count
    
    # ========================================================================
    # Custom Statistics Methods
    # ========================================================================
    
    def log_stat(self, key: str, value: Any):
        """
        Log a custom statistic.
        
        Args:
            key: Statistic name (e.g., "deduped", "dq_failed_rows")
            value: Statistic value (can be int, float, str, etc.)
            
        Example:
            stats.log_stat("deduped", 5)
            stats.log_stat("dq_total_rows", 148)
            stats.log_stat("dq_failed_pct", 8.1)
        """
        self.custom_stats[key] = value
    
    def increment_stat(self, key: str, amount: int = 1):
        """
        Increment a counter statistic.
        
        Args:
            key: Statistic name
            amount: Amount to increment by (default: 1)
            
        Example:
            stats.increment_stat("errors")
            stats.increment_stat("warnings", 3)
        """
        current_value = self.custom_stats.get(key, 0)
        self.custom_stats[key] = current_value + amount
    
    # ========================================================================
    # Finalization and Persistence
    # ========================================================================
    
    def finalize(self):
        """
        Finalize statistics collection.
        
        Records end time and optionally persists to Delta table.
        """
        self.end_time = datetime.now()
        
        # Optionally persist to table (non-blocking)
        try:
            self._persist_to_table()
        except Exception as e:
            # Don't fail pipeline if stats persistence fails
            print(f"[PipelineStats] Warning: Failed to persist stats: {e}")
    
    def _persist_to_table(self):
        """
        Persist statistics to Delta table.
        
        Writes to: {catalog}.nova_framework.pipeline_stats
        """
        try:
            # Get Spark session
            spark = SparkSession.getActiveSession()
            if not spark:
                return  # No Spark session available
            
            # Get catalog from environment
            # Try to get from config, fallback to default
            try:
                from pelagisflow.core.config import get_config
                config = get_config()
                catalog = config.get_catalog_name()
            except:
                # Fallback if config not available
                catalog = "cluk_dev_nova"
            
            # Target table
            table_name = f"{catalog}.nova_framework.pipeline_stats"
            
            # Create stats row
            stats_row = Row(
                process_queue_id=self.process_queue_id,
                execution_start=self.start_time,
                execution_end=self.end_time,
                execution_time_seconds=self.execution_time_seconds,
                rows_read=self.rows_read,
                rows_written=self.rows_written,
                rows_invalid=self.rows_invalid,
                custom_stats=self.custom_stats,
                timestamp=datetime.now()
            )
            
            # Write to Delta table
            df = spark.createDataFrame([stats_row])
            df.write.format("delta").mode("append").saveAsTable(table_name)
            
        except Exception as e:
            # Log but don't fail
            print(f"[PipelineStats] Could not persist to Delta: {e}")
    
    # ========================================================================
    # Properties and Summary
    # ========================================================================
    
    @property
    def execution_time_seconds(self) -> float:
        """
        Get execution time in seconds.
        
        Returns:
            Execution time in seconds (0 if not finalized)
        """
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def summary(self) -> Dict[str, Any]:
        """
        Get complete statistics summary.
        
        Returns:
            Dictionary containing all statistics
            
        Example:
            summary = stats.summary()
            print(summary)
            # {
            #   'process_queue_id': 1,
            #   'execution_time_seconds': 45.2,
            #   'rows_read': 150,
            #   'rows_written': 80,
            #   'rows_invalid': 0,
            #   'custom_stats': {
            #     'deduped': 2,
            #     'dq_total_rows': 148,
            #     'dq_failed_rows': 12,
            #     ...
            #   }
            # }
        """
        return {
            "process_queue_id": self.process_queue_id,
            "execution_time_seconds": self.execution_time_seconds,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_invalid": self.rows_invalid,
            "custom_stats": self.custom_stats
        }
    
    # ========================================================================
    # String Representation
    # ========================================================================
    
    def __repr__(self) -> str:
        """String representation of statistics."""
        return (
            f"PipelineStats("
            f"process_queue_id={self.process_queue_id}, "
            f"rows_read={self.rows_read}, "
            f"rows_written={self.rows_written}, "
            f"execution_time={self.execution_time_seconds:.2f}s"
            f")"
        )
    
    def __str__(self) -> str:
        """User-friendly string representation."""
        custom_count = len(self.custom_stats)
        return (
            f"Pipeline Stats (ID: {self.process_queue_id})\n"
            f"  Rows Read: {self.rows_read}\n"
            f"  Rows Written: {self.rows_written}\n"
            f"  Rows Invalid: {self.rows_invalid}\n"
            f"  Execution Time: {self.execution_time_seconds:.2f}s\n"
            f"  Custom Stats: {custom_count} tracked"
        )