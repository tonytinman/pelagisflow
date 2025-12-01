"""
Pipeline statistics tracking for Nova Framework.

Provides clean, consistent statistics tracking during pipeline execution.
All legacy code removed - clean implementation.
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, FloatType, StringType
from nova_framework.observability.logging import get_logger

logger = get_logger("observability.stats")


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
        logger.debug(f"Logged rows_read: {count}")

    def log_rows_written(self, count: int):
        """
        Log number of rows written to target.

        Args:
            count: Number of rows written
        """
        self.rows_written = count
        logger.debug(f"Logged rows_written: {count}")
    
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

    def _calculate_throughput(self, row_count: int) -> Optional[float]:
        """
        Calculate throughput in rows per second.

        Args:
            row_count: Number of rows processed

        Returns:
            Throughput in rows/second, or None if not calculable
        """
        if row_count <= 0 or self.execution_time_seconds <= 0:
            return None
        return row_count / self.execution_time_seconds

    def finalize(self):
        """
        Finalize statistics collection.

        Records end time, calculates throughput, and optionally persists to Delta table.
        """
        self.end_time = datetime.now()

        # Calculate throughput metrics
        read_throughput = self._calculate_throughput(self.rows_read)
        write_throughput = self._calculate_throughput(self.rows_written)

        # Store throughput in custom stats for persistence
        if read_throughput is not None:
            self.custom_stats['read_throughput_rows_per_sec'] = round(read_throughput, 2)
        if write_throughput is not None:
            self.custom_stats['write_throughput_rows_per_sec'] = round(write_throughput, 2)

        # Build throughput log message
        throughput_msg = []
        if read_throughput is not None:
            throughput_msg.append(f"read throughput: {read_throughput:.2f} rows/sec")
        if write_throughput is not None:
            throughput_msg.append(f"write throughput: {write_throughput:.2f} rows/sec")

        throughput_str = ", ".join(throughput_msg) if throughput_msg else "no throughput data"

        logger.info(f"Finalizing stats for process_queue_id={self.process_queue_id}: "
                   f"{self.rows_read} rows read, {self.rows_written} rows written, "
                   f"{self.execution_time_seconds:.2f}s execution time, "
                   f"{throughput_str}")

        # Optionally persist to table (non-blocking)
        try:
            self._persist_to_table()
        except Exception as e:
            # Don't fail pipeline if stats persistence fails
            logger.error(f"Failed to persist stats to Delta table: {e}", exc_info=True)
    
    def _persist_to_table(self):
        """
        Persist statistics to Delta table.

        Writes to: {catalog}.nova_framework.pipeline_stats
        """
        # Get Spark session
        spark = SparkSession.getActiveSession()
        if not spark:
            logger.warning("No active Spark session - cannot persist stats to Delta table")
            return

        # Get catalog from environment
        try:
            from nova_framework.core.config import get_config
            config = get_config()
            catalog = config.get_catalog_name()
        except Exception as e:
            # Fallback if config not available
            logger.warning(f"Could not get catalog from config: {e}. Using default 'cluk_dev_nova'")
            catalog = "cluk_dev_nova"

        # Target table
        table_name = f"{catalog}.nova_framework.pipeline_stats"

        logger.info(f"Persisting pipeline stats to {table_name}")

        # Define explicit schema to avoid type inference issues
        schema = StructType([
            StructField("process_queue_id", IntegerType(), False),
            StructField("execution_start", TimestampType(), False),
            StructField("execution_end", TimestampType(), True),
            StructField("execution_time_seconds", FloatType(), False),
            StructField("rows_read", IntegerType(), False),
            StructField("rows_written", IntegerType(), False),
            StructField("rows_invalid", IntegerType(), False),
            StructField("custom_stats", StringType(), True),
            StructField("timestamp", TimestampType(), False)
        ])

        # Convert custom_stats dict to JSON string
        custom_stats_json = json.dumps(self.custom_stats) if self.custom_stats else None

        # Create data tuple with explicit schema
        data = [(
            self.process_queue_id,
            self.start_time,
            self.end_time,
            self.execution_time_seconds,
            self.rows_read,
            self.rows_written,
            self.rows_invalid,
            custom_stats_json,
            datetime.now()
        )]

        # Write to Delta table
        df = spark.createDataFrame(data, schema)
        df.write.format("delta").mode("append").saveAsTable(table_name)

        logger.info(f"Successfully persisted stats to {table_name}")
    
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