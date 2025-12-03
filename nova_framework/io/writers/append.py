from pyspark.sql import DataFrame
from typing import Dict, Any, Optional
import logging

from nova_framework.io.writers.base import AbstractWriter

logger = logging.getLogger(__name__)


class AppendWriter(AbstractWriter):
    """
    Appends new data to target table.
    
    Use case: Fact tables, event logs, audit tables
    """
    
    def write(
        self,
        df: DataFrame,
        target_table: Optional[str] = None,
        partition_cols: Optional[list] = None,
        deduplicate: bool = False,
        dedup_cols: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Append data to target table.
        
        Args:
            df: DataFrame to write
            target_table: Target table name (default: from contract)
            partition_cols: Columns to partition by
            deduplicate: Remove duplicates before appending
            dedup_cols: Columns to use for deduplication
            
        Returns:
            Dictionary with write statistics
        """
        target = self._get_target_table(target_table)
        
        # Deduplicate if requested
        if deduplicate:
            original_count = df.count()
            if dedup_cols:
                df = df.dropDuplicates(dedup_cols)
            else:
                df = df.dropDuplicates()
            
            dedup_count = original_count - df.count()
            logger.info(f"Removed {dedup_count:,} duplicate rows")
            self.pipeline_stats.log_stat("rows_deduped", dedup_count)
        
        row_count = df.count()
        logger.info(f"Appending {row_count:,} rows to {target}")
        
        # Write
        writer = df.write.format("delta").mode("append")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.saveAsTable(target)
        
        # Log stats
        self._log_write_stats(row_count, "append")
        
        return {
            "strategy": "append",
            "target_table": target,
            "rows_written": row_count,
            "deduplicated": deduplicate,
            "dedup_removed": dedup_count if deduplicate else 0
        }