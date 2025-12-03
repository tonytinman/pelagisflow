from pyspark.sql import DataFrame
from typing import Dict, Any, Optional
import logging

from nova_framework.io.writers.base import AbstractWriter

logger = logging.getLogger(__name__)


class OverwriteWriter(AbstractWriter):
    """
    Overwrites the target table with new data.
    
    Use case: Full refresh of dimensional tables, static reference data
    """
    
    def write(
        self,
        df: DataFrame,
        target_table: Optional[str] = None,
        partition_cols: Optional[list] = None,
        optimize: bool = True
    ) -> Dict[str, Any]:
        """
        Overwrite target table.
        
        Args:
            df: DataFrame to write
            target_table: Target table name (default: from contract)
            partition_cols: Columns to partition by
            optimize: Run OPTIMIZE after write
            
        Returns:
            Dictionary with write statistics
        """
        target = self._get_target_table(target_table)
        row_count = df.count()
        
        logger.info(f"Overwriting {row_count:,} rows to {target}")
        
        # Write
        writer = df.write.format("delta").mode("overwrite")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        elif self.contract.natural_key_columns:
            # Use natural keys for partitioning if specified
            partition_col = "partition_key"
            if partition_col in df.columns:
                writer = writer.partitionBy(partition_col)
        
        writer.saveAsTable(target)
        
        # Optimize
        if optimize:
            self.spark.sql(f"OPTIMIZE {target}")
            logger.info(f"Optimized {target}")
        
        # Log stats
        self._log_write_stats(row_count, "overwrite")
        
        return {
            "strategy": "overwrite",
            "target_table": target,
            "rows_written": row_count,
            "partitioned": partition_cols is not None,
            "optimized": optimize
        }