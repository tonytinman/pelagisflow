from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, Optional

from nova_framework.io.writers.base import AbstractWriter
from nova_framework.observability.logging import get_logger

logger = get_logger(__name__)


class SCD4Writer(AbstractWriter):
    """
    SCD Type 4 writer - Current + Historical table pattern.
    
    Maintains two tables:
    - Current table: Latest version only (fast queries)
    - Historical table: Full history with SCD2 pattern
    
    Use case: Large dimensions where current data needs to be fast
    """
    
    def write(
        self,
        df: DataFrame,
        current_table: Optional[str] = None,
        historical_table: Optional[str] = None,
        natural_key_col: str = "natural_key_hash",
        change_key_col: str = "change_key_hash",
        partition_col: str = "partition_key",
        process_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Write using SCD Type 4 pattern.
        
        Args:
            df: Incoming DataFrame
            current_table: Current table name
            historical_table: Historical table name
            natural_key_col: Natural key column
            change_key_col: Change tracking column
            partition_col: Partition column
            process_date: Effective date
            
        Returns:
            Dictionary with write statistics
        """
        # Build table names
        if current_table is None:
            base = f"{self.catalog}.{self.contract.schema_name}.{self.contract.table_name}"
            current_table = f"{base}_current"
            historical_table = f"{base}_history"
        
        logger.info(f"SCD4 write to current={current_table}, history={historical_table}")
        
        # Write to historical table using SCD2
        from nova_framework.io.writers.scd2 import SCD2Writer
        
        scd2_writer = SCD2Writer(self.context, self.pipeline_stats)
        scd2_stats = scd2_writer.write(
            df=df,
            target_table=historical_table,
            natural_key_col=natural_key_col,
            change_key_col=change_key_col,
            partition_col=partition_col,
            process_date=process_date
        )
        
        # Build current table from historical (only current records)
        historical_df = self.spark.table(historical_table)
        current_df = historical_df.filter("is_current = true")
        
        # Drop SCD2 columns for current table
        scd2_cols = ["effective_from", "effective_to", "is_current", "deletion_flag"]
        for col in scd2_cols:
            if col in current_df.columns:
                current_df = current_df.drop(col)
        
        # Overwrite current table
        current_count = current_df.count()
        
        current_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(current_table)
        
        logger.info(f"Current table updated: {current_count:,} rows")
        
        self._log_write_stats(current_count, "scd4_current")
        
        return {
            "strategy": "scd4",
            "current_table": current_table,
            "historical_table": historical_table,
            "current_rows": current_count,
            "historical_stats": scd2_stats
        }