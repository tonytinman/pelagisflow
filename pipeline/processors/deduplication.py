"""
Deduplication processor.
"""

from pyspark.sql import DataFrame
from typing import List, Optional
from pelagisflow.observability.stats import PipelineStats


class DeduplicationProcessor:
    """Handles DataFrame deduplication."""
    
    @staticmethod
    def deduplicate(
        df: DataFrame,
        key_columns: List[str],
        stats: Optional[PipelineStats] = None
    ) -> DataFrame:
        """
        Remove duplicate rows based on key columns.
        
        Args:
            df: Input DataFrame
            key_columns: Columns to use for deduplication
            stats: Optional stats tracker
            
        Returns:
            Deduplicated DataFrame
        """
        original_count = df.count()
        df_deduped = df.dropDuplicates(key_columns)
        dedup_count = original_count - df_deduped.count()
        
        if stats:
            stats.log_stat("deduped", dedup_count) 
        
        return df_deduped