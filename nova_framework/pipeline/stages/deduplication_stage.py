"""
Deduplication Stage - Removes duplicate rows.

This stage removes duplicate rows based on hash columns (natural_key_hash
and change_key_hash).
"""

from pyspark.sql import DataFrame
from nova_framework.pipeline.stages.base import AbstractStage
from nova_framework.pipeline.processors.deduplication import DeduplicationProcessor


class DeduplicationStage(AbstractStage):
    """
    Stage for removing duplicate rows.
    
    Uses natural_key_hash and change_key_hash for deduplication.
    Keeps the first occurrence of each unique combination.
    
    Args:
        context: Execution context
        stats: Statistics tracker
        
    Example:
        stage = DeduplicationStage(context, stats)
        df_deduped = stage.execute(df)
    """
    
    def __init__(self, context, stats):
        super().__init__(context, stats, "Deduplication")
        self.processor = DeduplicationProcessor()
    
    def execute(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate rows from DataFrame.
        
        Deduplication is based on hash columns:
        - If both natural_key_hash and change_key_hash exist, use both
        - If only one exists, use that one
        - If neither exists, skip deduplication
        
        Args:
            df: Input DataFrame
            
        Returns:
            Deduplicated DataFrame
            
        Example:
            # DataFrame with duplicates
            df = spark.createDataFrame([
                (1, "Alice", 100),
                (1, "Alice", 100),  # Duplicate
                (2, "Bob", 200)
            ])
            
            # After deduplication
            df_deduped = stage.execute(df)
            # Returns 2 rows (duplicate removed)
        """
        self.logger.info("Starting deduplication")
        
        # Determine which columns to use for deduplication
        key_columns = []
        
        if 'natural_key_hash' in df.columns:
            key_columns.append('natural_key_hash')
            self.logger.info("Using natural_key_hash for deduplication")
        
        if 'change_key_hash' in df.columns:
            key_columns.append('change_key_hash')
            self.logger.info("Using change_key_hash for deduplication")
        
        # If no hash columns, can't deduplicate
        if not key_columns:
            self.logger.warning(
                "No hash columns found (natural_key_hash, change_key_hash), "
                "skipping deduplication"
            )
            return df
        
        # Perform deduplication
        original_count = df.count()
        df_deduped = self.processor.deduplicate(df, key_columns, self.stats)
        final_count = df_deduped.count()
        removed_count = original_count - final_count
        
        self.logger.info(
            f"Deduplication complete: {original_count} → {final_count} rows "
            f"({removed_count} duplicates removed)"
        )
        
        return df_deduped
    
    def skip_condition(self) -> bool:
        """
        Skip if no hash columns will exist.
        
        Note: We can't check the DataFrame here (don't have it yet),
        so we check if HashingStage would have run.
        
        Returns:
            True if stage should be skipped, False otherwise
        """
        # If HashingStage was skipped (no natural keys), we should skip too
        should_skip = not self.context.contract.natural_key_columns
        
        if should_skip:
            self.logger.info(
                "Skipping DeduplicationStage - no hash columns available"
            )
        
        return should_skip