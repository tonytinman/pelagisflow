"""
Hashing Stage - Adds hash columns for change tracking.

This stage adds hash columns used for SCD (Slowly Changing Dimension)
tracking and partitioning.
"""

from pyspark.sql import DataFrame
from pelagisflow.pipeline.stages.base import AbstractStage
from pelagisflow.pipeline.processors.hashing import HashingProcessor


class HashingStage(AbstractStage):
    """
    Stage for adding hash columns for change tracking.
    
    Adds the following columns:
    - natural_key_hash: Hash of natural key columns
    - change_key_hash: Hash of change tracking columns
    - partition_key: Partition hash (modulo of natural_key_hash)
    
    Args:
        context: Execution context
        stats: Statistics tracker
        
    Example:
        stage = HashingStage(context, stats)
        df_with_hashes = stage.execute(df)
    """
    
    def __init__(self, context, stats):
        super().__init__(context, stats, "Hashing")
        self.processor = HashingProcessor()
    
    def execute(self, df: DataFrame) -> DataFrame:
        """
        Add hash columns to DataFrame.
        
        Hash columns are added based on contract configuration:
        - natural_key_columns → natural_key_hash
        - change_tracking_columns → change_key_hash
        - recommended_partitions → partition_key
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with hash columns added
            
        Example:
            # Contract specifies:
            # natural_key_columns: [customer_id]
            # change_tracking_columns: [name, email]
            
            df_out = stage.execute(df)
            # df_out has:
            # - natural_key_hash (hash of customer_id)
            # - change_key_hash (hash of name + email)
            # - partition_key (for data distribution)
        """
        self.logger.info("Adding hash columns for change tracking")
        
        # Add natural key hash
        natural_key_cols = self.context.contract.natural_key_columns
        if natural_key_cols:
            self.logger.info(f"Adding natural_key_hash from columns: {natural_key_cols}")
            df = self.processor.add_hash_column(
                df, 
                'natural_key_hash', 
                natural_key_cols
            )
        else:
            self.logger.warning("No natural_key_columns defined in contract")
        
        # Add change key hash
        change_key_cols = self.context.contract.change_tracking_columns
        if change_key_cols:
            self.logger.info(f"Adding change_key_hash from columns: {change_key_cols}")
            df = self.processor.add_hash_column(
                df, 
                'change_key_hash', 
                change_key_cols
            )
        else:
            self.logger.warning("No change_tracking_columns defined in contract")
        
        # Add partition hash (only if natural_key_hash exists)
        if 'natural_key_hash' in df.columns:
            num_partitions = self.context.contract.recommended_partitions
            self.logger.info(f"Adding partition_key with {num_partitions} partitions")
            df = self.processor.add_partition_hash(df, num_partitions)
        
        self.logger.info("Hash columns added successfully")
        
        return df
    
    def skip_condition(self) -> bool:
        """
        Skip this stage if no natural keys are defined.
        
        Returns:
            True if stage should be skipped, False otherwise
        """
        should_skip = not self.context.contract.natural_key_columns
        
        if should_skip:
            self.logger.info(
                "Skipping HashingStage - no natural_key_columns defined in contract"
            )
        
        return should_skip