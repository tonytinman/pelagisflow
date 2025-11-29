"""
Hash column processor for change tracking.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List


class HashingProcessor:
    """Adds hash columns for change tracking."""
    
    @staticmethod
    def add_hash_column(
        df: DataFrame,
        column_name: str,
        key_columns: List[str]
    ) -> DataFrame:
        """
        Add hash column based on key columns.
        
        Args:
            df: Input DataFrame
            column_name: Name for hash column
            key_columns: Columns to include in hash
            
        Returns:
            DataFrame with hash column added
        """
        return df.withColumn(
            column_name,
            F.hash(*[F.coalesce(F.col(c), F.lit("NULL")) for c in key_columns])
        )
    
    @staticmethod
    def add_partition_hash(
        df: DataFrame,
        num_partitions: int = 100,
        source_column: str = "natural_key_hash",
        target_column: str = "partition_key"
    ) -> DataFrame:
        """
        Add partition hash column.
        
        Args:
            df: Input DataFrame
            num_partitions: Number of partitions
            source_column: Column to hash
            target_column: Name for partition column
            
        Returns:
            DataFrame with partition column added
        """
        return df.withColumn(
            target_column,
            F.abs(F.col(source_column)) % num_partitions
        )