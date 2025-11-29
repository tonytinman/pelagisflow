"""
Lineage tracking processor.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class LineageProcessor:
    """Adds lineage tracking columns to DataFrames."""
    
    @staticmethod
    def add_standard_lineage(
        df: DataFrame,
        process_queue_id: int,
        processed_by: str = "nova_framework"
    ) -> DataFrame:
        """
        Add standard lineage columns.
        
        Args:
            df: Input DataFrame
            process_queue_id: Process queue ID
            processed_by: Name of processing system
            
        Returns:
            DataFrame with lineage columns added
        """
        return df \
            .withColumn("source_file_path", F.col("_metadata.file_path")) \
            .withColumn("source_file_name", F.col("_metadata.file_name")) \
            .withColumn("process_queue_id", F.lit(process_queue_id)) \
            .withColumn("processed_at", F.current_timestamp()) \
            .withColumn("processed_by", F.lit(processed_by))