"""
Lineage Stage - Adds lineage tracking columns to DataFrame.

This stage adds standard lineage columns that track the source and
processing metadata for each row.
"""

from pyspark.sql import DataFrame
from pelagisflow.pipeline.stages.base import AbstractStage
from pelagisflow.pipeline.processors.lineage import LineageProcessor


class LineageStage(AbstractStage):
    """
    Stage for adding lineage columns to DataFrame.
    
    Adds the following columns:
    - source_file_path: Path to source file
    - source_file_name: Name of source file
    - process_queue_id: Process queue ID
    - processed_at: Timestamp when processed
    - processed_by: System that processed the data
    
    Args:
        context: Execution context
        stats: Statistics tracker
        
    Example:
        stage = LineageStage(context, stats)
        df_with_lineage = stage.execute(df)
    """
    
    def __init__(self, context, stats):
        super().__init__(context, stats, "Lineage")
        self.processor = LineageProcessor()
    
    def execute(self, df: DataFrame) -> DataFrame:
        """
        Add lineage columns to DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with lineage columns added
            
        Example:
            df_in = spark.createDataFrame([(1, "Alice"), (2, "Bob")])
            df_out = stage.execute(df_in)
            # df_out has additional columns:
            # - source_file_path
            # - source_file_name
            # - process_queue_id
            # - processed_at
            # - processed_by
        """
        self.logger.info("Adding lineage tracking columns")
        
        df_with_lineage = self.processor.add_standard_lineage(
            df, 
            self.context.process_queue_id
        )
        
        self.logger.info("Lineage columns added successfully")
        
        return df_with_lineage