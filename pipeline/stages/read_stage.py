"""
Read Stage - Reads data from source.

This stage uses IOFactory to create the appropriate reader based on
the reader type (file or table).
"""

from pyspark.sql import DataFrame
from typing import Optional
from pelagisflow.pipeline.stages.base import AbstractStage
from pelagisflow.io.factory import IOFactory


class ReadStage(AbstractStage):
    """
    Stage for reading data from source.
    
    Uses IOFactory to create appropriate reader based on reader_type.
    
    Args:
        context: Execution context
        stats: Statistics tracker
        reader_type: Type of reader ("file" or "table")
        
    Example:
        # Read from files (default)
        stage = ReadStage(context, stats)
        
        # Read from table
        stage = ReadStage(context, stats, reader_type="table")
    """
    
    def __init__(self, context, stats, reader_type: str = "file"):
        super().__init__(context, stats, "Read")
        self.reader_type = reader_type
    
    def execute(self, df: Optional[DataFrame]) -> DataFrame:
        """
        Read data from source.
        
        Note: Input df is ignored (read is typically first stage)
        
        Returns:
            DataFrame with read data
            
        Example:
            df = stage.execute(None)
            # Returns DataFrame with data from source
        """
        self.logger.info(f"Reading data using {self.reader_type} reader")
        
        # Create reader from factory
        reader = IOFactory.create_reader(
            self.reader_type,
            self.context,
            self.stats
        )
        
        # Read data
        df_read, read_report = reader.read(verbose=True)
        
        # Store report in context for later access
        self.context.set_state("read_report", read_report)
        
        # Log read metrics
        total_rows = read_report.get("total_rows", 0)
        valid_rows = read_report.get("valid_rows", 0)
        invalid_rows = read_report.get("invalid_rows", 0)
        
        self.stats.log_rows_read(total_rows)
        if invalid_rows > 0:
            self.stats.log_rows_invalid(invalid_rows)
        
        self.logger.info(
            f"Read complete: {total_rows} total rows, "
            f"{valid_rows} valid, {invalid_rows} invalid"
        )
        
        return df_read