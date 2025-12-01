from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, Optional
from nova_framework.core.context import ExecutionContext
from nova_framework.observability.stats import PipelineStats


class AbstractWriter(ABC):
    """
    Abstract base class for all data writers.
    
    All concrete writers must implement the write() method.
    """
    
    def __init__(self, context: ExecutionContext, pipeline_stats: PipelineStats):
        """
        Initialize writer with context.
        
        Args:
            context: Pipeline context containing contract and configuration
            pipeline_stats: Statistics tracker for metrics
        """
        self.context = context
        self.pipeline_stats = pipeline_stats
        self.contract = context.contract
        self.catalog = context.catalog
        self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise RuntimeError("No active Spark session found")
    
    @abstractmethod
    def write(self, df: DataFrame, **kwargs) -> Dict[str, Any]:
        """
        Write DataFrame using specific strategy.
        
        Args:
            df: DataFrame to write
            **kwargs: Strategy-specific parameters
            
        Returns:
            Dictionary with write statistics
            
        Raises:
            NotImplementedError: Must be implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement write()")
    
    def _get_target_table(self, table_name: Optional[str] = None) -> str:
        """Build fully qualified target table name."""
        if table_name:
            return table_name
        
        return f"{self.catalog}.{self.contract.schema_name}.{self.contract.table_name}"
    
    def _log_write_stats(self, rows_written: int, strategy: str):
        """Log write statistics."""
        self.pipeline_stats.log_stat("rows_written", rows_written)
        self.pipeline_stats.log_stat(f"write_strategy_{strategy}", rows_written)