from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, Any, Tuple
from nova_framework.core.context import ExecutionContext
from nova_framework.observability.stats import PipelineStats


class AbstractReader(ABC):
    """
    Abstract base class for all data readers.
    
    All concrete readers must implement the read() method.
    Provides common functionality for schema validation, metrics, etc.
    """
    
    def __init__(self, context: ExecutionContext, pipeline_stats: PipelineStats):
        """
        Initialize reader with context.
        
        Args:
            context: Pipeline context containing contract and configuration
            pipeline_stats: Statistics tracker for metrics
        """
        self.context = context
        self.pipeline_stats = pipeline_stats
        self.contract = context.contract
        self.catalog = context.catalog
        
    @abstractmethod
    def read(self, **kwargs) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Read data and return DataFrame with read report.
        
        Returns:
            Tuple of (DataFrame, read_report_dict)
            
        Raises:
            NotImplementedError: Must be implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement read()")
    
    def _log_read_stats(self, total_rows: int, valid_rows: int, invalid_rows: int):
        """Log read statistics to pipeline stats."""
        self.pipeline_stats.log_stat("rows_read", total_rows)
        self.pipeline_stats.log_stat("rows_valid", valid_rows)
        self.pipeline_stats.log_stat("rows_invalid", invalid_rows)