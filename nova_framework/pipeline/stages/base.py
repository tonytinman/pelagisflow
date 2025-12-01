"""
Base classes for pipeline stages.

A stage is a single step in a pipeline that transforms data.
Stages can be chained together to form complete pipelines.
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Optional
from datetime import datetime
from nova_framework.core.context import ExecutionContext
from nova_framework.observability.stats import PipelineStats
from nova_framework.observability.logging import get_logger

logger = get_logger("pipeline.stages")


class AbstractStage(ABC):
    """
    Abstract base class for pipeline stages.
    
    Each stage represents a single operation in the pipeline.
    Stages are executed in sequence by the pipeline orchestrator.
    """
    
    def __init__(
        self, 
        context: ExecutionContext, 
        stats: PipelineStats,
        stage_name: Optional[str] = None
    ):
        """
        Initialize stage.
        
        Args:
            context: Execution context
            stats: Statistics tracker
            stage_name: Optional stage name (defaults to class name)
        """
        self.context = context
        self.stats = stats
        self.stage_name = stage_name or self.__class__.__name__
        self.logger = get_logger(f"pipeline.stages.{self.stage_name}")
    
    @abstractmethod
    def execute(self, df: Optional[DataFrame]) -> DataFrame:
        """
        Execute the stage logic.
        
        Args:
            df: Input DataFrame (None for first stage)
            
        Returns:
            Transformed DataFrame
            
        Raises:
            NotImplementedError: Must be implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement execute()")
    
    def run(self, df: Optional[DataFrame]) -> DataFrame:
        """
        Run the stage with logging, timing, and error handling.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
            
        Raises:
            Exception: If stage execution fails
        """
        self.logger.info(f"[{self.stage_name}] Starting...")
        start_time = datetime.now()
        
        try:
            # Execute stage logic
            result_df = self.execute(df)
            
            # Log success
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.logger.info(f"[{self.stage_name}] Completed in {duration:.2f}s")
            
            # Record timing metric
            self.stats.log_stat(f"stage_{self.stage_name}_duration_seconds", duration)
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"[{self.stage_name}] Failed: {str(e)}")
            raise
    
    def skip_condition(self) -> bool:
        """
        Determine if this stage should be skipped.
        
        Override this method to implement conditional stage execution.
        
        Returns:
            True if stage should be skipped, False otherwise
        """
        return False