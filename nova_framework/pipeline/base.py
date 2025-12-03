"""
Base pipeline class using Template Method pattern.

Defines the skeleton of pipeline execution while allowing subclasses
to customize the stages.
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from pyspark.sql import DataFrame
from core.context import ExecutionContext
from observability.stats import PipelineStats
from pipeline.stages.base import AbstractStage
from observability.logging import get_logger

logger = get_logger("pipeline.base")


class BasePipeline(ABC):
    """
    Abstract base class for all pipelines.
    
    Uses Template Method pattern to define pipeline skeleton.
    Subclasses define the stages to execute.
    """
    
    def __init__(self, context: ExecutionContext, stats: PipelineStats):
        """
        Initialize pipeline.
        
        Args:
            context: Execution context
            stats: Statistics tracker
        """
        self.context = context
        self.stats = stats
        self.stages: List[AbstractStage] = []
        self.logger = get_logger(f"pipeline.{self.__class__.__name__}")
    
    @abstractmethod
    def build_stages(self) -> List[AbstractStage]:
        """
        Build the list of stages for this pipeline.
        
        Returns:
            List of stages to execute
            
        Raises:
            NotImplementedError: Must be implemented by subclass
        """
        raise NotImplementedError("Subclasses must implement build_stages()")
    
    def execute(self) -> str:
        """
        Execute the pipeline by running all stages.
        
        Template method that defines the execution flow:
        1. Build stages
        2. Execute each stage in sequence
        3. Handle errors
        
        Returns:
            "SUCCESS" or "FAILED"
        """
        self.logger.info(f"[{self.__class__.__name__}] Starting pipeline execution")
        
        try:
            # Build stages
            self.stages = self.build_stages()
            
            self.logger.info(f"[{self.__class__.__name__}] Pipeline has {len(self.stages)} stages")
            
            # Execute stages in sequence
            df: Optional[DataFrame] = None
            
            for stage in self.stages:
                # Check skip condition
                if stage.skip_condition():
                    self.logger.info(f"[{stage.stage_name}] Skipped (skip condition met)")
                    continue
                
                # Run stage
                df = stage.run(df)
            
            self.logger.info(f"[{self.__class__.__name__}] Pipeline completed successfully")
            
            return "SUCCESS"
            
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Pipeline failed: {str(e)}")
            raise
    
    def validate(self) -> bool:
        """
        Validate pipeline configuration.
        
        Override this to add custom validation logic.
        
        Returns:
            True if valid, False otherwise
        """
        return True