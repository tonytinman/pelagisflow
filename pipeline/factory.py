"""
Pipeline factory for creating appropriate pipeline instances.

Provides a registry-based factory pattern for pipeline creation.
"""

from typing import Dict, Type
from nova_framework.pipeline.base import BasePipeline
from nova_framework.pipeline.strategies import (
    IngestionPipeline,
    TransformationPipeline,
    ValidationPipeline
)
from nova_framework.core.context import ExecutionContext
from nova_framework.observability.stats import PipelineStats
from nova_framework.observability.logging import get_logger

logger = get_logger("pipeline.factory")


class PipelineFactory:
    """
    Factory for creating pipeline instances based on type.
    
    Supports:
    - Built-in pipelines (ingestion, transformation, validation)
    - Custom pipeline registration
    - Contract-driven pipeline selection
    """
    
    # Registry of available pipelines
    PIPELINES: Dict[str, Type[BasePipeline]] = {
        "ingestion": IngestionPipeline,
        "transformation": TransformationPipeline,
        "validation": ValidationPipeline
    }
    
    @classmethod
    def create(
        cls,
        context: ExecutionContext,
        stats: PipelineStats
    ) -> BasePipeline:
        """
        Create pipeline based on context.contract.pipeline_type.
        
        Args:
            context: Execution context with loaded contract
            stats: Statistics tracker
            
        Returns:
            Concrete pipeline instance
            
        Raises:
            ValueError: If pipeline_type is unknown
            
        Example:
            context = ExecutionContext(...)
            stats = PipelineStats(process_queue_id=1)
            
            pipeline = PipelineFactory.create(context, stats)
            result = pipeline.execute()
        """
        pipeline_type = context.contract.pipeline_type
        
        logger.info(f"Creating pipeline of type: {pipeline_type}")
        
        pipeline_class = cls.PIPELINES.get(pipeline_type)
        
        if pipeline_class is None:
            available = list(cls.PIPELINES.keys())
            raise ValueError(
                f"Unknown pipeline_type '{pipeline_type}' from contract: "
                f"'{context.data_contract_name}'. "
                f"Available types: {available}"
            )
        
        # Instantiate pipeline
        pipeline = pipeline_class(context, stats)
        
        logger.info(f"Created {pipeline.__class__.__name__}")
        
        return pipeline
    
    @classmethod
    def register_pipeline(
        cls, 
        name: str, 
        pipeline_class: Type[BasePipeline]
    ):
        """
        Register a custom pipeline type.
        
        Args:
            name: Pipeline type name (used in contract)
            pipeline_class: Pipeline class (must extend BasePipeline)
            
        Example:
            class MyCustomPipeline(BasePipeline):
                def build_stages(self):
                    return [...]
            
            PipelineFactory.register_pipeline("my_custom", MyCustomPipeline)
            
            # Then in contract:
            # customProperties:
            #   pipelineType: my_custom
        """
        if not issubclass(pipeline_class, BasePipeline):
            raise TypeError(
                f"Pipeline class must extend BasePipeline, "
                f"got {pipeline_class.__name__}"
            )
        
        cls.PIPELINES[name] = pipeline_class
        logger.info(f"Registered custom pipeline: {name} -> {pipeline_class.__name__}")
    
    @classmethod
    def unregister_pipeline(cls, name: str):
        """
        Unregister a pipeline type.
        
        Args:
            name: Pipeline type name to remove
        """
        if name in cls.PIPELINES:
            del cls.PIPELINES[name]
            logger.info(f"Unregistered pipeline: {name}")
    
    @classmethod
    def list_pipelines(cls) -> Dict[str, Type[BasePipeline]]:
        """
        Get all registered pipeline types.
        
        Returns:
            Dictionary mapping pipeline names to classes
        """
        return cls.PIPELINES.copy()