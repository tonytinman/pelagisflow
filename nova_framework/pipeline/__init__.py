"""
Nova Framework Pipeline Module

Provides stage-based pipeline orchestration for data processing workflows.

Main Components:
- orchestrator: Pipeline entry point
- factory: Pipeline factory for creating pipeline instances
- base: BasePipeline abstract class
- strategies: Concrete pipeline implementations (ingestion, transformation, validation)
- stages: Individual pipeline stages (read, lineage, hash, dedup, quality, write, transform)
- processors: Data processing utilities (lineage, hashing, deduplication)

Usage:
    from pipeline import Pipeline

    # Create and run pipeline
    pipeline = Pipeline()
    result = pipeline.run(
        process_queue_id=1,
        data_contract_name="customer_data",
        source_ref="2024-11-28",
        env="dev"
    )

    print(f"Result: {result}")  # "SUCCESS"
"""

# Main entry point
from pipeline.orchestrator import Pipeline

# Factory
from pipeline.factory import PipelineFactory

# Base class
from pipeline.base import BasePipeline

# Concrete pipeline strategies
from pipeline.strategies import (
    IngestionPipeline,
    TransformationPipeline,
    ValidationPipeline
)

# Stages (for custom pipelines)
from pipeline.stages import (
    AbstractStage,
    ReadStage,
    LineageStage,
    HashingStage,
    DeduplicationStage,
    QualityStage,
    WriteStage,
    TransformationStage
)

# Processors (for custom transformations)
from pipeline.processors import (
    LineageProcessor,
    HashingProcessor,
    DeduplicationProcessor
)

__all__ = [
    # Main entry point
    "Pipeline",
    
    # Factory
    "PipelineFactory",
    
    # Base class for custom pipelines
    "BasePipeline",
    
    # Concrete pipeline strategies
    "IngestionPipeline",
    "TransformationPipeline",
    "ValidationPipeline",
    
    # Stages
    "AbstractStage",
    "ReadStage",
    "LineageStage",
    "HashingStage",
    "DeduplicationStage",
    "QualityStage",
    "WriteStage",
    "TransformationStage",
    
    # Processors
    "LineageProcessor",
    "HashingProcessor",
    "DeduplicationProcessor",
]