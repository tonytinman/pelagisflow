"""
Pipeline Stages Module

This module contains all stage implementations for building pipelines.
Stages are composable units that can be combined to create complete pipelines.

Available Stages:
- ReadStage: Read data from source (files or tables)
- LineageStage: Add lineage tracking columns
- HashingStage: Add hash columns for change tracking
- DeduplicationStage: Remove duplicate rows
- QualityStage: Apply data quality rules
- WriteStage: Write data to target
- TransformationStage: Execute transformations

Example:
    from nova_framework.pipeline.stages import (
        ReadStage,
        LineageStage,
        WriteStage
    )
    
    # Create stages
    read = ReadStage(context, stats)
    lineage = LineageStage(context, stats)
    write = WriteStage(context, stats)
    
    # Execute in sequence
    df = read.run(None)
    df = lineage.run(df)
    df = write.run(df)
"""

from nova_framework.pipeline.stages.base import AbstractStage
from nova_framework.pipeline.stages.read_stage import ReadStage
from nova_framework.pipeline.stages.lineage_stage import LineageStage
from nova_framework.pipeline.stages.hashing_stage import HashingStage
from nova_framework.pipeline.stages.deduplication_stage import DeduplicationStage
from nova_framework.pipeline.stages.quality_stage import QualityStage
from nova_framework.pipeline.stages.write_stage import WriteStage
from nova_framework.pipeline.stages.transformation_stage import TransformationStage

__all__ = [
    # Base class
    "AbstractStage",
    
    # Concrete stages
    "ReadStage",
    "LineageStage",
    "HashingStage",
    "DeduplicationStage",
    "QualityStage",
    "WriteStage",
    "TransformationStage",
]