"""
Ingestion Pipeline - Bronze layer data ingestion.

Reads data from source files, applies transformations, and writes to bronze tables.
"""

from typing import List
from pelagisflow.pipeline.base import BasePipeline
from pelagisflow.pipeline.stages.base import AbstractStage
from pelagisflow.pipeline.stages.read_stage import ReadStage
from pelagisflow.pipeline.stages.lineage_stage import LineageStage
from pelagisflow.pipeline.stages.hashing_stage import HashingStage
from pelagisflow.pipeline.stages.deduplication_stage import DeduplicationStage
from pelagisflow.pipeline.stages.quality_stage import QualityStage
from pelagisflow.pipeline.stages.write_stage import WriteStage


class IngestionPipeline(BasePipeline):
    """
    Ingestion pipeline for bronze layer data.
    
    Flow:
    1. Read data from source files
    2. Add lineage tracking columns
    3. Add hash columns for change tracking
    4. Remove duplicates
    5. Apply data quality rules
    6. Write to target table
    
    Use case: Ingesting raw data from external sources into bronze layer.
    
    Example:
        context = ExecutionContext(
            process_queue_id=1,
            data_contract_name="customer_data",
            source_ref="2024-11-28",
            env="dev"
        )
        stats = PipelineStats(process_queue_id=1)
        
        pipeline = IngestionPipeline(context, stats)
        result = pipeline.execute()
    """
    
    def build_stages(self) -> List[AbstractStage]:
        """
        Build stages for ingestion pipeline.
        
        Returns:
            List of stages to execute
        """
        return [
            ReadStage(self.context, self.stats, reader_type="file"),
            LineageStage(self.context, self.stats),
            HashingStage(self.context, self.stats),
            DeduplicationStage(self.context, self.stats),
            QualityStage(self.context, self.stats),
            WriteStage(self.context, self.stats)
        ]