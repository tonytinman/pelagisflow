"""
Transformation Pipeline - Silver/Gold layer transformations.

Executes SQL/PySpark transformations to create curated datasets.
"""

from typing import List
from pelagisflow.pipeline.base import BasePipeline
from pelagisflow.pipeline.stages.base import AbstractStage
from pelagisflow.pipeline.stages.transformaton_stage import TransformationStage
from pelagisflow.pipeline.stages.lineage_stage import LineageStage
from pelagisflow.pipeline.stages.hashing_stage import HashingStage
from pelagisflow.pipeline.stages.deduplication_stage import DeduplicationStage
from pelagisflow.pipeline.stages.quality_stage import QualityStage
from pelagisflow.pipeline.stages.write_stage import WriteStage


class TransformationPipeline(BasePipeline):
    """
    Transformation pipeline for silver/gold layer data.
    
    Flow:
    1. Execute SQL transformation (reads from bronze/silver tables)
    2. Add lineage tracking columns
    3. Add hash columns (if natural keys defined)
    4. Remove duplicates (if applicable)
    5. Apply data quality rules
    6. Write to target table
    
    Use case: Transforming bronze data to silver/gold layers using SQL.
    
    Contract requirements:
        customProperties:
          transformationSql: |
            SELECT ... FROM bronze_schema.table ...
    
    Example:
        context = ExecutionContext(
            process_queue_id=2,
            data_contract_name="customer_summary",
            source_ref="2024-11-28",
            env="dev"
        )
        stats = PipelineStats(process_queue_id=2)
        
        pipeline = TransformationPipeline(context, stats)
        result = pipeline.execute()
    """
    
    def build_stages(self) -> List[AbstractStage]:
        """
        Build stages for transformation pipeline.
        
        Returns:
            List of stages to execute
        """
        return [
            TransformationStage(self.context, self.stats),
            LineageStage(self.context, self.stats),
            HashingStage(self.context, self.stats),
            DeduplicationStage(self.context, self.stats),
            QualityStage(self.context, self.stats),
            WriteStage(self.context, self.stats)
        ]