"""
Transformation Pipeline - Silver/Gold layer transformations.

Executes SQL/PySpark transformations to create curated datasets.
"""

from typing import List
from nova_framework.pipeline.base import BasePipeline
from nova_framework.pipeline.stages.base import AbstractStage
from nova_framework.pipeline.stages.transformation_stage import TransformationStage
from nova_framework.pipeline.stages.lineage_stage import LineageStage
from nova_framework.pipeline.stages.hashing_stage import HashingStage
from nova_framework.pipeline.stages.deduplication_stage import DeduplicationStage
from nova_framework.pipeline.stages.quality_stage import QualityStage
from nova_framework.pipeline.stages.write_stage import WriteStage
from nova_framework.pipeline.stages.access_control_stage import AccessControlStage


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
    7. Apply access control rules (GRANTs/REVOKEs)

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
            WriteStage(self.context, self.stats),
            AccessControlStage(self.context, self.stats, environment=self.context.env)
        ]