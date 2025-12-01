"""
Validation Pipeline - Data quality validation only.

Reads data and applies quality rules without writing to target.
"""

from typing import List
from pelagisflow.pipeline.base import BasePipeline
from pelagisflow.pipeline.stages.base import AbstractStage
from pelagisflow.pipeline.stages.read_stage import ReadStage
from pelagisflow.pipeline.stages.quality_stage import QualityStage


class ValidationPipeline(BasePipeline):
    """
    Validation-only pipeline for data quality checks.
    
    Flow:
    1. Read data from table
    2. Apply data quality rules
    3. Report results (no writes to target table)
    
    Use case: 
    - Ad-hoc data quality audits
    - Validation runs on existing data
    - DQ smoke tests
    
    Note: This pipeline does NOT write to the target table,
    only to DQ errors table.
    
    Example:
        context = ExecutionContext(
            process_queue_id=3,
            data_contract_name="customer_data",
            source_ref="audit_20241128",
            env="dev"
        )
        stats = PipelineStats(process_queue_id=3)
        
        pipeline = ValidationPipeline(context, stats)
        result = pipeline.execute()
    """
    
    def build_stages(self) -> List[AbstractStage]:
        """
        Build stages for validation pipeline.
        
        Returns:
            List of stages to execute
        """
        return [
            ReadStage(self.context, self.stats, reader_type="table"),
            QualityStage(self.context, self.stats)
        ]