"""
Pipeline orchestrator - main entry point for all pipeline executions.

This is the top-level class that coordinates pipeline execution.
"""

from pelagisflow.pipeline.factory import PipelineFactory
from pelagisflow.core.context import ExecutionContext
from pelagisflow.observability.stats import PipelineStats
from pelagisflow.observability.telemetry import TelemetryEmitter
from pelagisflow.observability.logging import get_logger

logger = get_logger("pipeline.orchestrator")


class Pipeline:
    """
    Main pipeline orchestrator.
    
    Entry point for all pipeline executions. Coordinates:
    - Context creation
    - Stats tracking
    - Pipeline selection
    - Execution
    - Error handling
    
    This is the class that external systems call to run pipelines.
    """
    
    def run(
        self,
        process_queue_id: int,
        data_contract_name: str,
        source_ref: str,
        env: str
    ) -> str:
        """
        Execute pipeline for given contract.
        
        Args:
            process_queue_id: Process queue ID for tracking
            data_contract_name: Name of data contract to execute
            source_ref: Source reference (date, file name, etc.)
            env: Environment (dev, test, prod)
            
        Returns:
            "SUCCESS" or "FAILED"
            
        Example:
            pipeline = Pipeline()
            result = pipeline.run(
                process_queue_id=1,
                data_contract_name="customer_data",
                source_ref="2024-11-28",
                env="dev"
            )
            
            if result == "SUCCESS":
                print("Pipeline completed successfully")
        """
        origin = f"Pipeline.run({process_queue_id},{data_contract_name},{source_ref},{env})"
        
        TelemetryEmitter.emit(origin, "Pipeline [Start]", process_queue_id)
        logger.info(f"Starting pipeline: {data_contract_name}")
        
        result = "FAILED"
        
        try:
            # Create execution context
            logger.info("Creating execution context")
            context = ExecutionContext(
                process_queue_id=process_queue_id,
                data_contract_name=data_contract_name,
                source_ref=source_ref,
                env=env
            )
            
            # Create stats tracker
            stats = PipelineStats(process_queue_id=process_queue_id)
            
            # Create pipeline from factory
            logger.info(f"Creating pipeline for type: {context.contract.pipeline_type}")
            pipeline = PipelineFactory.create(context, stats)
            
            # Execute pipeline
            logger.info("Executing pipeline")
            result = pipeline.execute()
            
            # Finalize stats
            logger.info("Finalizing statistics")
            stats.finalize()
            
            logger.info(f"Pipeline completed with result: {result}")
            
        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
            TelemetryEmitter.emit(origin, f"ERROR: {str(e)}", process_queue_id)
            raise
        
        finally:
            TelemetryEmitter.emit(origin, f"Pipeline [End][{result}]", process_queue_id)
        
        return result