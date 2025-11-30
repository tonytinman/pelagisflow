"""
Access Control Stage - Pipeline integration for access control.

This is a thin adapter that integrates the access control domain
module into the pipeline execution flow.
"""

from typing import Optional
from pyspark.sql import DataFrame
from nova_framework.pipeline.stages.base import AbstractStage
from nova_framework.core.context import ExecutionContext
from nova_framework.observability.stats import PipelineStats

# Import from access module (domain logic)
from nova_framework.access import (
    AccessMetadataLoader,
    UCPrivilegeInspector,
    PrivilegeDeltaGenerator,
    GrantRevoker
)


class AccessControlStage(AbstractStage):
    """
    Pipeline stage for table-level differential access control.
    
    This stage orchestrates access control enforcement by:
    1. Loading intended privileges from metadata
    2. Querying actual privileges from UC
    3. Calculating deltas (GRANTs + REVOKEs)
    4. Executing changes
    5. Logging results
    
    The actual business logic is in the access module - this stage
    is just an adapter for pipeline integration.
    
    Design Pattern: Facade/Adapter
    - Provides simple interface to complex access control subsystem
    - Adapts access module to AbstractStage interface
    
    Usage:
        In IngestionPipeline.build_stages():
        
        stages = [
            ReadStage(...),
            LineageStage(...),
            ...
            WriteStage(...)
        ]
        
        # Add access control if enabled
        if self._access_control_enabled():
            stages.append(AccessControlStage(
                self.context,
                self.stats,
                environment="dev"
            ))
        
        return stages
    """
    
    def __init__(
        self,
        context: ExecutionContext,
        stats: PipelineStats,
        environment: str,
        registry_path: Optional[str] = None,
        dry_run: bool = False
    ):
        """
        Initialize access control stage.
        
        Args:
            context: Pipeline execution context
            stats: Pipeline statistics tracker
            environment: Environment (dev, test, prod)
            registry_path: Contract registry path (auto-detected if None)
            dry_run: If True, preview changes without executing
        """
        super().__init__(context, stats, "AccessControl")
        self.environment = environment
        self.dry_run = dry_run
        
        # Auto-detect registry path from context
        if registry_path is None:
            catalog = context.contract.catalog
            registry_path = f"/Volumes/{catalog}/contract_registry"
        
        # Initialize access control components (domain logic)
        self.metadata_loader = AccessMetadataLoader(
            registry_path=registry_path,
            environment=environment
        )
        self.uc_inspector = UCPrivilegeInspector(
            spark=context.spark
        )
        self.delta_generator = PrivilegeDeltaGenerator()
        self.grant_revoker = GrantRevoker(
            spark=context.spark,
            dry_run=dry_run
        )
    
    def execute(self, df: DataFrame) -> DataFrame:
        """
        Execute access control enforcement.
        
        This method orchestrates the access control workflow using
        components from the access module.
        
        Args:
            df: Input DataFrame (passed through unchanged)
            
        Returns:
            Same DataFrame (access control is a side effect)
        """
        # Get table details from context
        catalog = self.context.contract.catalog
        schema = self.context.contract.schema_name
        table = self.context.contract.table_name
        qualified_table = f"{catalog}.{schema}.{table}"
        
        self.logger.info(f"Processing access control for {qualified_table}")
        
        # Step 1: Load intended privileges (from metadata)
        try:
            intended = self.metadata_loader.get_intended_privileges(
                catalog, schema, table
            )
        except Exception as e:
            self.logger.warning(
                f"Failed to load access metadata: {e}. Skipping access control."
            )
            return df
        
        if not intended:
            self.logger.info("No access rules defined - skipping")
            return df
        
        self.logger.info(f"Intended privileges: {len(intended)}")
        
        # Step 2: Query actual privileges (from UC)
        try:
            actual = self.uc_inspector.get_actual_privileges(
                catalog, schema, table
            )
        except Exception as e:
            self.logger.error(f"Failed to query UC privileges: {e}")
            return df
        
        self.logger.info(f"Actual privileges: {len(actual)}")
        
        # Step 3: Calculate deltas (differential analysis)
        deltas, no_change_count = self.delta_generator.generate_deltas(
            intended, actual
        )
        
        grants, revokes = self.delta_generator.group_by_action(deltas)
        
        self.logger.info(
            f"Changes: {len(grants)} GRANTs, {len(revokes)} REVOKEs, "
            f"{no_change_count} correct"
        )
        
        # Step 4: Execute changes (if any)
        if deltas:
            if self.dry_run:
                self.logger.info("DRY RUN MODE - Changes not executed")
            
            result = self.grant_revoker.apply_deltas(
                table=qualified_table,
                deltas=deltas,
                intended_count=len(intended),
                actual_count=len(actual),
                no_change_count=no_change_count
            )
            
            # Step 5: Log results and record stats
            self.logger.info(
                f"Result: GRANTs {result.grants_succeeded}/{result.grants_attempted}, "
                f"REVOKEs {result.revokes_succeeded}/{result.revokes_attempted}, "
                f"Success: {result.success_rate:.1f}%"
            )
            
            if result.errors:
                self.logger.warning(f"Encountered {len(result.errors)} errors")
                for error in result.errors:
                    self.logger.warning(f"  {error}")
            
            # Record detailed statistics
            self._record_stats(result)
        else:
            self.logger.info("No changes needed - already correct")
            self.stats.log_stat("access_no_change_count", no_change_count)
        
        # Pass through DataFrame unchanged
        # Access control is a side effect, doesn't modify data
        return df
    
    def _record_stats(self, result):
        """
        Record access control statistics to pipeline stats.
        
        These stats can be queried from pipeline_stats table for
        observability and monitoring.
        """
        self.stats.log_stat("access_intended_count", result.intended_count)
        self.stats.log_stat("access_actual_count", result.actual_count)
        self.stats.log_stat("access_no_change_count", result.no_change_count)
        self.stats.log_stat("access_grants_attempted", result.grants_attempted)
        self.stats.log_stat("access_grants_succeeded", result.grants_succeeded)
        self.stats.log_stat("access_grants_failed", result.grants_failed)
        self.stats.log_stat("access_revokes_attempted", result.revokes_attempted)
        self.stats.log_stat("access_revokes_succeeded", result.revokes_succeeded)
        self.stats.log_stat("access_revokes_failed", result.revokes_failed)
        self.stats.log_stat("access_execution_seconds", result.execution_time_seconds)
    
    def skip_condition(self) -> bool:
        """
        Determine if access control should be skipped.
        
        Override from AbstractStage to support conditional execution.
        
        Returns:
            True if access control is explicitly disabled in contract
        """
        # Check if access control is explicitly disabled
        # Default to enabled (True) if not specified
        return self.context.contract.get("accessControl.enabled", True) == False