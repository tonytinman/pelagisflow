"""
Privacy Stage - Pipeline integration for data privacy and column masking.

This is a thin adapter that integrates the privacy/masking domain
module into the pipeline execution flow.
"""

from typing import Optional
from pyspark.sql import DataFrame
from nova_framework.pipeline.stages.base import AbstractStage
from nova_framework.core.context import ExecutionContext
from nova_framework.observability.stats import PipelineStats

# Import from access module (domain logic)
from nova_framework.access import PrivacyEngine


class PrivacyStage(AbstractStage):
    """
    Pipeline stage for role-based column masking enforcement.

    This stage orchestrates privacy enforcement by:
    1. Extracting privacy metadata from data contract
    2. Determining exempt AD groups (via role mappings)
    3. Querying current masking state from UC
    4. Calculating deltas (CREATE/DROP masking)
    5. Executing changes
    6. Logging results

    The actual business logic is in the access module - this stage
    is just an adapter for pipeline integration.

    Design Pattern: Facade/Adapter
    - Provides simple interface to complex privacy subsystem
    - Adapts privacy engine to AbstractStage interface

    Usage:
        In IngestionPipeline.build_stages():

        stages = [
            ReadStage(...),
            LineageStage(...),
            ...
            WriteStage(...),
            AccessControlStage(...)  # Table-level access
        ]

        # Add privacy/masking if enabled
        if self._privacy_enabled():
            stages.append(PrivacyStage(
                self.context,
                self.stats,
                environment="dev"
            ))

        return stages

    Key Features:
    - Role-based masking: Groups with table access see raw data
    - Automatic exemption calculation from role mappings
    - Differential enforcement: Only changes what's needed
    - Integrates with existing access control system
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
        Initialize privacy stage.

        Args:
            context: Pipeline execution context
            stats: Pipeline statistics tracker
            environment: Environment (dev, test, prod)
            registry_path: Contract registry path (auto-detected if None)
            dry_run: If True, preview changes without executing
        """
        super().__init__(context, stats, "Privacy")
        self.environment = environment
        self.dry_run = dry_run

        # Auto-detect registry path from context
        if registry_path is None:
            catalog = context.contract.catalog
            registry_path = f"/Volumes/{catalog}/contract_registry"

        # Initialize privacy engine (domain logic)
        self.privacy_engine = PrivacyEngine(
            spark=context.spark,
            registry_path=registry_path,
            environment=environment,
            dry_run=dry_run
        )

    def execute(self, df: Optional[DataFrame] = None) -> Optional[DataFrame]:
        """
        Execute privacy enforcement.

        This stage doesn't transform the DataFrame - it applies
        column masking policies to the target table in UC.

        Args:
            df: Input DataFrame (passed through unchanged)

        Returns:
            Same DataFrame (privacy operates on UC metadata)

        Side Effects:
            - Creates/updates column masking policies in Unity Catalog
            - Logs enforcement results to pipeline stats
        """
        self.logger.info("Starting privacy enforcement")

        # Extract table info from context
        catalog = self.context.contract.catalog
        schema = self.context.contract.schema_name
        table = self.context.contract.table_name

        self.logger.info(
            f"Enforcing privacy for {catalog}.{schema}.{table}"
        )

        # Execute privacy enforcement
        result = self.privacy_engine.enforce_privacy(
            contract=self.context.contract,
            catalog=catalog,
            schema=schema,
            table=table
        )

        # Log results
        self.logger.info(
            f"Privacy enforcement completed: "
            f"Created={result.policies_created}, "
            f"Dropped={result.policies_dropped}, "
            f"Failed={result.policies_failed}, "
            f"Success={result.success_rate:.1f}%"
        )

        # Log detailed results
        self.logger.info(
            f"Columns with privacy: {result.columns_with_privacy}"
        )
        self.logger.info(
            f"Masking intents: {result.masking_intents}"
        )
        self.logger.info(
            f"Current masked columns: {result.current_masked_columns}"
        )
        self.logger.info(
            f"No change needed: {result.no_change_count}"
        )

        # Log errors if any
        if result.errors:
            self.logger.error(f"Privacy enforcement errors:")
            for error in result.errors:
                self.logger.error(f"  - {error}")

        # Update pipeline stats
        self.stats.record_metric(
            stage="Privacy",
            metric="policies_created",
            value=result.policies_created
        )
        self.stats.record_metric(
            stage="Privacy",
            metric="policies_dropped",
            value=result.policies_dropped
        )
        self.stats.record_metric(
            stage="Privacy",
            metric="policies_failed",
            value=result.policies_failed
        )
        self.stats.record_metric(
            stage="Privacy",
            metric="execution_time_seconds",
            value=result.execution_time_seconds
        )

        # Fail pipeline if privacy enforcement failed (configurable)
        if not result.is_successful and not self._allow_partial_failure():
            raise RuntimeError(
                f"Privacy enforcement failed for {catalog}.{schema}.{table}: "
                f"{', '.join(result.errors)}"
            )

        # Pass through DataFrame unchanged
        return df

    def _allow_partial_failure(self) -> bool:
        """
        Check if partial privacy failures are allowed.

        Can be configured via contract customProperties or pipeline config.

        Returns:
            True if pipeline should continue despite privacy failures
        """
        # Check contract for configuration
        allow_partial = self.context.contract.get(
            "customProperties.privacy.allowPartialFailure",
            False
        )

        return allow_partial
