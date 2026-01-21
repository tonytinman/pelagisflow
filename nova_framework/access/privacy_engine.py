"""
Data Privacy enforcement engine.

Inspects Unity Catalog masking state, compares with data contract requirements
and role-based access, and applies necessary masking policies.
"""

import time
from typing import List, Optional
from pyspark.sql import SparkSession

from .privacy_models import (
    ColumnPrivacyMetadata,
    PrivacyClassification,
    MaskingStrategy,
    MaskingIntent,
    MaskingDelta,
    UCMaskingPolicy,
    PrivacyEnforcementResult
)
from .uc_masking_inspector import UCMaskingInspector
from .privacy_metadata_loader import PrivacyMetadataLoader
from .masking_functions import MaskingFunctions
from nova_framework.contract.contract import DataContract


class PrivacyEngine:
    """
    Main engine for role-based data privacy and column masking enforcement.

    Workflow:
        1. Load privacy metadata from data contract
        2. Determine exempt AD groups (via role mappings)
        3. Calculate intended masking state (MaskingIntent)
        4. Query actual masking state from UC (UCMaskingPolicy)
        5. Calculate delta (MaskingDelta)
        6. Apply changes to UC (CREATE/DROP masking)
        7. Return results

    Key Feature: Role-based masking
    - Groups with table access see unmasked data
    - Groups without access see masked data (hash/redact/etc)
    - Uses Unity Catalog's is_account_group_member() function

    Usage:
        engine = PrivacyEngine(
            spark=spark,
            registry_path="/Volumes/catalog/contract_registry",
            environment="dev"
        )

        result = engine.enforce_privacy(
            contract=contract,
            catalog="cluk_dev_nova",
            dry_run=False
        )

        print(f"Created {result.policies_created} masking policies")
    """

    def __init__(
        self,
        spark: SparkSession,
        registry_path: str,
        environment: str,
        dry_run: bool = False
    ):
        """
        Initialize privacy engine.

        Args:
            spark: Active Spark session with UC access
            registry_path: Path to contract registry
            environment: Environment (dev, test, prod)
            dry_run: If True, calculate changes but don't apply them
        """
        self.spark = spark
        self.registry_path = registry_path
        self.environment = environment
        self.dry_run = dry_run

        # Initialize components
        self.inspector = UCMaskingInspector(spark)
        self.metadata_loader = PrivacyMetadataLoader(
            registry_path=registry_path,
            environment=environment
        )
        self.masking_functions = MaskingFunctions()

    def enforce_privacy(
        self,
        contract: DataContract,
        catalog: str,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ) -> PrivacyEnforcementResult:
        """
        Enforce privacy/masking policies for a data asset.

        This is the main entry point. Performs differential analysis
        and applies only necessary changes.

        Args:
            contract: Data contract with privacy metadata
            catalog: UC catalog name
            schema: UC schema (if None, extracted from contract)
            table: UC table (if None, extracted from contract)

        Returns:
            PrivacyEnforcementResult with enforcement summary

        Example:
            contract = DataContract("data.galahad.gallive_manager_address", "dev")
            result = engine.enforce_privacy(contract, "cluk_dev_nova")

            # Result shows:
            # - Columns with privacy: 7 (PII columns)
            # - Masking policies created: 7
            # - Exempt groups: {"CLUK-CAZ-EDP-dev-finance-nova-data-engineer"}
        """
        start_time = time.time()
        errors = []

        # Extract table identifiers from contract
        if schema is None:
            schema = contract.get("schema.name") or contract.schema_name
        if table is None:
            table = contract.get("schema.table") or contract.table_name

        qualified_table = f"{catalog}.{schema}.{table}"

        try:
            # Step 1: Extract privacy metadata from contract
            privacy_metadata = self._extract_privacy_metadata(contract)

            if not privacy_metadata:
                # No columns with privacy classifications
                return PrivacyEnforcementResult(
                    table=qualified_table,
                    columns_with_privacy=0,
                    masking_intents=0,
                    current_masked_columns=0,
                    no_change_count=0,
                    policies_created=0,
                    policies_dropped=0,
                    policies_failed=0,
                    execution_time_seconds=time.time() - start_time,
                    errors=[]
                )

            # Step 2: Calculate intended masking state (with role-based exemptions)
            masking_intents = self.metadata_loader.get_masking_intents_for_table(
                catalog=catalog,
                schema=schema,
                table=table,
                privacy_metadata=privacy_metadata
            )

            # Step 3: Get actual masking state from UC
            current_masks = self.inspector.get_column_masks(
                catalog, schema, table
            )

            # Step 4: Calculate delta (what needs to change)
            deltas = self._calculate_masking_deltas(
                masking_intents,
                current_masks
            )

            # Step 5: Apply changes (unless dry_run)
            policies_created = 0
            policies_dropped = 0
            policies_failed = 0

            if not self.dry_run:
                for delta in deltas:
                    try:
                        self._apply_masking_delta(delta)
                        if delta.action == "CREATE":
                            policies_created += 1
                        else:
                            policies_dropped += 1
                    except Exception as e:
                        policies_failed += 1
                        errors.append(
                            f"Failed to {delta.action} mask on "
                            f"{delta.column_name}: {str(e)}"
                        )

            # Calculate result metrics
            columns_with_privacy = len([
                m for m in privacy_metadata
                if m.privacy != PrivacyClassification.NONE
            ])

            no_change_count = len(masking_intents) - len(deltas)

            execution_time = time.time() - start_time

            return PrivacyEnforcementResult(
                table=qualified_table,
                columns_with_privacy=columns_with_privacy,
                masking_intents=len(masking_intents),
                current_masked_columns=len(current_masks),
                no_change_count=no_change_count,
                policies_created=policies_created,
                policies_dropped=policies_dropped,
                policies_failed=policies_failed,
                execution_time_seconds=execution_time,
                errors=errors
            )

        except Exception as e:
            execution_time = time.time() - start_time
            errors.append(f"Privacy enforcement failed: {str(e)}")

            return PrivacyEnforcementResult(
                table=qualified_table,
                columns_with_privacy=0,
                masking_intents=0,
                current_masked_columns=0,
                no_change_count=0,
                policies_created=0,
                policies_dropped=0,
                policies_failed=0,
                execution_time_seconds=execution_time,
                errors=errors
            )

    def _extract_privacy_metadata(
        self,
        contract: DataContract
    ) -> List[ColumnPrivacyMetadata]:
        """
        Extract privacy metadata from data contract.

        Reads schema.properties[] and extracts privacy/masking info
        for each column.

        Args:
            contract: Data contract

        Returns:
            List of ColumnPrivacyMetadata
        """
        metadata = []

        properties = contract.get("schema.properties", [])

        for prop in properties:
            column_name = prop.get("name")
            data_type = prop.get("type", "string")
            privacy = prop.get("privacy", "none")
            masking_strategy = prop.get("maskingStrategy", "none")
            description = prop.get("description", "")
            is_primary_key = prop.get("isPrimaryKey", False)

            metadata.append(ColumnPrivacyMetadata(
                column_name=column_name,
                data_type=data_type,
                privacy=PrivacyClassification(privacy),
                masking_strategy=MaskingStrategy(masking_strategy),
                description=description,
                is_primary_key=is_primary_key
            ))

        return metadata

    def _calculate_masking_deltas(
        self,
        intents: List[MaskingIntent],
        current_masks: List[UCMaskingPolicy]
    ) -> List[MaskingDelta]:
        """
        Calculate delta between intended and actual masking state.

        Compares what should exist (intents) with what does exist
        (current_masks) to determine what changes are needed.

        Args:
            intents: Intended masking state (with role-based exemptions)
            current_masks: Current masking state in UC

        Returns:
            List of MaskingDelta (changes needed)
        """
        deltas = []

        # Create lookup maps
        intent_map = {
            (intent.table, intent.column_name): intent
            for intent in intents
        }

        current_map = {
            (mask.table, mask.column_name): mask
            for mask in current_masks
        }

        # Find columns that need masking added/updated
        for key, intent in intent_map.items():
            current = current_map.get(key)

            # Generate expected masking expression
            expected_expr = self.masking_functions.get_masking_expression(
                strategy=intent.masking_strategy.value,
                column_name=intent.column_name,
                column_type=intent.column_type,
                exempt_groups=intent.exempt_groups
            )

            if current is None:
                # Need to CREATE masking
                deltas.append(MaskingDelta(
                    action="CREATE",
                    table=intent.table,
                    column_name=intent.column_name,
                    column_type=intent.column_type,
                    masking_strategy=intent.masking_strategy,
                    exempt_groups=intent.exempt_groups.copy(),
                    current_masking=None,
                    reason=intent.reason
                ))
            else:
                # Check if masking expression needs updating
                # Normalize whitespace for comparison
                current_normalized = self._normalize_sql(current.masking_expression or "")
                expected_normalized = self._normalize_sql(expected_expr)

                if current_normalized != expected_normalized:
                    # Drop old masking
                    deltas.append(MaskingDelta(
                        action="DROP",
                        table=intent.table,
                        column_name=intent.column_name,
                        column_type=intent.column_type,
                        masking_strategy=None,
                        exempt_groups=set(),
                        current_masking=current.masking_expression,
                        reason="Masking expression needs update"
                    ))
                    # Create new masking
                    deltas.append(MaskingDelta(
                        action="CREATE",
                        table=intent.table,
                        column_name=intent.column_name,
                        column_type=intent.column_type,
                        masking_strategy=intent.masking_strategy,
                        exempt_groups=intent.exempt_groups.copy(),
                        current_masking=None,
                        reason=intent.reason
                    ))

        # Find columns that need masking removed (in current but not intent)
        for key, current in current_map.items():
            if key not in intent_map:
                # Need to DROP masking
                deltas.append(MaskingDelta(
                    action="DROP",
                    table=current.table,
                    column_name=current.column_name,
                    column_type="unknown",
                    masking_strategy=None,
                    exempt_groups=set(),
                    current_masking=current.masking_expression,
                    reason="Column no longer requires masking per contract"
                ))

        return deltas

    def _normalize_sql(self, sql: str) -> str:
        """
        Normalize SQL expression for comparison.

        Removes extra whitespace, newlines, etc.

        Args:
            sql: SQL expression

        Returns:
            Normalized SQL
        """
        import re
        # Remove extra whitespace and newlines
        normalized = re.sub(r'\s+', ' ', sql.strip())
        return normalized

    def _apply_masking_delta(self, delta: MaskingDelta) -> None:
        """
        Apply a single masking delta to Unity Catalog.

        Executes ALTER TABLE statement to create or drop column masking.

        Args:
            delta: Masking delta to apply

        Raises:
            Exception: If SQL execution fails

        Example SQL:
            CREATE:
            ALTER TABLE catalog.schema.table
            ALTER COLUMN email
            SET MASK CASE
                WHEN is_account_group_member('engineers') THEN email
                ELSE sha2(email, 256)
            END

            DROP:
            ALTER TABLE catalog.schema.table
            ALTER COLUMN email
            DROP MASK
        """
        if delta.action == "CREATE":
            # Generate masking expression with role-based exemptions
            masking_expr = self.masking_functions.get_masking_expression(
                strategy=delta.masking_strategy.value,
                column_name=delta.column_name,
                column_type=delta.column_type,
                exempt_groups=delta.exempt_groups
            )

            # Apply column mask using ALTER TABLE
            sql = f"""
                ALTER TABLE {delta.table}
                ALTER COLUMN {delta.column_name}
                SET MASK {masking_expr}
            """

            self.spark.sql(sql)

        elif delta.action == "DROP":
            # Drop column mask
            sql = f"""
                ALTER TABLE {delta.table}
                ALTER COLUMN {delta.column_name}
                DROP MASK
            """

            self.spark.sql(sql)

    def preview_changes(
        self,
        contract: DataContract,
        catalog: str,
        schema: Optional[str] = None,
        table: Optional[str] = None
    ) -> List[MaskingDelta]:
        """
        Preview masking changes without applying them.

        Useful for dry-run or reviewing changes before enforcement.

        Args:
            contract: Data contract
            catalog: UC catalog
            schema: UC schema (optional)
            table: UC table (optional)

        Returns:
            List of MaskingDelta that would be applied
        """
        # Extract table identifiers
        if schema is None:
            schema = contract.get("schema.name") or contract.schema_name
        if table is None:
            table = contract.get("schema.table") or contract.table_name

        # Get privacy metadata and intents
        privacy_metadata = self._extract_privacy_metadata(contract)

        masking_intents = self.metadata_loader.get_masking_intents_for_table(
            catalog=catalog,
            schema=schema,
            table=table,
            privacy_metadata=privacy_metadata
        )

        # Get current state
        current_masks = self.inspector.get_column_masks(
            catalog, schema, table
        )

        # Calculate and return deltas
        return self._calculate_masking_deltas(masking_intents, current_masks)
