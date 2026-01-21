"""
Privacy metadata loader.

Loads privacy metadata from data contracts and determines which AD groups
should have access to unmasked (sensitive) data based on role mappings.
"""

from typing import List, Dict, Set, Optional
from pathlib import Path
from .metadata_loader import AccessMetadataLoader
from .privacy_models import MaskingIntent, ColumnPrivacyMetadata
from .models import UCPrivilege


class PrivacyMetadataLoader:
    """
    Load privacy metadata and determine group-based masking exemptions.

    This loader integrates with the existing access control metadata system
    to determine which AD groups should see unmasked sensitive data.

    Logic:
    - Groups with table-level access (SELECT, MODIFY, ALL PRIVILEGES) that are
      scoped to include the table should see unmasked data
    - Groups without access or explicitly excluded from table scope see masked data

    Usage:
        loader = PrivacyMetadataLoader(
            registry_path="/Volumes/catalog/contract_registry",
            environment="dev"
        )

        exempt_groups = loader.get_exempt_groups(
            catalog="cluk_dev_nova",
            schema="bronze_galahad",
            table="gallive_manager_address"
        )

        # Returns: {"CLUK-CAZ-EDP-dev-finance-nova-data-engineer", ...}
    """

    def __init__(
        self,
        registry_path: str,
        environment: str,
        cache_enabled: bool = True
    ):
        """
        Initialize privacy metadata loader.

        Args:
            registry_path: Path to contract registry root
            environment: Environment (dev, test, prod)
            cache_enabled: Enable caching (useful for batch processing)
        """
        self.registry_path = Path(registry_path)
        self.environment = environment
        self.cache_enabled = cache_enabled

        # Reuse AccessMetadataLoader for role/mapping infrastructure
        self.access_loader = AccessMetadataLoader(
            registry_path=str(registry_path),
            environment=environment,
            cache_enabled=cache_enabled
        )

    def get_exempt_groups(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> Set[str]:
        """
        Get AD groups that should see unmasked data for a table.

        These are groups that have table-level access (via role mappings)
        and are in scope for the table.

        Args:
            catalog: UC catalog
            schema: UC schema (= domain name)
            table: Table name

        Returns:
            Set of AD group names that see unmasked data

        Example:
            For bronze_galahad.gallive_manager_address:

            Domain roles:
            - engineer.general: includes gallive_manager_address
              -> mapped to: CLUK-CAZ-EDP-dev-finance-nova-data-engineer
            - analyst.general: excludes gallive_manager_address
              -> mapped to: CLUK-CAZ-EDP-dev-data-admin

            Result: {"CLUK-CAZ-EDP-dev-finance-nova-data-engineer"}
            (Only engineer.general is in scope)
        """
        # Get intended privileges for this table from access control system
        # This already applies scope rules (include/exclude)
        intended_privileges = self.access_loader.get_intended_privileges(
            catalog=catalog,
            schema=schema,
            table=table
        )

        # Extract unique AD groups that have any table-level privilege
        # These groups should see unmasked data
        exempt_groups = set()
        for intent in intended_privileges:
            # Groups with SELECT, MODIFY, or ALL PRIVILEGES can see raw data
            if intent.privilege in (
                UCPrivilege.SELECT,
                UCPrivilege.MODIFY,
                UCPrivilege.ALL_PRIVILEGES
            ):
                exempt_groups.add(intent.ad_group)

        return exempt_groups

    def get_masking_intents_for_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        privacy_metadata: List[ColumnPrivacyMetadata]
    ) -> List[MaskingIntent]:
        """
        Get masking intents for a table's sensitive columns.

        Combines column privacy metadata with role-based access to generate
        complete masking intents.

        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name
            privacy_metadata: List of column privacy metadata from contract

        Returns:
            List of MaskingIntent (what masking SHOULD exist)

        Example:
            For table with PII columns and engineer access:

            privacy_metadata = [
                ColumnPrivacyMetadata(
                    column_name="POST_CODE",
                    privacy=PrivacyClassification.PII,
                    masking_strategy=MaskingStrategy.HASH,
                    ...
                )
            ]

            Result: [
                MaskingIntent(
                    table="catalog.schema.table",
                    column_name="POST_CODE",
                    privacy=PrivacyClassification.PII,
                    masking_strategy=MaskingStrategy.HASH,
                    exempt_groups={"CLUK-CAZ-EDP-dev-finance-nova-data-engineer"},
                    reason="PII data requires masking"
                )
            ]
        """
        qualified_table = f"{catalog}.{schema}.{table}"

        # Get AD groups that should see unmasked data
        exempt_groups = self.get_exempt_groups(catalog, schema, table)

        intents = []

        for column in privacy_metadata:
            # Skip columns without privacy classification
            if not column.requires_masking:
                continue

            # Get effective masking strategy
            strategy = column.effective_masking_strategy

            intent = MaskingIntent(
                table=qualified_table,
                column_name=column.column_name,
                column_type=column.data_type,
                privacy=column.privacy,
                masking_strategy=strategy,
                exempt_groups=exempt_groups.copy(),
                reason=f"{column.privacy.value} data requires {strategy.value} masking"
            )

            intents.append(intent)

        return intents

    def get_domain_metadata(self, domain: str) -> Optional[Dict]:
        """
        Get complete domain metadata (roles + mappings).

        Delegates to AccessMetadataLoader.

        Args:
            domain: Domain name

        Returns:
            Dict with keys: domain, roles, mappings, global_roles
            None if metadata doesn't exist
        """
        try:
            return self.access_loader._load_domain_metadata(domain)
        except FileNotFoundError:
            return None

    def clear_cache(self):
        """Clear all caches."""
        self.access_loader.clear_cache()
