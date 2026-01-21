"""
Privacy metadata loader with sensitivity-aware access control.

Loads privacy metadata from data contracts and determines which AD groups
should have access to unmasked (sensitive) data based on role mappings
and explicit sensitive_access definitions in domain roles.
"""

from typing import List, Dict, Set, Optional
from pathlib import Path
from .metadata_loader import AccessMetadataLoader
from .privacy_models import (
    MaskingIntent,
    ColumnPrivacyMetadata,
    PrivacyClassification
)
from .models import UCPrivilege


class PrivacyMetadataLoader:
    """
    Load privacy metadata and determine sensitivity-aware masking exemptions.

    This loader integrates with the existing access control metadata system
    and reads the `sensitive_access` field from domain roles to determine
    which AD groups can see which sensitivity levels of data unmasked.

    New Logic (Explicit Sensitivity Access):
    - Domain roles define `sensitive_access: [pii, quasi_pii, special, ...]`
    - Only groups whose role includes a sensitivity level in their sensitive_access
      list can see that level of data unmasked
    - `sensitive_access: [none]` or empty = all data is masked
    - Groups must ALSO have table-level access (scope rules still apply)

    Usage:
        loader = PrivacyMetadataLoader(
            registry_path="/Volumes/catalog/contract_registry",
            environment="dev"
        )

        # Get groups that can see PII data unmasked
        exempt_groups = loader.get_exempt_groups_for_sensitivity(
            catalog="cluk_dev_nova",
            schema="bronze_galahad",
            table="gallive_manager_address",
            sensitivity=PrivacyClassification.PII
        )

        # Returns: {"CLUK-CAZ-EDP-dev-customer-services-finance-advisors", ...}
        # (Only groups with 'pii' in their sensitive_access list)
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

        # Cache for sensitive_access lookups
        self._sensitive_access_cache: Dict[str, Dict[str, List[str]]] = {}

    def get_role_sensitive_access(
        self,
        domain: str,
        role_name: str
    ) -> Set[str]:
        """
        Get the sensitive_access list for a specific domain role.

        Args:
            domain: Domain name (e.g., "bronze_galahad")
            role_name: Role name (e.g., "analyst.general")

        Returns:
            Set of sensitivity levels this role can access unmasked
            Empty set if not defined (= no access to sensitive data)

        Example:
            For domain role:
            analyst.sensitive:
              sensitive_access: [pii, quasi_pii]

            Result: {"pii", "quasi_pii"}
        """
        # Check cache
        cache_key = domain
        if cache_key in self._sensitive_access_cache:
            role_access = self._sensitive_access_cache[cache_key].get(role_name)
            if role_access is not None:
                return set(role_access)

        # Load domain metadata
        domain_metadata = self.get_domain_metadata(domain)
        if not domain_metadata:
            return set()

        # Build cache for this domain
        if cache_key not in self._sensitive_access_cache:
            self._sensitive_access_cache[cache_key] = {}

        # Extract sensitive_access for all roles
        domain_roles = domain_metadata.get("roles", {})
        for role, role_def in domain_roles.items():
            sensitive_access = role_def.get("sensitive_access", [])

            # Handle "none" as special value
            if sensitive_access == ["none"] or sensitive_access == "none":
                sensitive_access = []

            # Normalize to list
            if isinstance(sensitive_access, str):
                sensitive_access = [sensitive_access]

            self._sensitive_access_cache[cache_key][role] = sensitive_access

        # Return for requested role
        role_access = self._sensitive_access_cache[cache_key].get(role_name, [])
        return set(role_access)

    def get_exempt_groups_for_sensitivity(
        self,
        catalog: str,
        schema: str,
        table: str,
        sensitivity: PrivacyClassification
    ) -> Set[str]:
        """
        Get AD groups that can see a specific sensitivity level unmasked.

        Checks:
        1. Group has table-level access (via access control system)
        2. Group's domain role includes this sensitivity in sensitive_access

        Args:
            catalog: UC catalog
            schema: UC schema (= domain name)
            table: Table name
            sensitivity: Privacy classification level

        Returns:
            Set of AD group names that see this sensitivity level unmasked

        Example:
            For bronze_galahad.gallive_manager_address with sensitivity=PII:

            Domain roles:
            - analyst.general: sensitive_access: [quasi_pii]
              -> Cannot see PII unmasked
            - analyst.sensitive: sensitive_access: [pii, quasi_pii]
              -> CAN see PII unmasked
            - engineer.general: sensitive_access: [none]
              -> Cannot see PII unmasked

            Mappings:
            - analyst.sensitive -> CLUK-CAZ-EDP-dev-sensitive-data-analysts

            Result: {"CLUK-CAZ-EDP-dev-sensitive-data-analysts"}
        """
        domain = schema  # Schema name = domain name

        # Skip for non-sensitive data
        if sensitivity == PrivacyClassification.NONE:
            return set()

        # Get intended privileges (groups with table access)
        intended_privileges = self.access_loader.get_intended_privileges(
            catalog=catalog,
            schema=schema,
            table=table
        )

        # Get domain metadata to map AD groups -> roles
        domain_metadata = self.get_domain_metadata(domain)
        if not domain_metadata:
            return set()

        # Build reverse mapping: AD group -> role names
        mappings = domain_metadata.get("mappings", {})
        ad_group_to_roles: Dict[str, Set[str]] = {}

        for role_name, role_mapping in mappings.items():
            ad_groups = role_mapping.get("ad_groups", [])
            for ad_group in ad_groups:
                if ad_group not in ad_group_to_roles:
                    ad_group_to_roles[ad_group] = set()
                ad_group_to_roles[ad_group].add(role_name)

        # Filter groups by sensitivity access
        exempt_groups = set()

        for intent in intended_privileges:
            # Must have table-level privilege
            if intent.privilege not in (
                UCPrivilege.SELECT,
                UCPrivilege.MODIFY,
                UCPrivilege.ALL_PRIVILEGES
            ):
                continue

            ad_group = intent.ad_group

            # Get roles for this AD group
            roles = ad_group_to_roles.get(ad_group, set())

            # Check if ANY role for this group has access to this sensitivity
            has_sensitive_access = False
            for role_name in roles:
                role_sensitive_access = self.get_role_sensitive_access(domain, role_name)

                # Check if this role can access this sensitivity level
                if sensitivity.value in role_sensitive_access:
                    has_sensitive_access = True
                    break

            if has_sensitive_access:
                exempt_groups.add(ad_group)

        return exempt_groups

    def get_exempt_groups(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> Set[str]:
        """
        Get AD groups with table-level access (legacy method).

        This method is kept for backward compatibility but should
        use get_exempt_groups_for_sensitivity() for sensitivity-aware access.

        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name

        Returns:
            Set of AD groups with table access
        """
        intended_privileges = self.access_loader.get_intended_privileges(
            catalog=catalog,
            schema=schema,
            table=table
        )

        exempt_groups = set()
        for intent in intended_privileges:
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

        Combines column privacy metadata with role-based sensitive_access
        to generate complete masking intents.

        Each column's exempt groups are calculated based on its specific
        sensitivity level.

        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name
            privacy_metadata: List of column privacy metadata from contract

        Returns:
            List of MaskingIntent (what masking SHOULD exist)

        Example:
            For table with mixed sensitivity columns:

            privacy_metadata = [
                ColumnPrivacyMetadata(
                    column_name="POSTCODE",
                    privacy=PrivacyClassification.QUASI_PII,
                    ...
                ),
                ColumnPrivacyMetadata(
                    column_name="EMAIL",
                    privacy=PrivacyClassification.PII,
                    ...
                )
            ]

            Domain role analyst.general:
              sensitive_access: [quasi_pii]

            Domain role analyst.sensitive:
              sensitive_access: [pii, quasi_pii]

            Result:
            - POSTCODE: Both analyst.general and analyst.sensitive can see unmasked
            - EMAIL: Only analyst.sensitive can see unmasked
        """
        qualified_table = f"{catalog}.{schema}.{table}"

        intents = []

        for column in privacy_metadata:
            # Skip columns without privacy classification
            if not column.requires_masking:
                continue

            # Get effective masking strategy
            strategy = column.effective_masking_strategy

            # Get exempt groups for THIS sensitivity level
            exempt_groups = self.get_exempt_groups_for_sensitivity(
                catalog=catalog,
                schema=schema,
                table=table,
                sensitivity=column.privacy
            )

            intent = MaskingIntent(
                table=qualified_table,
                column_name=column.column_name,
                column_type=column.data_type,
                privacy=column.privacy,
                masking_strategy=strategy,
                exempt_groups=exempt_groups,
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
        self._sensitive_access_cache.clear()
