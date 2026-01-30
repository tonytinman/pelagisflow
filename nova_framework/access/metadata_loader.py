"""
Access metadata loader.

Loads access control metadata from the contract registry and calculates
intended privileges for tables.
"""

import yaml
from pathlib import Path
from typing import List, Dict, Optional
from .models import PrivilegeIntent, UCPrivilege


class AccessMetadataLoader:
    """
    Load access control metadata from contract registry.
    
    Registry structure:
        /Volumes/{catalog}/contract_registry/
        ├── access/
        │   ├── global_roles.yaml
        │   └── domains/{domain}/
        │       ├── domain.roles.yaml
        │       └── domain.mappings.{env}.yaml
    
    Usage:
        loader = AccessMetadataLoader(
            registry_path="/Volumes/catalog/contract_registry",
            environment="dev"
        )
        
        intents = loader.get_intended_privileges(
            catalog="catalog",
            schema="finance",
            table="payment_table1"
        )
    """
    
    def __init__(
        self,
        registry_path: str,
        environment: str,
        cache_enabled: bool = True
    ):
        """
        Initialize metadata loader.
        
        Args:
            registry_path: Path to contract registry root
            environment: Environment (dev, test, prod)
            cache_enabled: Enable caching (useful for batch processing)
        """
        self.registry_path = Path(registry_path)
        self.access_path = self.registry_path / "access"
        self.environment = environment
        self.cache_enabled = cache_enabled
        
        # Caches
        self._global_roles_cache: Optional[Dict] = None
        self._domain_metadata_cache: Dict[str, Dict] = {}
    
    def get_intended_privileges(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> List[PrivilegeIntent]:
        """
        Get intended privileges for a specific table.
        
        This is the main entry point. Returns what SHOULD exist based
        on metadata.
        
        Args:
            catalog: UC catalog
            schema: UC schema (= domain name)
            table: Table name
            
        Returns:
            List of PrivilegeIntent (desired state)
            
        Raises:
            FileNotFoundError: If metadata files not found
            yaml.YAMLError: If YAML is malformed
        """
        domain = schema
        qualified_table = f"{catalog}.{schema}.{table}"
        
        # Load domain metadata
        try:
            domain_metadata = self._load_domain_metadata(domain)
        except FileNotFoundError:
            # No access metadata for this domain
            return []
        
        # Build intended privileges for this table
        intents = []
        
        roles = domain_metadata["roles"]
        mappings = domain_metadata["mappings"]
        global_roles = domain_metadata["global_roles"]
        
        # Process each role
        for role_name, role_def in roles.items():
            # Check if this table is in scope for this role
            if not self._table_in_scope(table, role_def.get("scope", {})):
                continue
            
            # Get AD groups mapped to this role
            role_mapping = mappings.get(role_name, {})
            ad_groups = role_mapping.get("ad_groups", [])
            
            if not ad_groups:
                continue
            
            # Get privileges from global role
            inherits = role_def.get("inherits")
            if not inherits or inherits not in global_roles:
                continue
            
            global_role = global_roles[inherits]
            privileges = global_role.get("privileges", [])
            
            # Create PrivilegeIntent for each (group, privilege) combination
            for ad_group in ad_groups:
                for privilege in privileges:
                    intents.append(PrivilegeIntent(
                        table=qualified_table,
                        ad_group=ad_group,
                        privilege=UCPrivilege(privilege),
                        reason=f"Role: {role_name} (inherits {inherits})",
                        role_name=role_name,
                    ))
        
        return intents
    
    def _load_domain_metadata(self, domain: str) -> Dict:
        """
        Load complete metadata for a domain.
        
        Args:
            domain: Domain name
            
        Returns:
            Dict with keys: domain, roles, mappings, global_roles
            
        Raises:
            FileNotFoundError: If metadata files not found
        """
        cache_key = f"{domain}_{self.environment}"
        
        # Check cache
        if self.cache_enabled and cache_key in self._domain_metadata_cache:
            return self._domain_metadata_cache[cache_key]
        
        domain_path = self.access_path / "domains" / domain
        
        # Load domain roles
        roles_file = domain_path / "domain.roles.yaml"
        if not roles_file.exists():
            raise FileNotFoundError(f"Domain roles not found: {roles_file}")
        
        with open(roles_file) as f:
            roles_data = yaml.safe_load(f)
        
        # Load domain mappings for environment
        mappings_file = domain_path / f"domain.mappings.{self.environment}.yaml"
        if not mappings_file.exists():
            raise FileNotFoundError(f"Domain mappings not found: {mappings_file}")
        
        with open(mappings_file) as f:
            mappings_data = yaml.safe_load(f)
        
        # Load global roles (cached)
        if self._global_roles_cache is None:
            global_roles_file = self.access_path / "global_roles.yaml"
            if not global_roles_file.exists():
                raise FileNotFoundError(f"Global roles not found: {global_roles_file}")
            
            with open(global_roles_file) as f:
                self._global_roles_cache = yaml.safe_load(f)
        
        # Build metadata dict
        metadata = {
            "domain": domain,
            "roles": roles_data.get("roles", {}),
            "mappings": mappings_data.get("mappings", {}),
            "global_roles": self._global_roles_cache.get("roles", {})
        }
        
        # Cache if enabled
        if self.cache_enabled:
            self._domain_metadata_cache[cache_key] = metadata
        
        return metadata
    
    def _table_in_scope(self, table: str, scope: Dict) -> bool:
        """
        Check if table is in scope for a role.
        
        Scope can use either include list (whitelist) or exclude list (blacklist).
        
        Args:
            table: Table name to check
            scope: Scope definition from role
                   Examples:
                   {"include": ["*"]}
                   {"include": ["table1", "table2"]}
                   {"exclude": ["sensitive_table"]}
            
        Returns:
            True if table is in scope
        """
        if not scope:
            return False
        
        # Include list (whitelist)
        if "include" in scope:
            include_list = scope["include"]
            # Wildcard matches all
            if "*" in include_list:
                return True
            # Explicit match
            return table in include_list
        
        # Exclude list (blacklist)
        elif "exclude" in scope:
            exclude_list = scope["exclude"]
            # Not in exclude list = in scope
            return table not in exclude_list
        
        return False
    
    def clear_cache(self):
        """Clear all caches."""
        self._global_roles_cache = None
        self._domain_metadata_cache.clear()