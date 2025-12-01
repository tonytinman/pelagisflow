"""
Unity Catalog privilege inspector.

Queries Unity Catalog to determine actual privilege state for tables.
"""

from typing import List
from pyspark.sql import SparkSession
from .models import ActualPrivilege, UCPrivilege


class UCPrivilegeInspector:
    """
    Query Unity Catalog to get actual privilege state.
    
    Uses SHOW GRANTS to determine what privileges currently exist
    for a table.
    
    Usage:
        inspector = UCPrivilegeInspector(spark)
        
        actuals = inspector.get_actual_privileges(
            catalog="catalog",
            schema="finance",
            table="payment_table1"
        )
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize inspector.
        
        Args:
            spark: Active Spark session with UC access
        """
        self.spark = spark
    
    def get_actual_privileges(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> List[ActualPrivilege]:
        """
        Get current privileges for a specific table.
        
        Queries Unity Catalog using SHOW GRANTS and returns the
        current privilege assignments.
        
        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name
            
        Returns:
            List of ActualPrivilege (current state in UC)
            Empty list if table doesn't exist or has no grants
            
        Example:
            actuals = inspector.get_actual_privileges(
                "catalog", "finance", "payment_table1"
            )
            # Returns: [
            #   ActualPrivilege(
            #       table="catalog.finance.payment_table1",
            #       ad_group="ad_grp_payments_dev",
            #       privilege=UCPrivilege.SELECT
            #   )
            # ]
        """
        qualified_table = f"{catalog}.{schema}.{table}"
        
        # Query current grants
        try:
            df = self.spark.sql(f"SHOW GRANTS ON TABLE {qualified_table}")
        except Exception:
            # Table might not exist yet, or no grants exist
            # Return empty list instead of failing
            return []
        
        # Parse results
        privileges = []
        
        for row in df.collect():
            # SHOW GRANTS returns columns:
            # - principal: Group/user name
            # - principal_type: GROUP, USER, or SERVICE_PRINCIPAL
            # - action_type: Privilege (SELECT, MODIFY, ALL PRIVILEGES, etc.)
            # - object_type: TABLE, SCHEMA, CATALOG, etc.
            # - object_key: Fully qualified object name
            
            # Only process GROUP principals
            # (We don't manage USER or SERVICE_PRINCIPAL grants)
            if row.principal_type != "GROUP":
                continue
            
            # Only process table-level privileges we manage
            try:
                privilege = UCPrivilege(row.action_type)
            except ValueError:
                # Not a privilege we manage (e.g., OWNERSHIP, USE SCHEMA)
                # Skip it
                continue
            
            privileges.append(ActualPrivilege(
                table=qualified_table,
                ad_group=row.principal,
                privilege=privilege
            ))
        
        return privileges
    
    def table_exists(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> bool:
        """
        Check if a table exists.
        
        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name
            
        Returns:
            True if table exists
        """
        try:
            qualified_table = f"{catalog}.{schema}.{table}"
            self.spark.sql(f"DESCRIBE TABLE {qualified_table}")
            return True
        except Exception:
            return False
    
    def get_workspace_groups(self) -> List[str]:
        """
        Get all AD groups provisioned in the workspace.
        
        This can be used to validate that mapped AD groups exist
        before attempting to grant privileges.
        
        Returns:
            List of AD group names in workspace
            
        Note:
            This is useful for validation but not required for
            basic enforcement (UC will error if group doesn't exist)
        """
        try:
            df = self.spark.sql("SHOW GROUPS")
            groups = [row.groupName for row in df.collect()]
            return groups
        except Exception:
            # SHOW GROUPS might not be available in all environments
            return []