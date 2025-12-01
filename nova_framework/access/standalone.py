"""
Standalone access control enforcement tool.

Provides on-demand access control enforcement outside of pipeline execution.
"""

from typing import Dict, Optional
from pyspark.sql import SparkSession
from .metadata_loader import AccessMetadataLoader
from .uc_inspector import UCPrivilegeInspector
from .delta_generator import PrivilegeDeltaGenerator
from .grant_revoker import GrantRevoker
from .models import AccessControlResult


class StandaloneAccessControlTool:
    """
    On-demand access control enforcement with full differential analysis.
    
    Use cases:
    - Apply/refresh access control to existing tables
    - Bulk enforcement across schema
    - Audit current vs intended state
    - Testing and validation
    
    Usage:
        tool = StandaloneAccessControlTool(
            spark=spark,
            environment="dev",
            dry_run=False
        )
        
        # Single table
        result = tool.apply_to_table(
            catalog="cluk_dev_nova",
            schema="finance",
            table="payment_table1"
        )
        
        # Entire schema
        results = tool.apply_to_schema(
            catalog="cluk_dev_nova",
            schema="finance"
        )
    """
    
    def __init__(
        self,
        spark: SparkSession,
        environment: str,
        registry_path: Optional[str] = None,
        dry_run: bool = False
    ):
        """
        Initialize standalone tool.
        
        Args:
            spark: Active Spark session
            environment: Environment (dev, test, prod)
            registry_path: Contract registry path (auto-detected if None)
            dry_run: If True, preview changes without executing
        """
        self.spark = spark
        self.environment = environment
        self.dry_run = dry_run
        
        # Auto-detect registry path
        if registry_path is None:
            try:
                catalog = spark.conf.get("nova.catalog")
                registry_path = f"/Volumes/{catalog}/contract_registry"
            except:
                raise ValueError(
                    "registry_path must be provided or nova.catalog "
                    "must be set in Spark conf"
                )
        
        # Initialize components
        self.metadata_loader = AccessMetadataLoader(
            registry_path=registry_path,
            environment=environment
        )
        self.uc_inspector = UCPrivilegeInspector(spark=spark)
        self.delta_generator = PrivilegeDeltaGenerator()
        self.grant_revoker = GrantRevoker(spark=spark, dry_run=dry_run)
    
    def apply_to_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        verbose: bool = True
    ) -> AccessControlResult:
        """
        Apply differential access control to a single table.
        
        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name
            verbose: If True, print detailed output
            
        Returns:
            AccessControlResult with execution details
        """
        qualified_table = f"{catalog}.{schema}.{table}"
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"Applying access control: {qualified_table}")
            print(f"{'='*60}\n")
        
        # Step 1: Load intended privileges
        intended = self.metadata_loader.get_intended_privileges(
            catalog, schema, table
        )
        
        if not intended:
            if verbose:
                print("No access rules defined for this table")
            return AccessControlResult(
                table=qualified_table,
                intended_count=0,
                actual_count=0,
                no_change_count=0,
                grants_attempted=0,
                grants_succeeded=0,
                grants_failed=0,
                revokes_attempted=0,
                revokes_succeeded=0,
                revokes_failed=0,
                execution_time_seconds=0.0
            )
        
        if verbose:
            print(f"Intended privileges: {len(intended)}")
        
        # Step 2: Query actual privileges
        actual = self.uc_inspector.get_actual_privileges(
            catalog, schema, table
        )
        
        if verbose:
            print(f"Actual privileges: {len(actual)}")
        
        # Step 3: Generate deltas
        deltas, no_change_count = self.delta_generator.generate_deltas(
            intended, actual
        )
        
        grants, revokes = self.delta_generator.group_by_action(deltas)
        
        if verbose:
            print(f"\nChanges needed:")
            print(f"  GRANTs:  {len(grants)}")
            print(f"  REVOKEs: {len(revokes)}")
            print(f"  Correct: {no_change_count}")
        
        # Show SQL statements
        if verbose and deltas:
            if grants:
                print(f"\nGRANTs to execute:")
                for grant in grants:
                    print(f"  {grant.sql}")
            
            if revokes:
                print(f"\nREVOKEs to execute:")
                for revoke in revokes:
                    print(f"  {revoke.sql}")
        
        # Step 4: Execute changes
        if deltas:
            if verbose:
                print(f"\nExecuting changes...")
                if self.dry_run:
                    print("[DRY RUN MODE - No changes executed]")
            
            result = self.grant_revoker.apply_deltas(
                table=qualified_table,
                deltas=deltas,
                intended_count=len(intended),
                actual_count=len(actual),
                no_change_count=no_change_count
            )
        else:
            if verbose:
                print("\nNo changes needed - privileges already correct")
            
            result = AccessControlResult(
                table=qualified_table,
                intended_count=len(intended),
                actual_count=len(actual),
                no_change_count=no_change_count,
                grants_attempted=0,
                grants_succeeded=0,
                grants_failed=0,
                revokes_attempted=0,
                revokes_succeeded=0,
                revokes_failed=0,
                execution_time_seconds=0.0
            )
        
        # Print results
        if verbose:
            print(f"\nResult:")
            print(f"  GRANTs:  {result.grants_succeeded}/{result.grants_attempted}")
            print(f"  REVOKEs: {result.revokes_succeeded}/{result.revokes_attempted}")
            print(f"  Success rate: {result.success_rate:.1f}%")
            print(f"  Execution time: {result.execution_time_seconds:.2f}s")
            
            if result.errors:
                print(f"\nErrors:")
                for error in result.errors:
                    print(f"  - {error}")
        
        return result
    
    def apply_to_schema(
        self,
        catalog: str,
        schema: str,
        verbose: bool = True
    ) -> Dict[str, AccessControlResult]:
        """
        Apply access control to all tables in a schema.
        
        Args:
            catalog: UC catalog
            schema: UC schema
            verbose: If True, print detailed output
            
        Returns:
            Dict of table_name -> AccessControlResult
        """
        if verbose:
            print(f"\n{'='*60}")
            print(f"Applying access control to schema: {catalog}.{schema}")
            print(f"{'='*60}\n")
        
        # Get all tables in schema
        tables_df = self.spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
        tables = [row.tableName for row in tables_df.collect()]
        
        if verbose:
            print(f"Found {len(tables)} tables\n")
        
        results = {}
        
        # Process each table
        for table in tables:
            result = self.apply_to_table(
                catalog, schema, table, verbose=verbose
            )
            results[table] = result
        
        # Print summary
        if verbose:
            print(f"\n{'='*60}")
            print("SUMMARY")
            print(f"{'='*60}\n")
            
            total_grants = sum(r.grants_attempted for r in results.values())
            total_revokes = sum(r.revokes_attempted for r in results.values())
            total_succeeded_grants = sum(r.grants_succeeded for r in results.values())
            total_succeeded_revokes = sum(r.revokes_succeeded for r in results.values())
            
            print(f"Processed {len(tables)} tables:")
            print(f"  Total GRANTs:  {total_succeeded_grants}/{total_grants}")
            print(f"  Total REVOKEs: {total_succeeded_revokes}/{total_revokes}")
            
            # List tables with errors
            failed = [t for t, r in results.items() if not r.is_successful]
            
            if failed:
                print(f"\nTables with errors ({len(failed)}):")
                for table in failed:
                    print(f"  - {table}")
        
        return results
    
    def audit_table(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> dict:
        """
        Audit a table's access control without making changes.
        
        Returns the intended vs actual state for review.
        
        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name
            
        Returns:
            Dict with audit results
        """
        qualified_table = f"{catalog}.{schema}.{table}"
        
        # Load intended
        intended = self.metadata_loader.get_intended_privileges(
            catalog, schema, table
        )
        
        # Query actual
        actual = self.uc_inspector.get_actual_privileges(
            catalog, schema, table
        )
        
        # Generate deltas (but don't execute)
        deltas, no_change = self.delta_generator.generate_deltas(
            intended, actual
        )
        
        grants, revokes = self.delta_generator.group_by_action(deltas)
        
        return {
            "table": qualified_table,
            "intended_count": len(intended),
            "actual_count": len(actual),
            "no_change_count": no_change,
            "grants_needed": len(grants),
            "revokes_needed": len(revokes),
            "is_compliant": len(deltas) == 0,
            "intended": [
                {
                    "ad_group": i.ad_group,
                    "privilege": i.privilege.value,
                    "reason": i.reason
                }
                for i in intended
            ],
            "actual": [
                {
                    "ad_group": a.ad_group,
                    "privilege": a.privilege.value
                }
                for a in actual
            ],
            "grants": [g.sql for g in grants],
            "revokes": [r.sql for r in revokes]
        }