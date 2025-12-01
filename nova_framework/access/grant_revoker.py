"""
GRANT/REVOKE executor for Unity Catalog privileges.

Executes privilege changes (deltas) by running GRANT and REVOKE SQL statements.
"""

import time
from typing import List
from pyspark.sql import SparkSession
from .models import PrivilegeDelta, AccessControlResult


class GrantRevoker:
    """
    Execute GRANT and REVOKE SQL statements in Unity Catalog.

    Handles execution of privilege deltas with error handling,
    success/failure tracking, and optional dry-run mode.

    Usage:
        revoker = GrantRevoker(spark=spark, dry_run=False)

        result = revoker.apply_deltas(
            table="catalog.schema.table1",
            deltas=[...],
            intended_count=5,
            actual_count=3,
            no_change_count=2
        )
    """

    def __init__(self, spark: SparkSession, dry_run: bool = False):
        """
        Initialize grant/revoke executor.

        Args:
            spark: Active Spark session with UC access
            dry_run: If True, preview SQL without executing
        """
        self.spark = spark
        self.dry_run = dry_run

    def apply_deltas(
        self,
        table: str,
        deltas: List[PrivilegeDelta],
        intended_count: int,
        actual_count: int,
        no_change_count: int
    ) -> AccessControlResult:
        """
        Apply privilege deltas by executing GRANT and REVOKE statements.

        Args:
            table: Fully qualified table name
            deltas: List of privilege changes to apply
            intended_count: Number of intended privileges
            actual_count: Number of actual privileges (before changes)
            no_change_count: Number of privileges already correct

        Returns:
            AccessControlResult with execution details

        Example:
            result = revoker.apply_deltas(
                table="catalog.schema.table1",
                deltas=[
                    PrivilegeDelta(action="GRANT", ...),
                    PrivilegeDelta(action="REVOKE", ...)
                ],
                intended_count=5,
                actual_count=3,
                no_change_count=2
            )

            print(f"Success: {result.is_successful}")
            print(f"GRANTs: {result.grants_succeeded}/{result.grants_attempted}")
        """
        start_time = time.time()

        # Initialize counters
        grants_attempted = 0
        grants_succeeded = 0
        grants_failed = 0
        revokes_attempted = 0
        revokes_succeeded = 0
        revokes_failed = 0
        errors = []

        # Process each delta
        for delta in deltas:
            if delta.action == "GRANT":
                grants_attempted += 1

                try:
                    if not self.dry_run:
                        self.spark.sql(delta.sql)
                    grants_succeeded += 1

                except Exception as e:
                    grants_failed += 1
                    error_msg = f"GRANT failed for {delta.ad_group}: {str(e)}"
                    errors.append(error_msg)

            elif delta.action == "REVOKE":
                revokes_attempted += 1

                try:
                    if not self.dry_run:
                        self.spark.sql(delta.sql)
                    revokes_succeeded += 1

                except Exception as e:
                    revokes_failed += 1
                    error_msg = f"REVOKE failed for {delta.ad_group}: {str(e)}"
                    errors.append(error_msg)

        execution_time = time.time() - start_time

        # Build result
        result = AccessControlResult(
            table=table,
            intended_count=intended_count,
            actual_count=actual_count,
            no_change_count=no_change_count,
            grants_attempted=grants_attempted,
            grants_succeeded=grants_succeeded,
            grants_failed=grants_failed,
            revokes_attempted=revokes_attempted,
            revokes_succeeded=revokes_succeeded,
            revokes_failed=revokes_failed,
            execution_time_seconds=execution_time,
            errors=errors
        )

        return result

    def execute_single_delta(self, delta: PrivilegeDelta) -> bool:
        """
        Execute a single GRANT or REVOKE statement.

        Args:
            delta: Privilege delta to execute

        Returns:
            True if successful, False if failed

        Example:
            delta = PrivilegeDelta(action="GRANT", ...)
            success = revoker.execute_single_delta(delta)
        """
        try:
            if not self.dry_run:
                self.spark.sql(delta.sql)
            return True
        except Exception:
            return False

    def validate_permissions(self) -> bool:
        """
        Validate that Spark session has permissions to GRANT/REVOKE.

        This is a basic check - attempts a harmless query to verify
        the session is active and connected to UC.

        Returns:
            True if session appears valid
        """
        try:
            # Simple query to verify session is active
            self.spark.sql("SELECT 1").collect()
            return True
        except Exception:
            return False
