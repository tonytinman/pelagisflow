"""
Unity Catalog masking policy inspector.

Queries Unity Catalog to determine current column masking state for tables.
"""

from typing import List, Dict, Optional
from pyspark.sql import SparkSession
from .privacy_models import UCMaskingPolicy


class UCMaskingInspector:
    """
    Query Unity Catalog to get current column masking state.

    Inspects table metadata to determine which columns have masking
    policies applied and what expressions are used.

    Usage:
        inspector = UCMaskingInspector(spark)

        policies = inspector.get_column_masks(
            catalog="cluk_dev_nova",
            schema="bronze_galahad",
            table="gallive_manager_address"
        )
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize inspector.

        Args:
            spark: Active Spark session with UC access
        """
        self.spark = spark

    def get_column_masks(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> List[UCMaskingPolicy]:
        """
        Get current masking policies for all columns in a table.

        Queries Unity Catalog table metadata to determine which columns
        have masking expressions or policies applied.

        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name

        Returns:
            List of UCMaskingPolicy (current masking state)
            Empty list if table doesn't exist or has no masking

        Example:
            policies = inspector.get_column_masks(
                "cluk_dev_nova", "bronze_galahad", "gallive_manager_address"
            )
            # Returns: [
            #   UCMaskingPolicy(
            #       table="cluk_dev_nova.bronze_galahad.gallive_manager_address",
            #       column_name="POST_CODE",
            #       masking_expression="CASE WHEN is_account_group_member('engineers') THEN POST_CODE ELSE sha2(POST_CODE, 256) END",
            #       policy_name=None
            #   )
            # ]
        """
        qualified_table = f"{catalog}.{schema}.{table}"

        # Check if table exists first
        if not self.table_exists(catalog, schema, table):
            return []

        policies = []

        # Try to get column masks from UC system tables
        # Note: Unity Catalog stores column masks in:
        # - system.information_schema.column_masks (if available)
        # - Table extended properties
        # - DESCRIBE TABLE EXTENDED output

        # Method 1: Try system.information_schema.column_masks
        try:
            masks_from_system = self._get_masks_from_system_tables(
                catalog, schema, table
            )
            if masks_from_system:
                return masks_from_system
        except Exception:
            # system.information_schema.column_masks not available
            pass

        # Method 2: Try DESCRIBE TABLE EXTENDED
        try:
            masks_from_describe = self._get_masks_from_describe(
                qualified_table
            )
            if masks_from_describe:
                return masks_from_describe
        except Exception:
            pass

        # No masking found
        return []

    def _get_masks_from_system_tables(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> List[UCMaskingPolicy]:
        """
        Get column masks from UC system.information_schema.column_masks.

        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name

        Returns:
            List of UCMaskingPolicy

        Note:
            This table may not exist in all UC deployments.
        """
        qualified_table = f"{catalog}.{schema}.{table}"

        query = f"""
            SELECT
                column_name,
                mask_expression
            FROM system.information_schema.column_masks
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
        """

        try:
            result = self.spark.sql(query).collect()

            policies = []
            for row in result:
                if row.mask_expression:  # Only include if mask exists
                    policies.append(UCMaskingPolicy(
                        table=qualified_table,
                        column_name=row.column_name,
                        masking_expression=row.mask_expression,
                        policy_name=None
                    ))

            return policies

        except Exception:
            # Query failed - table doesn't exist or not accessible
            return []

    def _get_masks_from_describe(
        self,
        qualified_table: str
    ) -> List[UCMaskingPolicy]:
        """
        Get column masks from DESCRIBE TABLE EXTENDED.

        Parses table metadata to find column masking information.

        Args:
            qualified_table: Fully qualified table name

        Returns:
            List of UCMaskingPolicy
        """
        try:
            # Get extended table description
            df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {qualified_table}")
            rows = df.collect()

            policies = []

            # Parse output for column mask information
            # Format varies by UC version, but typically:
            # col_name | data_type | comment
            # ...
            # # Detailed Table Information
            # ...
            # Column Masks: column_name: expression

            in_mask_section = False
            for row in rows:
                col_name = row.col_name if hasattr(row, 'col_name') else None

                # Look for column mask section
                if col_name and "Column Mask" in col_name:
                    in_mask_section = True
                    continue

                if in_mask_section and col_name:
                    # Parse mask info
                    # Format: "column_name: mask_expression"
                    if ":" in col_name:
                        parts = col_name.split(":", 1)
                        if len(parts) == 2:
                            column = parts[0].strip()
                            expression = parts[1].strip()

                            policies.append(UCMaskingPolicy(
                                table=qualified_table,
                                column_name=column,
                                masking_expression=expression,
                                policy_name=None
                            ))

            return policies

        except Exception:
            return []

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
            self.spark.sql(f"DESCRIBE TABLE {qualified_table}").collect()
            return True
        except Exception:
            return False

    def column_exists(
        self,
        catalog: str,
        schema: str,
        table: str,
        column: str
    ) -> bool:
        """
        Check if a column exists in a table.

        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name
            column: Column name

        Returns:
            True if column exists
        """
        columns = self.get_table_columns(catalog, schema, table)
        return column in columns

    def get_table_columns(
        self,
        catalog: str,
        schema: str,
        table: str
    ) -> Dict[str, str]:
        """
        Get all columns and their types for a table.

        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name

        Returns:
            Dictionary mapping column names to data types

        Example:
            {"customer_id": "bigint", "email": "string"}
        """
        try:
            qualified_table = f"{catalog}.{schema}.{table}"
            df = self.spark.sql(f"DESCRIBE TABLE {qualified_table}")

            columns = {}
            for row in df.collect():
                col_name = row.col_name

                # Skip metadata rows
                if col_name.startswith("#") or col_name == "" or col_name.startswith("Detailed"):
                    continue

                # Stop at partition information
                if "Partition" in col_name or "partition" in col_name:
                    break

                data_type = row.data_type if hasattr(row, 'data_type') else "unknown"
                columns[col_name] = data_type

            return columns

        except Exception:
            return {}

    def get_column_type(
        self,
        catalog: str,
        schema: str,
        table: str,
        column: str
    ) -> Optional[str]:
        """
        Get data type for a specific column.

        Args:
            catalog: UC catalog
            schema: UC schema
            table: Table name
            column: Column name

        Returns:
            Data type string or None if column doesn't exist
        """
        columns = self.get_table_columns(catalog, schema, table)
        return columns.get(column)
