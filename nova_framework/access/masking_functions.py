"""
Column masking functions for Unity Catalog with role-based access control.

Generates SQL for Unity Catalog masking UDFs. Each masking function produces
a SQL expression body suitable for a CREATE FUNCTION statement, using 'val'
as the function parameter name.

Unity Catalog column masks require a registered function - inline expressions
are not supported with ALTER TABLE ... SET MASK. This module generates both
the function body and the full CREATE FUNCTION / ALTER TABLE SQL.
"""

from typing import Set

# Column types where string-based masking strategies (hash, redact, partial,
# mask_email, mask_postcode) can be applied directly. Non-string columns
# fall back to nullify since the UDF return type must match the column type.
_STRING_COMPATIBLE_TYPES = frozenset({
    "string", "varchar", "char", "text",
})


def _is_string_type(column_type: str) -> bool:
    """Check if column type supports string masking strategies."""
    normalised = column_type.strip().lower()
    # Handle parameterised types like varchar(255)
    base_type = normalised.split("(")[0].strip()
    return base_type in _STRING_COMPATIBLE_TYPES


def _build_conditional_mask(
    masked_expr: str,
    exempt_groups: Set[str],
) -> str:
    """
    Build conditional masking expression based on group membership.

    Uses 'val' as the UDF parameter name throughout.

    Args:
        masked_expr: SQL expression for the masked value
        exempt_groups: AD groups that see unmasked data

    Returns:
        SQL CASE expression with group-based conditions

    Example output (exempt_groups = {"engineers", "admins"}):
        CASE
            WHEN is_account_group_member('admins') THEN val
            WHEN is_account_group_member('engineers') THEN val
            ELSE sha2(val, 256)
        END
    """
    if not exempt_groups:
        return masked_expr

    when_clauses = []
    for group in sorted(exempt_groups):
        safe_group = group.replace("'", "''")
        when_clauses.append(
            f"WHEN is_account_group_member('{safe_group}') THEN val"
        )

    case_expr = (
        "CASE\n    "
        + "\n    ".join(when_clauses)
        + f"\n    ELSE {masked_expr}\nEND"
    )
    return case_expr


# ---------------------------------------------------------------------------
# Strategy implementations - each returns a SQL expression body using 'val'
# ---------------------------------------------------------------------------

def _hash_expr(exempt_groups: Set[str], algorithm: str = "sha2") -> str:
    if algorithm == "md5":
        masked = "md5(val)"
    else:
        masked = "sha2(val, 256)"
    return _build_conditional_mask(masked, exempt_groups)


def _redact_expr(exempt_groups: Set[str], replacement: str = "REDACTED") -> str:
    masked = f"CASE WHEN val IS NOT NULL THEN '{replacement}' ELSE NULL END"
    return _build_conditional_mask(masked, exempt_groups)


def _nullify_expr(exempt_groups: Set[str]) -> str:
    return _build_conditional_mask("NULL", exempt_groups)


def _mask_email_expr(exempt_groups: Set[str]) -> str:
    masked = (
        "CASE\n"
        "            WHEN val IS NOT NULL AND INSTR(val, '@') > 0 THEN\n"
        "                CONCAT(\n"
        "                    SUBSTRING(val, 1, 2),\n"
        "                    '***@',\n"
        "                    SUBSTRING(val, INSTR(val, '@') + 1)\n"
        "                )\n"
        "            ELSE NULL\n"
        "        END"
    )
    return _build_conditional_mask(masked, exempt_groups)


def _mask_postcode_expr(exempt_groups: Set[str]) -> str:
    masked = (
        "CASE\n"
        "            WHEN val IS NOT NULL THEN\n"
        "                CONCAT(\n"
        "                    TRIM(REGEXP_EXTRACT("
        "val, '^([A-Z]{1,2}[0-9]{1,2}[A-Z]?)', 1)),\n"
        "                    ' ***'\n"
        "                )\n"
        "            ELSE NULL\n"
        "        END"
    )
    return _build_conditional_mask(masked, exempt_groups)


def _partial_expr(
    exempt_groups: Set[str],
    visible_chars: int = 4,
    position: str = "end",
    mask_char: str = "*",
) -> str:
    if position == "end":
        masked = (
            "CASE\n"
            "                WHEN val IS NOT NULL THEN\n"
            "                    CONCAT(\n"
            f"                        REPEAT('{mask_char}', "
            f"GREATEST(0, LENGTH(val) - {visible_chars})),\n"
            f"                        SUBSTRING(val, -{visible_chars})\n"
            "                    )\n"
            "                ELSE NULL\n"
            "            END"
        )
    else:
        masked = (
            "CASE\n"
            "                WHEN val IS NOT NULL THEN\n"
            "                    CONCAT(\n"
            f"                        SUBSTRING(val, 1, {visible_chars}),\n"
            f"                        REPEAT('{mask_char}', "
            f"GREATEST(0, LENGTH(val) - {visible_chars}))\n"
            "                    )\n"
            "                ELSE NULL\n"
            "            END"
        )
    return _build_conditional_mask(masked, exempt_groups)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class MaskingFunctions:
    """
    Generate SQL for Unity Catalog column masking UDFs.

    All generated SQL uses 'val' as the function parameter and is
    type-aware: string-only strategies (hash, redact, partial, mask_email,
    mask_postcode) fall back to nullify for non-string columns because the
    UDF return type must match the column type.
    """

    @staticmethod
    def get_masking_expression(
        strategy: str,
        column_name: str,
        column_type: str,
        exempt_groups: Set[str] = None,
    ) -> str:
        """
        Get masking SQL expression body for a given strategy.

        The returned expression uses 'val' as the parameter name and is
        suitable for the RETURN clause of a CREATE FUNCTION statement.

        For non-string columns, strategies that produce string output
        (hash, redact, partial, mask_email, mask_postcode) fall back to
        nullify since the UDF return type must match the column type.

        Args:
            strategy: Masking strategy name
            column_name: Column name (used only for error messages)
            column_type: Column data type (determines type compatibility)
            exempt_groups: AD groups that see unmasked data

        Returns:
            SQL expression body using 'val' as parameter

        Raises:
            ValueError: If strategy is not recognised
        """
        exempt_groups = exempt_groups or set()
        strategy = strategy.lower()

        if strategy == "none":
            return "val"

        # For non-string columns, only nullify is type-safe
        is_string = _is_string_type(column_type)

        if strategy == "nullify" or not is_string:
            return _nullify_expr(exempt_groups)
        elif strategy == "hash":
            return _hash_expr(exempt_groups)
        elif strategy == "redact":
            return _redact_expr(exempt_groups)
        elif strategy == "mask_email":
            return _mask_email_expr(exempt_groups)
        elif strategy == "mask_postcode":
            return _mask_postcode_expr(exempt_groups)
        elif strategy == "partial":
            return _partial_expr(exempt_groups)
        else:
            raise ValueError(
                f"Unsupported masking strategy '{strategy}' "
                f"for column '{column_name}'"
            )

    @staticmethod
    def get_function_name(
        catalog: str,
        schema: str,
        table: str,
        column_name: str,
    ) -> str:
        """
        Generate a deterministic fully-qualified UDF name for a column mask.

        Convention: {catalog}.{schema}.nova_mask_{table}_{column}

        All parts are lowercased to avoid case-sensitivity issues.
        """
        safe_table = table.lower()
        safe_column = column_name.lower()
        return f"{catalog}.{schema}.nova_mask_{safe_table}_{safe_column}"

    @staticmethod
    def generate_create_function_sql(
        function_name: str,
        column_type: str,
        masking_body: str,
    ) -> str:
        """
        Generate CREATE OR REPLACE FUNCTION SQL for a masking UDF.

        Args:
            function_name: Fully-qualified function name
            column_type: Column data type (used for parameter and return type)
            masking_body: SQL expression body (from get_masking_expression)

        Returns:
            Complete CREATE OR REPLACE FUNCTION SQL statement
        """
        return (
            f"CREATE OR REPLACE FUNCTION {function_name}(val {column_type})\n"
            f"RETURNS {column_type}\n"
            f"RETURN {masking_body}"
        )

    @staticmethod
    def generate_set_mask_sql(
        table: str,
        column_name: str,
        function_name: str,
    ) -> str:
        """
        Generate ALTER TABLE ... SET MASK SQL.

        Args:
            table: Fully-qualified table name
            column_name: Column to mask
            function_name: Fully-qualified masking function name

        Returns:
            ALTER TABLE SQL statement
        """
        return (
            f"ALTER TABLE {table}\n"
            f"ALTER COLUMN {column_name}\n"
            f"SET MASK {function_name}"
        )

    @staticmethod
    def generate_drop_mask_sql(table: str, column_name: str) -> str:
        """Generate ALTER TABLE ... DROP MASK SQL."""
        return (
            f"ALTER TABLE {table}\n"
            f"ALTER COLUMN {column_name}\n"
            f"DROP MASK"
        )

    @staticmethod
    def generate_drop_function_sql(function_name: str) -> str:
        """Generate DROP FUNCTION IF EXISTS SQL."""
        return f"DROP FUNCTION IF EXISTS {function_name}"
