"""
Column masking functions for Unity Catalog with role-based access control.

Provides SQL expressions for various masking strategies that can be
applied to sensitive data columns, with conditional unmasking for
authorized AD groups.
"""

from typing import Optional, Set


class MaskingFunctions:
    """
    Collection of masking functions that generate SQL expressions.

    These functions return SQL expressions that can be used in
    Unity Catalog ALTER TABLE statements or masking policies.

    All functions support role-based access: groups in exempt_groups
    see raw data, others see masked data.
    """

    @staticmethod
    def _build_conditional_mask(
        column_name: str,
        masked_expr: str,
        exempt_groups: Set[str]
    ) -> str:
        """
        Build conditional masking expression based on group membership.

        Args:
            column_name: Column to mask
            masked_expr: SQL expression for masked value
            exempt_groups: AD groups that see unmasked data

        Returns:
            SQL CASE expression with group-based conditions

        Example:
            If exempt_groups = {"engineers", "admins"}:
            CASE
                WHEN is_account_group_member('engineers') THEN column_name
                WHEN is_account_group_member('admins') THEN column_name
                ELSE <masked_expr>
            END
        """
        if not exempt_groups:
            # No exempt groups - mask for everyone
            return masked_expr

        # Build WHEN clauses for each exempt group
        when_clauses = []
        for group in sorted(exempt_groups):  # Sort for deterministic output
            # Escape single quotes in group name
            safe_group = group.replace("'", "''")
            when_clauses.append(
                f"WHEN is_account_group_member('{safe_group}') THEN {column_name}"
            )

        # Build complete CASE expression
        case_expr = "CASE\n    " + "\n    ".join(when_clauses) + f"\n    ELSE {masked_expr}\nEND"
        return case_expr

    @staticmethod
    def hash_column(
        column_name: str,
        exempt_groups: Set[str] = None,
        algorithm: str = "sha2"
    ) -> str:
        """
        Generate SQL for hashing a column with role-based access.

        Args:
            column_name: Column to hash
            exempt_groups: AD groups that see unmasked data
            algorithm: Hash algorithm (sha2, md5)

        Returns:
            SQL expression for conditional hashing

        Example:
            hash_column("email", {"engineers"})
            ->
            CASE
                WHEN is_account_group_member('engineers') THEN email
                ELSE sha2(email, 256)
            END
        """
        exempt_groups = exempt_groups or set()

        # Generate base masking expression
        if algorithm == "sha2":
            masked = f"sha2({column_name}, 256)"
        elif algorithm == "md5":
            masked = f"md5({column_name})"
        else:
            masked = f"sha2({column_name}, 256)"  # Default to SHA-256

        return MaskingFunctions._build_conditional_mask(
            column_name, masked, exempt_groups
        )

    @staticmethod
    def redact_column(
        column_name: str,
        exempt_groups: Set[str] = None,
        replacement: str = "REDACTED"
    ) -> str:
        """
        Generate SQL for full redaction with role-based access.

        Args:
            column_name: Column to redact
            exempt_groups: AD groups that see unmasked data
            replacement: Replacement text

        Returns:
            SQL expression for conditional redaction

        Example:
            redact_column("ssn", {"hr_team"})
            ->
            CASE
                WHEN is_account_group_member('hr_team') THEN ssn
                ELSE CASE WHEN ssn IS NOT NULL THEN 'REDACTED' ELSE NULL END
            END
        """
        exempt_groups = exempt_groups or set()

        # Generate base masking expression
        masked = f"CASE WHEN {column_name} IS NOT NULL THEN '{replacement}' ELSE NULL END"

        return MaskingFunctions._build_conditional_mask(
            column_name, masked, exempt_groups
        )

    @staticmethod
    def nullify_column(
        column_name: str,
        exempt_groups: Set[str] = None
    ) -> str:
        """
        Generate SQL for nullification with role-based access.

        Args:
            column_name: Column to nullify
            exempt_groups: AD groups that see unmasked data

        Returns:
            SQL expression for conditional nullification

        Example:
            nullify_column("salary", {"finance_team"})
            ->
            CASE
                WHEN is_account_group_member('finance_team') THEN salary
                ELSE NULL
            END
        """
        exempt_groups = exempt_groups or set()

        return MaskingFunctions._build_conditional_mask(
            column_name, "NULL", exempt_groups
        )

    @staticmethod
    def mask_email(
        column_name: str,
        exempt_groups: Set[str] = None
    ) -> str:
        """
        Generate SQL for email masking with role-based access.

        Masks email by showing first 2 chars and domain, hiding middle part.
        Example: john.smith@example.com -> jo***@example.com

        Args:
            column_name: Email column to mask
            exempt_groups: AD groups that see unmasked data

        Returns:
            SQL expression for conditional email masking

        Example:
            mask_email("email", {"sales_team"})
            ->
            CASE
                WHEN is_account_group_member('sales_team') THEN email
                ELSE CASE WHEN email IS NOT NULL AND INSTR(email, '@') > 0 THEN
                    CONCAT(SUBSTRING(email, 1, 2), '***@', SUBSTRING(email, INSTR(email, '@') + 1))
                ELSE NULL END
            END
        """
        exempt_groups = exempt_groups or set()

        # Generate base masking expression
        masked = f"""CASE
            WHEN {column_name} IS NOT NULL AND INSTR({column_name}, '@') > 0 THEN
                CONCAT(
                    SUBSTRING({column_name}, 1, 2),
                    '***@',
                    SUBSTRING({column_name}, INSTR({column_name}, '@') + 1)
                )
            ELSE NULL
        END"""

        return MaskingFunctions._build_conditional_mask(
            column_name, masked, exempt_groups
        )

    @staticmethod
    def mask_postcode(
        column_name: str,
        exempt_groups: Set[str] = None
    ) -> str:
        """
        Generate SQL for UK postcode partial masking with role-based access.

        Masks UK postcode by showing only the outward code (first part),
        hiding the inward code.
        Example: SW1A 2AA -> SW1A ***

        Args:
            column_name: Postcode column to mask
            exempt_groups: AD groups that see unmasked data

        Returns:
            SQL expression for conditional postcode masking

        Example:
            mask_postcode("POST_CODE", {"data_team"})
            ->
            CASE
                WHEN is_account_group_member('data_team') THEN POST_CODE
                ELSE CASE WHEN POST_CODE IS NOT NULL THEN
                    CONCAT(TRIM(REGEXP_EXTRACT(POST_CODE, '^([A-Z]{1,2}[0-9]{1,2}[A-Z]?)', 1)), ' ***')
                ELSE NULL END
            END
        """
        exempt_groups = exempt_groups or set()

        # Generate base masking expression
        masked = f"""CASE
            WHEN {column_name} IS NOT NULL THEN
                CONCAT(
                    TRIM(REGEXP_EXTRACT({column_name}, '^([A-Z]{{1,2}}[0-9]{{1,2}}[A-Z]?)', 1)),
                    ' ***'
                )
            ELSE NULL
        END"""

        return MaskingFunctions._build_conditional_mask(
            column_name, masked, exempt_groups
        )

    @staticmethod
    def mask_partial(
        column_name: str,
        exempt_groups: Set[str] = None,
        visible_chars: int = 4,
        position: str = "end",
        mask_char: str = "*"
    ) -> str:
        """
        Generate SQL for partial masking with role-based access.

        Shows only a portion of the value, masks the rest.

        Args:
            column_name: Column to partially mask
            exempt_groups: AD groups that see unmasked data
            visible_chars: Number of characters to show
            position: Which part to show ('start' or 'end')
            mask_char: Character to use for masking

        Returns:
            SQL expression for conditional partial masking

        Examples:
            mask_partial("card_number", {"merchants"}, 4, "end")
            -> Shows last 4 digits: ****1234

            mask_partial("phone", {"support"}, 3, "start")
            -> Shows first 3 digits: 123****
        """
        exempt_groups = exempt_groups or set()

        # Generate base masking expression
        if position == "end":
            # Show last N characters
            masked = f"""CASE
                WHEN {column_name} IS NOT NULL THEN
                    CONCAT(
                        REPEAT('{mask_char}', GREATEST(0, LENGTH({column_name}) - {visible_chars})),
                        SUBSTRING({column_name}, -{visible_chars})
                    )
                ELSE NULL
            END"""
        else:
            # Show first N characters
            masked = f"""CASE
                WHEN {column_name} IS NOT NULL THEN
                    CONCAT(
                        SUBSTRING({column_name}, 1, {visible_chars}),
                        REPEAT('{mask_char}', GREATEST(0, LENGTH({column_name}) - {visible_chars}))
                    )
                ELSE NULL
            END"""

        return MaskingFunctions._build_conditional_mask(
            column_name, masked, exempt_groups
        )

    @staticmethod
    def get_masking_expression(
        strategy: str,
        column_name: str,
        column_type: str,
        exempt_groups: Set[str] = None
    ) -> str:
        """
        Get masking SQL expression for a given strategy.

        Args:
            strategy: Masking strategy (hash, redact, partial, etc.)
            column_name: Column to mask
            column_type: Column data type
            exempt_groups: AD groups that see unmasked data

        Returns:
            SQL expression for role-based masking

        Raises:
            ValueError: If strategy is not supported
        """
        exempt_groups = exempt_groups or set()
        strategy = strategy.lower()

        if strategy == "hash":
            return MaskingFunctions.hash_column(column_name, exempt_groups)
        elif strategy == "redact":
            return MaskingFunctions.redact_column(column_name, exempt_groups)
        elif strategy == "nullify":
            return MaskingFunctions.nullify_column(column_name, exempt_groups)
        elif strategy == "mask_email":
            return MaskingFunctions.mask_email(column_name, exempt_groups)
        elif strategy == "mask_postcode":
            return MaskingFunctions.mask_postcode(column_name, exempt_groups)
        elif strategy == "partial":
            return MaskingFunctions.mask_partial(column_name, exempt_groups)
        elif strategy == "none":
            return column_name  # No masking
        else:
            raise ValueError(f"Unsupported masking strategy: {strategy}")
