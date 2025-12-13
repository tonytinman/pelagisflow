"""
Nova Data Quality Engine

Provides data cleansing and validation capabilities for pipeline processing.

- Cleansing rules modify data
- Validation rules annotate data without modifying original values
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from nova_framework.observability import get_logger

logger = get_logger("quality.dqengine")


class DQEngine:
    """
    Data Quality Engine for cleansing and validation.
    
    Supports:
    - Cleansing rules: Transform data (trim, uppercase, regex replace, etc.)
    - Validation rules: Check data quality and generate scores
    - Weighted scoring: Assign weights to rules for importance
    - Error tracking: Generate detailed error reports
    
    Example:
        engine = DQEngine()
        
        # Apply cleansing
        df_clean = engine.apply_cleansing(df, cleansing_rules)
        
        # Apply validation
        summary, df_quality, df_errors = engine.apply_dq(df_clean, quality_rules)
    """

    def __init__(self):
        """Initialize DQ Engine."""
        pass

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _safe_cast_to_double(self, col):
        """
        Safely cast column to double, handling non-numeric values.
        
        Args:
            col: Column name
            
        Returns:
            Double value or None for non-numeric values
        """
        cleaned = F.regexp_replace(F.col(col), "[^0-9.+-]", "")
        return F.when(
            cleaned.rlike("^[+-]?([0-9]*[.])?[0-9]+$"),
            cleaned.cast("double")
        ).otherwise(F.lit(None))

    # =========================================================================
    # CLEANSING (TRANSFORMATIONS)
    # =========================================================================

    def clean_trim(self, df, rule):
        """
        Trim whitespace from column(s).
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key
            
        Returns:
            DataFrame with trimmed column(s)
        """
        # Handle both singular and plural
        cols = rule.get("columns") or [rule.get("column")]
        for col in cols:
            if col:
                df = df.withColumn(col, F.trim(F.col(col)))
        return df

    def clean_upper(self, df, rule):
        """
        Convert column(s) to uppercase.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key
            
        Returns:
            DataFrame with uppercase column(s)
        """
        cols = rule.get("columns") or [rule.get("column")]
        for col in cols:
            if col:
                df = df.withColumn(col, F.upper(F.col(col)))
        return df

    def clean_lower(self, df, rule):
        """
        Convert column(s) to lowercase.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key
            
        Returns:
            DataFrame with lowercase column(s)
        """
        cols = rule.get("columns") or [rule.get("column")]
        for col in cols:
            if col:
                df = df.withColumn(col, F.lower(F.col(col)))
        return df

    def clean_regex_replace(self, df, rule):
        """
        Replace regex pattern in column.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column', 'pattern', and optional 'replacement'
            
        Returns:
            DataFrame with replaced values
        """
        col = rule.get("column")
        if not col:
            logger.warning("clean_regex_replace: missing 'column' in rule")
            return df
            
        return df.withColumn(
            col,
            F.regexp_replace(F.col(col), rule["pattern"], rule.get("replacement", ""))
        )

    def clean_nullify_empty_strings(self, df, rule):
        """
        Convert empty strings to NULL.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key
            
        Returns:
            DataFrame with nullified empty strings
        """
        cols = rule.get("columns") or [rule.get("column")]
        for col in cols:
            if col:
                df = df.withColumn(
                    col,
                    F.when(F.trim(F.col(col)) == "", None).otherwise(F.col(col))
                )
        return df

    def clean_normalize_boolean_values(self, df, rule):
        """
        Map truthy/falsy forms into True/False strings.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' key
            
        Returns:
            DataFrame with normalized boolean values
        """
        col = rule.get("column")
        if not col:
            logger.warning("clean_normalize_boolean_values: missing 'column' in rule")
            return df
            
        logger.debug(f"Normalizing boolean values for column: {col}")
        
        return df.withColumn(
            col,
            F.when(F.lower(F.col(col)).isin("1", "t", "true", "yes", "y"), "True")
             .when(F.lower(F.col(col)).isin("0", "f", "false", "no", "n"), "False")
             .otherwise(F.col(col))
        )

    # =========================================================================
    # EVALUATION RULES
    # =========================================================================

    def rule_not_null(self, df, rule):
        """
        Check if column value is null.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' key
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        return F.col(col).isNull()

    def rule_not_blank(self, df, rule):
        """
        Check if column value is null or blank.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' key
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        return (F.col(col).isNull()) | (F.trim(F.col(col)) == "")

    def rule_regex(self, df, rule):
        """
        Check if column value matches regex pattern.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'pattern' keys
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        pattern = rule["pattern"]
        return ~F.col(col).rlike(pattern)

    def rule_allowed_values(self, df, rule):
        """
        Check if column value is in allowed list.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'values' keys
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        values = rule["values"]
        return ~F.col(col).isin(values)

    def rule_min_length(self, df, rule):
        """
        Check if column value length is below minimum.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'value' (min length) keys
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        min_len = rule["value"]
        return F.length(F.col(col)) < min_len

    def rule_max_length(self, df, rule):
        """
        Check if column value length exceeds maximum.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'value' (max length) keys
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        max_len = rule["value"]
        return F.length(F.col(col)) > max_len

    def rule_digits_only(self, df, rule):
        """
        Check if column value contains only digits.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' key
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        return ~F.col(col).rlike("^[0-9]+$")

    def rule_letters_only(self, df, rule):
        """
        Check if column value contains only letters.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' key
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        return ~F.col(col).rlike("^[A-Za-z]+$")

    def rule_is_number(self, df, rule):
        """
        Check if column value can be converted to number.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' key
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        return self._safe_cast_to_double(col).isNull() & F.col(col).isNotNull()

    def rule_min(self, df, rule):
        """
        Check if numeric column value is below minimum.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'value' (min) keys
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        min_val = rule["value"]
        return self._safe_cast_to_double(col) < F.lit(min_val)

    def rule_max(self, df, rule):
        """
        Check if numeric column value exceeds maximum.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'value' (max) keys
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        max_val = rule["value"]
        return self._safe_cast_to_double(col) > F.lit(max_val)

    def rule_between(self, df, rule):
        """
        Check if numeric column value is outside range.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column', 'min', and 'max' keys
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        return ~(
            (self._safe_cast_to_double(col) >= F.lit(rule["min"])) &
            (self._safe_cast_to_double(col) <= F.lit(rule["max"]))
        )

    def rule_conditional(self, df, rule):
        """
        Evaluate custom SQL expression.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'expression' key (SQL returning true if FAIL)
            
        Returns:
            Boolean expression (True = failure)
        """
        expr = rule["expression"]
        return F.expr(expr)

    def rule_is_date(self, df, rule):
        """
        Check if column value is valid date.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' and optional 'format' keys
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        fmt = rule.get("format", "yyyy-MM-dd")
        return F.to_date(F.col(col), fmt).isNull() & F.col(col).isNotNull()

    def rule_unique(self, df, rule):
        """
        Check if column value is unique.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'column' key
            
        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]
        return F.count(F.col(col)).over(Window.partitionBy(col)) > 1

    def rule_composite_unique(self, df, rule):
        """
        Check if combination of columns is unique.
        
        Args:
            df: Input DataFrame
            rule: Dict with 'columns' key (list of column names)
            
        Returns:
            Boolean expression (True = failure)
        """
        cols = rule["columns"]
        return (
            F.count(F.lit(1)).over(Window.partitionBy(*cols)) > 1
        )

    # =========================================================================
    # DQ EXECUTION
    # =========================================================================

    def apply_cleansing(self, df, rules):
        """
        Apply cleansing rules to DataFrame.
        
        Args:
            df: Input DataFrame
            rules: List of cleansing rule dicts
            
        Returns:
            DataFrame with cleansing rules applied
            
        Example:
            rules = [
                {"rule": "trim", "columns": ["name", "email"]},
                {"rule": "upper", "column": "status"}
            ]
            df_clean = engine.apply_cleansing(df, rules)
        """
        for r in rules:
            try:
                handler = getattr(self, f"clean_{r['rule']}", None)
                if handler:
                    df = handler(df, r)
                else:
                    logger.warning(f"Unknown cleansing rule: {r['rule']}")
            except Exception as e:
                logger.error(f"Cleansing rule {r['rule']} failed: {str(e)}")
                # Continue with other rules
                continue
                
        return df

    def apply_dq(self, df, rules):
        """
        Apply DQ validation rules without creating a column per rule.
        Ensures schema stability even if no rules exist.

        CRITICAL: This method ALWAYS adds 4 DQ columns to the DataFrame, regardless
        of whether rules are defined. This prevents schema drift between initial
        loads (no rules) and subsequent loads (with rules).

        Columns Always Added:
        1. _dq_failures     - array<struct<...>> of rule failures (empty if no rules)
        2. _dq_fail_weight  - int, sum of failure weights (0 if no rules)
        3. dq_score         - double, quality score 0-100 (100 if no rules)
        4. _dq_error        - boolean, true if any failures (false if no rules)

        When rules is empty or None:
        - All rows get dq_score = 100.0 (perfect score)
        - _dq_failures = empty array []
        - _dq_fail_weight = 0
        - _dq_error = false

        This ensures consistent schema for downstream processing and prevents
        mergeSchema overhead when quality rules are added later.

        Args:
            df: Input DataFrame
            rules: List of validation rule dicts (can be empty or None)

        Returns:
            Tuple of (summary_dict, annotated_df, errors_df)

        Example:
            # With rules
            rules = [
                {"rule": "not_null", "column": "customer_id", "weight": 2},
                {"rule": "regex", "column": "email", "pattern": "^[\\w.]+@[\\w.]+$", "weight": 1}
            ]
            summary, df_quality, df_errors = engine.apply_dq(df, rules)
            # df_quality has dq_score based on rule evaluation

            # Without rules (empty list)
            summary, df_quality, df_errors = engine.apply_dq(df, [])
            # df_quality has dq_score = 100.0 for all rows
        """
        annotated = df
        failure_structs = []
        total_weight = 0

        # ---------------------------------------------------------
        # 1. Evaluate each rule into a boolean expression
        # ---------------------------------------------------------
        for r in rules:
            try:
                handler = getattr(self, f"rule_{r['rule']}", None)
                if handler is None:
                    logger.warning(f"Unknown validation rule: {r['rule']}")
                    continue

                expr = handler(annotated, r)  # boolean expr indicating failure
                weight = r.get("weight", 1)
                total_weight += weight

                failure_structs.append(
                    F.when(expr,
                        F.struct(
                            F.lit(r["rule"]).alias("rule"),
                            F.lit(r.get("column")).alias("column"),
                            F.lit(weight).alias("weight"),
                            F.col(r.get("column")).alias("failed_value")
                        )
                    )
                )
            except Exception as e:
                logger.error(f"Validation rule {r['rule']} failed: {str(e)}")
                # Continue with other rules
                continue

        # ---------------------------------------------------------
        # 2. Always create _dq_failures column
        #    CRITICAL: Column is created even when rules is empty to prevent schema drift
        #    Empty array if no rules exist
        # ---------------------------------------------------------
        if failure_structs:
            # Rules exist - create array of failure structs
            annotated = annotated.withColumn("_dq_failures", F.array(*failure_structs))
            annotated = annotated.withColumn(
                "_dq_failures",
                F.expr("filter(_dq_failures, x -> x is not null)")
            )
        else:
            # No rules - create explicit empty array with correct schema
            # This ensures the column exists with proper type for schema consistency
            annotated = annotated.withColumn(
                "_dq_failures",
                F.array().cast(
                    "array<struct<rule:string,column:string,weight:int,failed_value:string>>"
                )
            )

        # ---------------------------------------------------------
        # 3. Always create _dq_fail_weight
        #    Sum of weights from all failed rules (0 if no failures)
        # ---------------------------------------------------------
        annotated = annotated.withColumn(
            "_dq_fail_weight",
            F.expr("aggregate(_dq_failures, 0, (acc, x) -> acc + x.weight)")
        )

        # ---------------------------------------------------------
        # 4. Always create dq_score (CRITICAL for schema consistency)
        #    Quality score from 0-100 (higher = better quality)
        # ---------------------------------------------------------
        if total_weight > 0:
            # Rules exist - calculate score based on failures
            annotated = annotated.withColumn(
                "dq_score",
                ((F.lit(total_weight) - F.col("_dq_fail_weight")) /
                F.lit(total_weight) * 100.0)
            )
        else:
            # No rules defined → perfect score = 100
            # This prevents schema drift between initial load (no rules)
            # and subsequent loads (with rules)
            annotated = annotated.withColumn("dq_score", F.lit(100.0))

        # ---------------------------------------------------------
        # 5. Always create _dq_error
        #    Boolean flag: true if any validation failures exist
        # ---------------------------------------------------------
        annotated = annotated.withColumn(
            "_dq_error",
            F.col("_dq_fail_weight") > 0
        )

        # ---------------------------------------------------------
        # 6. Build errors_df (always exists, may be empty)
        #    FIXED: Make lineage columns conditional
        # ---------------------------------------------------------
        
        # Build select list dynamically
        select_cols = []
        
        # Add lineage/tracking columns if they exist
        if "process_queue_id" in df.columns:
            select_cols.append("process_queue_id")
        if "natural_key_hash" in df.columns:
            select_cols.append("natural_key_hash")
        if "processed_at" in df.columns:
            select_cols.append("processed_at")
        
        # Always add failure details
        select_cols.extend([
            "failure.rule",
            "failure.column",
            "failure.failed_value",
            "failure.weight",
            "dq_score",
            "_dq_error"
        ])
        
        errors_df = (
            annotated
                .filter(F.col("_dq_error") == True)
                .select(
                    *df.columns,  # retain original record
                    F.explode_outer("_dq_failures").alias("failure"),
                    "dq_score",
                    "_dq_error"
                )
                .select(*select_cols)
        )

        # ---------------------------------------------------------
        # 7. Summary dictionary
        # ---------------------------------------------------------
        total_rows = df.count()
        failed_rows = errors_df.select("process_queue_id" if "process_queue_id" in df.columns else F.lit(1)).distinct().count() if errors_df.count() > 0 else 0

        summary = {
            "total_rows": total_rows,
            "failed_rows": failed_rows,
            "failed_pct": (failed_rows / total_rows * 100) if total_rows else 0,
            "rule_count": len(rules),
            "rules_applied": [r["rule"] for r in rules],
            "total_weight": total_weight,
        }

        return summary, annotated, errors_df