"""
Nova Data Quality Engine

Provides data cleansing and validation capabilities for pipeline processing.

- Cleansing rules modify data
- Validation rules annotate data without modifying original values
"""

from functools import reduce
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

    def clean_truncate(self, df, rule):
        """
        Truncate strings to maximum length.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'max_length' keys

        Returns:
            DataFrame with truncated column

        Example:
            {"rule": "truncate", "column": "description", "max_length": 100}
        """
        col = rule.get("column")
        max_length = rule.get("max_length")

        if not col or not max_length:
            logger.warning("clean_truncate: missing 'column' or 'max_length' in rule")
            return df

        return df.withColumn(col, F.substring(F.col(col), 1, max_length))

    def clean_pad_left(self, df, rule):
        """
        Left-pad strings to fixed length (e.g., '5' -> '005').

        Args:
            df: Input DataFrame
            rule: Dict with 'column', 'length', and optional 'pad_char' (default: '0')

        Returns:
            DataFrame with left-padded column

        Example:
            {"rule": "pad_left", "column": "account_id", "length": 10, "pad_char": "0"}
        """
        col = rule.get("column")
        length = rule.get("length")
        pad_char = rule.get("pad_char", "0")

        if not col or not length:
            logger.warning("clean_pad_left: missing 'column' or 'length' in rule")
            return df

        return df.withColumn(col, F.lpad(F.col(col), length, pad_char))

    def clean_pad_right(self, df, rule):
        """
        Right-pad strings to fixed length.

        Args:
            df: Input DataFrame
            rule: Dict with 'column', 'length', and optional 'pad_char' (default: ' ')

        Returns:
            DataFrame with right-padded column

        Example:
            {"rule": "pad_right", "column": "code", "length": 20, "pad_char": " "}
        """
        col = rule.get("column")
        length = rule.get("length")
        pad_char = rule.get("pad_char", " ")

        if not col or not length:
            logger.warning("clean_pad_right: missing 'column' or 'length' in rule")
            return df

        return df.withColumn(col, F.rpad(F.col(col), length, pad_char))

    def clean_title_case(self, df, rule):
        """
        Convert to Title Case (for names, addresses).

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            DataFrame with title-cased column(s)

        Example:
            {"rule": "title_case", "columns": ["first_name", "last_name", "city"]}
        """
        cols = rule.get("columns") or [rule.get("column")]
        for col in cols:
            if col:
                df = df.withColumn(col, F.initcap(F.col(col)))
        return df

    def clean_remove_special_characters(self, df, rule):
        """
        Remove special characters, optionally keeping specific ones.

        Args:
            df: Input DataFrame
            rule: Dict with 'columns' and optional 'keep_chars' (default: '')

        Returns:
            DataFrame with special characters removed

        Example:
            {"rule": "remove_special_characters", "columns": ["name"], "keep_chars": ".-"}
            # Keeps hyphens and periods, removes all other special chars
        """
        import re

        cols = rule.get("columns") or [rule.get("column")]
        keep_chars = rule.get("keep_chars", "")

        # Build pattern: keep alphanumeric, spaces, and specified chars
        pattern = f"[^a-zA-Z0-9\\s{re.escape(keep_chars)}]"

        for col in cols:
            if col:
                df = df.withColumn(col, F.regexp_replace(F.col(col), pattern, ""))
        return df

    def clean_standardize_email(self, df, rule):
        """
        Lowercase and trim emails.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            DataFrame with standardized email column(s)

        Example:
            {"rule": "standardize_email", "columns": ["email", "alternate_email"]}
        """
        cols = rule.get("columns") or [rule.get("column")]
        for col in cols:
            if col:
                df = df.withColumn(col, F.lower(F.trim(F.col(col))))
        return df

    def clean_round_numeric(self, df, rule):
        """
        Round numeric values to specified decimal places.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' and optional 'decimal_places' (default: 2)

        Returns:
            DataFrame with rounded numeric column

        Example:
            {"rule": "round_numeric", "column": "price", "decimal_places": 2}
        """
        col = rule.get("column")
        decimal_places = rule.get("decimal_places", 2)

        if not col:
            logger.warning("clean_round_numeric: missing 'column' in rule")
            return df

        return df.withColumn(col, F.round(F.col(col), decimal_places))

    def clean_replace_nulls(self, df, rule):
        """
        Replace NULL values with default.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'default_value'

        Returns:
            DataFrame with nulls replaced

        Example:
            {"rule": "replace_nulls", "column": "status", "default_value": "UNKNOWN"}
        """
        col = rule.get("column")
        default_value = rule.get("default_value")

        if not col or default_value is None:
            logger.warning("clean_replace_nulls: missing 'column' or 'default_value' in rule")
            return df

        return df.withColumn(col, F.coalesce(F.col(col), F.lit(default_value)))

    def clean_extract_regex(self, df, rule):
        """
        Extract substring matching regex pattern.

        Args:
            df: Input DataFrame
            rule: Dict with 'column', 'pattern', and optional 'group' (default: 0)

        Returns:
            DataFrame with extracted value

        Example:
            # Extract area code from phone: pattern='\\((\\d{3})\\)', group=1
            {"rule": "extract_regex", "column": "phone", "pattern": "\\((\\d{3})\\)", "group": 1}
        """
        col = rule.get("column")
        pattern = rule.get("pattern")
        group = rule.get("group", 0)

        if not col or not pattern:
            logger.warning("clean_extract_regex: missing 'column' or 'pattern' in rule")
            return df

        return df.withColumn(col, F.regexp_extract(F.col(col), pattern, group))

    def clean_remove_accents(self, df, rule):
        """
        Remove accents from characters (é -> e, ñ -> n).

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            DataFrame with accents removed

        Example:
            {"rule": "remove_accents", "columns": ["name", "address"]}
        """
        cols = rule.get("columns") or [rule.get("column")]

        # Character mapping for accent removal
        accented = 'áéíóúàèìòùâêîôûäëïöüãõñçÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÄËÏÖÜÃÕÑÇ'
        unaccented = 'aeiouaeiouaeiouaeiouaoncAEIOUAEIOUAEIOUAEIOUAONC'

        for col in cols:
            if col:
                df = df.withColumn(
                    col,
                    F.expr(f"translate({col}, '{accented}', '{unaccented}')")
                )
        return df

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

    def rule_length_equals(self, df, rule):
        """
        Check if column value length does not equal exact length (for fixed-length codes).

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) and 'length' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "length_equals", "column": "country_code", "length": 2}
        """
        cols = rule.get("columns") or [rule.get("column")]
        length = rule["length"]

        conditions = [F.length(F.col(col)) != length for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_is_email(self, df, rule):
        """
        Check if column value is not a valid email format.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "is_email", "columns": ["email", "alternate_email"]}
        """
        cols = rule.get("columns") or [rule.get("column")]
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

        conditions = [~F.col(col).rlike(email_pattern) for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_is_url(self, df, rule):
        """
        Check if column value is not a valid URL format.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "is_url", "column": "website"}
        """
        cols = rule.get("columns") or [rule.get("column")]
        url_pattern = r"^https?://[^\s/$.?#].[^\s]*$"

        conditions = [~F.col(col).rlike(url_pattern) for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_is_uuid(self, df, rule):
        """
        Check if column value is not a valid UUID format.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "is_uuid", "column": "transaction_id"}
        """
        cols = rule.get("columns") or [rule.get("column")]
        uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"

        conditions = [~F.col(col).rlike(uuid_pattern) for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_is_positive(self, df, rule):
        """
        Check if numeric column value is not positive (> 0).

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "is_positive", "columns": ["quantity", "price"]}
        """
        cols = rule.get("columns") or [rule.get("column")]

        conditions = [self._safe_cast_to_double(col) <= 0 for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_is_negative(self, df, rule):
        """
        Check if numeric column value is not negative (< 0).

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "is_negative", "column": "adjustment"}
        """
        cols = rule.get("columns") or [rule.get("column")]

        conditions = [self._safe_cast_to_double(col) >= 0 for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_is_non_negative(self, df, rule):
        """
        Check if numeric column value is negative (not >= 0).

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "is_non_negative", "columns": ["age", "balance"]}
        """
        cols = rule.get("columns") or [rule.get("column")]

        conditions = [self._safe_cast_to_double(col) < 0 for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_starts_with(self, df, rule):
        """
        Check if column value does not start with specified prefix.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) and 'prefix' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "starts_with", "column": "account_id", "prefix": "ACC-"}
        """
        cols = rule.get("columns") or [rule.get("column")]
        prefix = rule["prefix"]

        conditions = [~F.col(col).startswith(prefix) for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_ends_with(self, df, rule):
        """
        Check if column value does not end with specified suffix.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) and 'suffix' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "ends_with", "column": "filename", "suffix": ".pdf"}
        """
        cols = rule.get("columns") or [rule.get("column")]
        suffix = rule["suffix"]

        conditions = [~F.col(col).endswith(suffix) for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_contains(self, df, rule):
        """
        Check if column value does not contain specified substring.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) and 'substring' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "contains", "column": "description", "substring": "URGENT"}
        """
        cols = rule.get("columns") or [rule.get("column")]
        substring = rule["substring"]

        conditions = [~F.col(col).contains(substring) for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_not_contains(self, df, rule):
        """
        Check if column value contains forbidden substring (blacklist).

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) and 'substring' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "not_contains", "column": "comment", "substring": "REDACTED"}
        """
        cols = rule.get("columns") or [rule.get("column")]
        substring = rule["substring"]

        conditions = [F.col(col).contains(substring) for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_is_future_date(self, df, rule):
        """
        Check if date is not in the future.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) and optional 'format' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "is_future_date", "column": "expiry_date", "format": "yyyy-MM-dd"}
        """
        cols = rule.get("columns") or [rule.get("column")]
        fmt = rule.get("format", "yyyy-MM-dd")

        conditions = [
            F.to_date(F.col(col), fmt) <= F.current_date() for col in cols if col
        ]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_is_past_date(self, df, rule):
        """
        Check if date is not in the past.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) and optional 'format' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "is_past_date", "column": "birth_date", "format": "yyyy-MM-dd"}
        """
        cols = rule.get("columns") or [rule.get("column")]
        fmt = rule.get("format", "yyyy-MM-dd")

        conditions = [
            F.to_date(F.col(col), fmt) >= F.current_date() for col in cols if col
        ]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_date_range(self, df, rule):
        """
        Check if date is not within specified range.

        Args:
            df: Input DataFrame
            rule: Dict with 'column', 'min_date', 'max_date', and optional 'format' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "date_range", "column": "transaction_date", "min_date": "2020-01-01", "max_date": "2025-12-31"}
        """
        col = rule["column"]
        min_date = rule["min_date"]
        max_date = rule["max_date"]
        fmt = rule.get("format", "yyyy-MM-dd")

        return (F.to_date(F.col(col), fmt) < F.lit(min_date)) | \
               (F.to_date(F.col(col), fmt) > F.lit(max_date))

    def rule_greater_than_column(self, df, rule):
        """
        Check if column value is not greater than reference column value.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'reference_column' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "greater_than_column", "column": "end_date", "reference_column": "start_date"}
        """
        col = rule["column"]
        reference_column = rule["reference_column"]

        return F.col(col) <= F.col(reference_column)

    def rule_less_than_column(self, df, rule):
        """
        Check if column value is not less than reference column value.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'reference_column' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "less_than_column", "column": "discount", "reference_column": "price"}
        """
        col = rule["column"]
        reference_column = rule["reference_column"]

        return F.col(col) >= F.col(reference_column)

    def rule_equals_column(self, df, rule):
        """
        Check if column value does not equal reference column value.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'reference_column' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "equals_column", "column": "email_confirmation", "reference_column": "email"}
        """
        col = rule["column"]
        reference_column = rule["reference_column"]

        return F.col(col) != F.col(reference_column)

    def rule_not_in_list(self, df, rule):
        """
        Check if value is in forbidden list (blacklist).

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) and 'forbidden_values' keys

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "not_in_list", "column": "status", "forbidden_values": ["DELETED", "ARCHIVED"]}
        """
        cols = rule.get("columns") or [rule.get("column")]
        forbidden_values = rule["forbidden_values"]

        conditions = [F.col(col).isin(forbidden_values) for col in cols if col]
        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

    def rule_is_valid_json(self, df, rule):
        """
        Check if column value does not contain valid JSON.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' (str) or 'columns' (list) key

        Returns:
            Boolean expression (True = failure)

        Example:
            {"rule": "is_valid_json", "column": "metadata"}
        """
        cols = rule.get("columns") or [rule.get("column")]

        conditions = []
        for col in cols:
            if col:
                # Try to parse JSON; NULL result indicates invalid JSON
                is_invalid = F.from_json(F.col(col), "string").isNull() & F.col(col).isNotNull()
                conditions.append(is_invalid)

        return reduce(lambda a, b: a | b, conditions) if len(conditions) > 1 else conditions[0]

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
        
        Args:
            df: Input DataFrame
            rules: List of validation rule dicts
            
        Returns:
            Tuple of (summary_dict, annotated_df, errors_df)
            
        Example:
            rules = [
                {"rule": "not_null", "column": "customer_id", "weight": 2},
                {"rule": "regex", "column": "email", "pattern": "^[\\w.]+@[\\w.]+$", "weight": 1}
            ]
            summary, df_quality, df_errors = engine.apply_dq(df, rules)
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
        #    Empty array if no rules exist
        # ---------------------------------------------------------
        if failure_structs:
            annotated = annotated.withColumn("_dq_failures", F.array(*failure_structs))
            annotated = annotated.withColumn(
                "_dq_failures",
                F.expr("filter(_dq_failures, x -> x is not null)")
            )
        else:
            # explicit empty array<struct<...>>
            annotated = annotated.withColumn(
                "_dq_failures",
                F.array().cast(
                    "array<struct<rule:string,column:string,weight:int,failed_value:string>>"
                )
            )

        # ---------------------------------------------------------
        # 3. Always create _dq_fail_weight
        # ---------------------------------------------------------
        annotated = annotated.withColumn(
            "_dq_fail_weight",
            F.expr("aggregate(_dq_failures, 0, (acc, x) -> acc + x.weight)")
        )

        # ---------------------------------------------------------
        # 4. Always create dq_score
        # ---------------------------------------------------------
        if total_weight > 0:
            annotated = annotated.withColumn(
                "dq_score",
                ((F.lit(total_weight) - F.col("_dq_fail_weight")) /
                F.lit(total_weight) * 100.0)
            )
        else:
            # No rules → score = 100
            annotated = annotated.withColumn("dq_score", F.lit(100.0))

        # ---------------------------------------------------------
        # 5. Always create _dq_error
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