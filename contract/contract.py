from pyspark.sql import functions as F
from pyspark.sql import Window


class DQEngine:
    """
    Databricks-native rule engine supporting:
    - cleansing rules (modify data)
    """

    def __init__(self):
        pass

    # ================================================================
    # MAIN EXECUTION LAYER
    # ================================================================
    def apply_cleansing(self, df, cleansing_rules):
        """
        Apply cleansing transformations to the DF.
        Rules DO modify the data.
        """
        cleansed_df = df

        for rule in cleansing_rules:
            rtype = rule["rule"]
            handler = getattr(self, f"clean_{rtype}", None)
            if handler is None:
                raise ValueError(f"Unsupported cleansing rule: {rtype}")

            cleansed_df = handler(cleansed_df, rule)

        return cleansed_df

    def apply_dq(self, df, dq_rules):
        annotated_df = df
        error_cols = []
        weights = []

        # ----------------------------------------
        # Apply validation rules + track weights
        # ----------------------------------------
        for rule in dq_rules:
            rtype = rule["rule"]
            handler = getattr(self, f"rule_{rtype}", None)
            if handler is None:
                raise ValueError(f"Unsupported DQ rule: {rtype}")

            weight = rule.get("weight", 1)
            weights.append(weight)

            annotated_df, err_col = handler(annotated_df, rule)
            error_cols.append((err_col, weight))

        total_weight = sum(w for _, w in error_cols)

        # ----------------------------------------
        # Convert error booleans → 1 = fail, 0 = pass
        # ----------------------------------------
        for err_col, weight in error_cols:
            annotated_df = annotated_df.withColumn(
                f"{err_col}_int",
                F.when(F.col(err_col), weight).otherwise(0)
            )

        # ----------------------------------------
        # Weighted sum of failures
        # ----------------------------------------
        annotated_df = annotated_df.withColumn(
            "_dq_fail_weight",
            sum([F.col(f"{c}_int") for c, _ in error_cols])
        )

        # ----------------------------------------
        # Weighted score:
        # dq_score = (sum of weights passed / sum(weights)) * 100
        # ----------------------------------------
        annotated_df = annotated_df.withColumn(
            "dq_score",
            ((F.lit(total_weight) - F.col("_dq_fail_weight")) / F.lit(total_weight) * 100)
            .cast("double")
        )

        # ----------------------------------------
        # Roll up simple error flag
        # ----------------------------------------
        annotated_df = annotated_df.withColumn(
            "_dq_error",
            (F.col("_dq_fail_weight") > 0)
        )

        df_errors = annotated_df.filter(F.col("_dq_error") == True)

        total_rows = df.count()
        failed_rows = df_errors.count()

        summary = {
            "total_rows": total_rows,
            "failed_rows": failed_rows,
            "failed_pct": failed_rows / total_rows if total_rows > 0 else 0,
            "rule_count": len(dq_rules),
            "rules_applied": [r["rule"] for r in dq_rules],
            "total_weight": total_weight
        }

        return summary, annotated_df, df_errors

    # ================================================================
    # CLEANSING RULES (modify data)
    # ================================================================

    def clean_trim(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.trim(F.col(col)))

    def clean_upper(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.upper(F.col(col)))

    def clean_lower(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.lower(F.col(col)))

    def clean_fill_null(self, df, rule):
        col = rule["column"]
        value = rule.get("value", "")
        return df.withColumn(col, F.when(F.col(col).isNull(), value).otherwise(F.col(col)))

    def clean_regex_replace(self, df, rule):
        col = rule["column"]
        pattern = rule["pattern"]
        replacement = rule.get("replacement", "")
        return df.withColumn(col, F.regexp_replace(F.col(col), pattern, replacement))

    def clean_nullify_empty_strings(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.when(F.col(col) == "", None).otherwise(F.col(col)))


    def clean_collapse_whitespace(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.regexp_replace(F.col(col), r"\s+", " "))


    def clean_normalize_boolean_values(self, df, rule):
        col = rule["column"]
        return df.withColumn(
            col,
            F.when(F.lower(F.col(col)).isin("y","yes","t","true","1"), "Y")
            .when(F.lower(F.col(col)).isin("n","no","f","false","0"), "N")
            .otherwise(F.col(col))
        )


    def clean_remove_control_characters(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.regexp_replace(F.col(col), r"[\x00-\x1F\x7F]", ""))


    def clean_strip_non_ascii(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.regexp_replace(F.col(col), r"[^\x00-\x7F]", ""))


    def clean_convert_numeric_strings(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.regexp_replace(F.col(col), r"[^0-9\.-]", ""))


    def clean_replace_invalid_numeric(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.col(col).cast("double"))


    def clean_parse_dates(self, df, rule):
        col = rule["column"]
        fmt = rule.get("format", "yyyy-MM-dd")
        return df.withColumn(col, F.to_date(F.col(col), fmt))


    def clean_json_strings(self, df, rule):
        col = rule["column"]
        return df.withColumn(col, F.regexp_replace(F.col(col), r"\\+", ""))

    # ================================================================
    # DQ RULES (evaluate data)
    # ================================================================

    def rule_not_null(self, df, rule):
        col = rule["column"]
        err = f"_err_not_null_{col}"
        return df.withColumn(err, F.col(col).isNull()), err

    def rule_regex(self, df, rule):
        col = rule["column"]
        pattern = rule["pattern"]
        err = f"_err_regex_{col}"
        return df.withColumn(err, ~F.col(col).rlike(pattern)), err

    def rule_allowed_values(self, df, rule):
        col = rule["column"]
        values = rule["values"]
        err = f"_err_allowed_{col}"
        return df.withColumn(err, ~F.col(col).isin(values)), err

    def rule_min(self, df, rule):
        col = rule["column"]
        min_val = rule["value"]
        err = f"_err_min_{col}"
        return df.withColumn(err, F.col(col) < min_val), err

    def rule_max(self, df, rule):
        col = rule["column"]
        max_val = rule["value"]
        err = f"_err_max_{col}"
        return df.withColumn(err, F.col(col) > max_val), err

    def rule_unique(self, df, rule):
        col = rule["column"]
        err = f"_err_unique_{col}"
        win = Window.partitionBy(col)
        dup = (F.count(col).over(win) > 1)
        return df.withColumn(err, dup), err

    def rule_conditional(self, df, rule):
        expr = rule["expression"]
        err = f"_err_cond_{abs(hash(expr))}"
        return df.withColumn(err, F.expr(expr)), err

    def rule_not_blank(self, df, rule):
        col = rule["column"]
        err = f"_err_not_blank_{col}"
        return df.withColumn(err, F.trim(F.col(col)) == ""), err


    def rule_min_length(self, df, rule):
        col = rule["column"]
        err = f"_err_min_length_{col}"
        return df.withColumn(err, F.length(F.col(col)) < rule["value"]), err


    def rule_max_length(self, df, rule):
        col = rule["column"]
        err = f"_err_max_length_{col}"
        return df.withColumn(err, F.length(F.col(col)) > rule["value"]), err


    def rule_digits_only(self, df, rule):
        col = rule["column"]
        err = f"_err_digits_only_{col}"
        return df.withColumn(err, ~F.col(col).rlike("^[0-9]+$")), err


    def rule_letters_only(self, df, rule):
        col = rule["column"]
        err = f"_err_letters_only_{col}"
        return df.withColumn(err, ~F.col(col).rlike("^[A-Za-z]+$")), err


    def rule_between(self, df, rule):
        col = rule["column"]
        err = f"_err_between_{col}"
        return df.withColumn(err,
            ~((F.col(col) >= rule["min"]) & (F.col(col) <= rule["max"]))
        ), err


    def rule_is_number(self, df, rule):
        col = rule["column"]
        err = f"_err_is_number_{col}"
        return df.withColumn(err, F.col(col).cast("double").isNull()), err


    def rule_is_date(self, df, rule):
        col = rule["column"]
        fmt = rule.get("format", "yyyy-MM-dd")
        err = f"_err_is_date_{col}"
        return df.withColumn(err, F.to_date(F.col(col), fmt).isNull()), err


# ================================================================
# PROMOTION GATE
# ================================================================
def evaluate_promotion_gate(summary, threshold="error"):
    failed = summary["failed_rows"]

    if failed == 0:
        return {"status": "PASS"}

    if threshold == "block":
        return {"status": "BLOCK", "failed": failed}

    if threshold == "error" and failed > 0:
        return {"status": "ERROR", "failed": failed}

    return {"status": "WARN", "failed": failed}