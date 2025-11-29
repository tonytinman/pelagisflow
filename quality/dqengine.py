from pyspark.sql import functions as F

from pyspark.sql import Window

 

class DQEngine:

    """

    Nova Data Quality Engine

    - Cleansing rules modify data

    - Evaluation rules annotate data without modifying original values

    """

 

    def __init__(self):

        pass

 

    # =========================================================================

    # HELPERS

    # =========================================================================

 

    def _safe_cast_to_double(self, col):

        cleaned = F.regexp_replace(F.col(col), "[^0-9.+-]", "")

        return F.when(

            cleaned.rlike("^[+-]?([0-9]*[.])?[0-9]+$"),

            cleaned.cast("double")

        ).otherwise(F.lit(None))

 

    # =========================================================================

    # CLEANSING (TRANSFORMATIONS)

    # =========================================================================

 

    def clean_trim(self, df, rule):

        return df.withColumn(rule["column"], F.trim(F.col(rule["column"])))

 

    def clean_upper(self, df, rule):

        return df.withColumn(rule["column"], F.upper(F.col(rule["column"])))

 

    def clean_lower(self, df, rule):

        return df.withColumn(rule["column"], F.lower(F.col(rule["column"])))

 

    def clean_regex_replace(self, df, rule):

        return df.withColumn(

            rule["column"],

            F.regexp_replace(F.col(rule["column"]), rule["pattern"], rule.get("replacement", ""))

        )

 

    def clean_nullify_empty_strings(self, df, rule):

        return df.withColumn(

            rule["column"],

            F.when(F.trim(F.col(rule["column"])) == "", None).otherwise(F.col(rule["column"]))

        )

 

    def clean_normalize_boolean_values(self, df, rule):

       

        """Map truthy/falsy forms into True/False."""

        col = rule["column"]

        print(f'cleaning: {rule}')

        return df.withColumn(

            col,

            F.when(F.lower(F.col(col)).isin("1", "t", "true", "yes", "y"), "True")

             .when(F.lower(F.col(col)).isin("0", "f", "false","no", "n"), "False")

             .otherwise(F.col(col))

        )

 

    # =========================================================================

    # EVALUATION RULES

    # =========================================================================

 

    def rule_not_null(self, df, rule):

        col = rule["column"]

        return F.col(col).isNull()

 

    def rule_not_blank(self, df, rule):

        col = rule["column"]

        return (F.col(col).isNull()) | (F.trim(F.col(col)) == "")

 

    def rule_regex(self, df, rule):

        col = rule["column"]

        pattern = rule["pattern"]

        return ~F.col(col).rlike(pattern)

 

    def rule_allowed_values(self, df, rule):

        col = rule["column"]

        values = rule["values"]

        return ~F.col(col).isin(values)

 

    def rule_min_length(self, df, rule):

        col = rule["column"]

        min_len = rule["value"]

        return F.length(F.col(col)) < min_len

 

    def rule_max_length(self, df, rule):

        col = rule["column"]

        max_len = rule["value"]

        return F.length(F.col(col)) > max_len

 

    def rule_digits_only(self, df, rule):

        col = rule["column"]

        return ~F.col(col).rlike("^[0-9]+$")

 

    def rule_letters_only(self, df, rule):

        col = rule["column"]

        return ~F.col(col).rlike("^[A-Za-z]+$")

 

    def rule_is_number(self, df, rule):

        col = rule["column"]

        return self._safe_cast_to_double(col).isNull() & F.col(col).isNotNull()

 

    def rule_min(self, df, rule):

        col = rule["column"]

        min_val = rule["value"]

        return self._safe_cast_to_double(col) < F.lit(min_val)

 

    def rule_max(self, df, rule):

        col = rule["column"]

        max_val = rule["value"]

        return self._safe_cast_to_double(col) > F.lit(max_val)

 

    def rule_between(self, df, rule):

        col = rule["column"]

        return ~(

            (self._safe_cast_to_double(col) >= F.lit(rule["min"])) &

            (self._safe_cast_to_double(col) <= F.lit(rule["max"]))

        )

 

    def rule_conditional(self, df, rule):

        expr = rule["expression"]  # SQL expression returning true if FAIL

        return F.expr(expr)

 

    def rule_is_date(self, df, rule):

        col = rule["column"]

        fmt = rule.get("format", "yyyy-MM-dd")

        return F.to_date(F.col(col), fmt).isNull() & F.col(col).isNotNull()

 

    def rule_unique(self, df, rule):

        col = rule["column"]

        return F.count(F.col(col)).over(Window.partitionBy(col)) > 1

 

    def rule_composite_unique(self, df, rule):

        cols = rule["columns"]

        return (

            F.count(F.lit(1)).over(Window.partitionBy(*cols)) > 1

        )

 

    # =========================================================================

    # DQ EXECUTION

    # =========================================================================

 

    def apply_cleansing(self, df, rules):

        for r in rules:

            handler = getattr(self, f"clean_{r['rule']}", None)

            if handler:

                df = handler(df, r)

        return df

 

    def apply_dq(self, df, rules):

        """

        Apply DQ rules without creating a column per rule.

        Ensures schema stability even if no rules exist.

        """

 

        annotated = df

        failure_structs = []

        total_weight = 0

 

        # ---------------------------------------------------------

        # 1. Evaluate each rule into a boolean expression

        # ---------------------------------------------------------

        for r in rules:

            handler = getattr(self, f"rule_{r['rule']}", None)

            if handler is None:

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

        # ---------------------------------------------------------

        errors_df = (

            annotated

                .filter(F.col("_dq_error") == True)

                .select(

                    *df.columns,  # retain original record

                    F.explode_outer("_dq_failures").alias("failure"),

                    "dq_score",

                    "_dq_error"

                )

                .select(

                    "process_queue_id",

                    "natural_key_hash",

                    "processed_at",

                    "failure.rule",

                    "failure.column",

                    "failure.failed_value",

                    "failure.weight",

                    "dq_score",

                    "_dq_error"

                )

        )

 

        # ---------------------------------------------------------

        # 7. Summary dictionary

        # ---------------------------------------------------------

        total_rows = df.count()

        failed_rows = errors_df.count()

 

        summary = {

            "total_rows": total_rows,

            "failed_rows": failed_rows,

            "failed_pct": failed_rows / total_rows if total_rows else 0,

            "rule_count": len(rules),

            "rules_applied": [r["rule"] for r in rules],

            "total_weight": total_weight,

        }

 

        return summary, annotated, errors_df