# Data Quality Framework - Architecture & Design

**Version:** 1.0
**Last Updated:** 2025-12-14
**Component:** NovaFlow DQEngine

---

## Table of Contents

1. [Overview](#overview)
2. [Solution Architecture](#solution-architecture)
3. [Data Flow](#data-flow)
4. [Design Patterns](#design-patterns)
5. [Integration Points](#integration-points)
6. [Extension & Customization](#extension--customization)
7. [Performance Considerations](#performance-considerations)
8. [Observability](#observability)

---

## Overview

### Purpose

The NovaFlow Data Quality (DQ) Framework provides a declarative, contract-driven approach to data cleansing and validation. It enables data engineers to define quality rules in YAML contracts that are automatically enforced during pipeline execution.

### Key Principles

1. **Declarative Configuration** - Quality rules defined in YAML, not code
2. **Separation of Concerns** - Cleansing (transformation) vs Validation (scoring)
3. **Non-Blocking** - Bad data flows through with quality scores, not dropped
4. **Observable** - Comprehensive tracking of quality metrics and violations
5. **Extensible** - Easy to add custom rules without framework changes

### Design Goals

- **Developer Productivity** - Write rules in YAML, not PySpark code
- **Consistency** - Same DQ rules across all pipelines
- **Traceability** - Full audit trail of quality decisions
- **Performance** - Efficient execution at scale
- **Maintainability** - Centralized rule definitions

---

## Solution Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA CONTRACT (YAML)                     │
│  ┌────────────────────┐        ┌──────────────────────┐        │
│  │ Cleansing Rules    │        │ Validation Rules     │        │
│  │ - trim             │        │ - not_null           │        │
│  │ - upper            │        │ - is_email           │        │
│  │ - standardize_email│        │ - min/max            │        │
│  └────────────────────┘        └──────────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       PIPELINE ORCHESTRATOR                      │
│                                                                  │
│  Read → Lineage → Hash → Dedup → [QUALITY] → Write → Access    │
│                                      ▲                           │
│                                      │                           │
│                               QualityStage                       │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                          DQ ENGINE                               │
│  ┌─────────────────┐              ┌────────────────────┐       │
│  │ apply_cleansing │              │ apply_dq           │       │
│  │ (transforms)    │ ──step 1──>  │ (validates)        │       │
│  └─────────────────┘              └────────────────────┘       │
│         │                                    │                  │
│         │                                    │                  │
│    ┌────▼────┐                          ┌───▼────┐            │
│    │ Modified│                          │Annotated│            │
│    │   Data  │                          │  w/DQ   │            │
│    └─────────┘                          │ Columns │            │
│                                         └─────────┘            │
└─────────────────────────────────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         OUTPUT & TRACKING                        │
│  ┌──────────────┐  ┌────────────────┐  ┌───────────────┐      │
│  │ Clean Data   │  │ DQ Violations  │  │ Pipeline Stats│      │
│  │ + dq_score   │  │ Table          │  │ Table         │      │
│  └──────────────┘  └────────────────┘  └───────────────┘      │
└─────────────────────────────────────────────────────────────────┘
```

### Component Breakdown

#### 1. **Data Contract (YAML)**
- **Purpose:** Declarative definition of quality rules
- **Location:** `samples/data.*.yaml`
- **Sections:**
  - `cleansing_rules` - Data transformations
  - `quality_rules` - Data validations

**Example:**
```yaml
cleansing_rules:
  - rule: trim
    columns: [name, email]
  - rule: standardize_email
    columns: [email]

quality_rules:
  - rule: not_null
    column: customer_id
    weight: 2
  - rule: is_email
    column: email
    weight: 1
```

#### 2. **QualityStage**
- **Purpose:** Pipeline integration point for DQ execution
- **Location:** `nova_framework/pipeline/stages/quality_stage.py`
- **Responsibilities:**
  - Orchestrates DQEngine calls
  - Writes violations to tracking table
  - Records statistics
  - Controls pipeline flow

**Key Methods:**
```python
def execute(self, df: DataFrame) -> DataFrame:
    # 1. Apply cleansing rules
    df_clean = self.engine.apply_cleansing(df, cleansing_rules)

    # 2. Apply validation rules
    summary, df_with_quality, df_errors = self.engine.apply_dq(df_clean, quality_rules)

    # 3. Write violations
    self._write_dq_errors(df_errors)

    # 4. Return data with dq_score
    return df_with_quality
```

#### 3. **DQEngine**
- **Purpose:** Core DQ rule execution engine
- **Location:** `nova_framework/quality/dq.py`
- **Responsibilities:**
  - Execute cleansing rules (modify data)
  - Execute validation rules (score data)
  - Generate violation reports
  - Maintain schema consistency

**Key Classes:**
```python
class DQEngine:
    def apply_cleansing(df, rules) -> DataFrame
    def apply_dq(df, rules) -> (summary, annotated_df, errors_df)

    # 16 cleansing methods
    def clean_trim(df, rule)
    def clean_upper(df, rule)
    ...

    # 35 validation methods
    def rule_not_null(df, rule)
    def rule_is_email(df, rule)
    ...
```

---

## Data Flow

### Pipeline Execution Flow

```
┌──────────────┐
│ Read Stage   │ ──> Raw DataFrame
└──────┬───────┘
       │
       ▼
┌──────────────┐
│Lineage Stage │ ──> Add process_queue_id, processed_at
└──────┬───────┘
       │
       ▼
┌──────────────┐
│Hashing Stage │ ──> Add natural_key_hash, change_key_hash
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Dedup Stage  │ ──> Remove duplicates
└──────┬───────┘
       │
       ▼
╔══════════════╗
║Quality Stage ║ ──> Apply DQ rules (FOCUS OF THIS DOC)
╚══════┬═══════╝
       │
       ▼
┌──────────────┐
│ Write Stage  │ ──> Write to Delta table
└──────┬───────┘
       │
       ▼
┌──────────────┐
│Access Control│ ──> Apply GRANTs/REVOKEs
└──────────────┘
```

### Quality Stage Detailed Flow

```
Input DataFrame (from Dedup Stage)
│
├─> Step 1: Cleansing
│   │
│   ├─> Loop through cleansing_rules
│   │   ├─> clean_trim(df, rule)         ──> Modify columns in-place
│   │   ├─> clean_upper(df, rule)        ──> Modify columns in-place
│   │   └─> clean_standardize_email(...) ──> Modify columns in-place
│   │
│   └─> df_clean (transformed data)
│
├─> Step 2: Validation
│   │
│   ├─> Loop through quality_rules
│   │   ├─> rule_not_null(df, rule)  ──> Boolean expr (True = fail)
│   │   ├─> rule_is_email(df, rule)  ──> Boolean expr (True = fail)
│   │   └─> rule_min(df, rule)       ──> Boolean expr (True = fail)
│   │
│   ├─> Create _dq_failures column (array of failure structs)
│   ├─> Create _dq_fail_weight column (sum of weights)
│   ├─> Create dq_score column (0-100 score)
│   └─> Create _dq_error column (boolean flag)
│
├─> Step 3: Generate Error Report
│   │
│   ├─> Filter rows where _dq_error = true
│   ├─> Explode _dq_failures array
│   └─> df_errors (violations for tracking)
│
├─> Step 4: Write Violations
│   │
│   └─> Insert into dq_errors_table
│       (for monitoring & alerting)
│
└─> Output: df_with_quality
    ├─ Original columns (transformed by cleansing)
    ├─ _dq_failures: array<struct<rule, column, weight, failed_value>>
    ├─ _dq_fail_weight: int
    ├─ dq_score: double (0-100)
    └─ _dq_error: boolean
```

---

## Design Patterns

### 1. **Strategy Pattern - Rule Execution**

Each rule is a strategy that can be plugged in dynamically.

```python
class DQEngine:
    def apply_cleansing(self, df, rules):
        for rule in rules:
            # Strategy pattern: lookup handler by rule name
            handler = getattr(self, f"clean_{rule['rule']}", None)
            if handler:
                df = handler(df, rule)
        return df

    def apply_dq(self, df, rules):
        for rule in rules:
            # Strategy pattern: lookup handler by rule name
            handler = getattr(self, f"rule_{rule['rule']}", None)
            if handler:
                expr = handler(df, rule)
                # Collect failure expressions
```

**Benefits:**
- Easy to add new rules - just add a new method
- No central dispatch logic
- Python's `getattr()` provides dynamic lookup

**Adding a New Rule:**
```python
# 1. Add method to DQEngine
def clean_my_new_rule(self, df, rule):
    """New custom rule."""
    column = rule.get("column")
    # Transform logic here
    return df.withColumn(column, ...)

# 2. Use in contract
cleansing_rules:
  - rule: my_new_rule
    column: some_field
```

### 2. **Builder Pattern - Schema Construction**

DQ columns are always added to maintain schema consistency.

```python
# Always build complete DQ schema
annotated = df

# 1. Build _dq_failures
if failure_structs:
    annotated = annotated.withColumn("_dq_failures", F.array(*failure_structs))
else:
    # Even with no rules, create empty typed array
    annotated = annotated.withColumn(
        "_dq_failures",
        F.array().cast("array<struct<rule:string,column:string,weight:int,failed_value:string>>")
    )

# 2. Build _dq_fail_weight
annotated = annotated.withColumn("_dq_fail_weight", ...)

# 3. Build dq_score
if total_weight > 0:
    annotated = annotated.withColumn("dq_score", ...)
else:
    annotated = annotated.withColumn("dq_score", F.lit(100.0))

# 4. Build _dq_error
annotated = annotated.withColumn("_dq_error", ...)
```

**Key Insight:**
- Schema is **always consistent** regardless of rule presence
- Prevents schema drift between pipeline runs
- No `mergeSchema` overhead in downstream tables

### 3. **Template Method Pattern - Stage Execution**

AbstractStage defines execution template; QualityStage implements specifics.

```python
# AbstractStage (template)
class AbstractStage:
    def run(self, df):
        if self.skip_condition():
            return df

        start_time = datetime.now()
        result_df = self.execute(df)  # Subclass implements
        end_time = datetime.now()

        # Log timing, stats, etc.
        return result_df

# QualityStage (concrete implementation)
class QualityStage(AbstractStage):
    def execute(self, df):
        # Custom DQ logic
        ...

    def skip_condition(self):
        # Never skip - always add dq_score
        return False
```

### 4. **Facade Pattern - QualityStage as Facade**

QualityStage simplifies complex DQEngine interactions.

```python
# Complex subsystem (DQEngine with many methods)
engine = DQEngine()
engine.apply_cleansing(df, rules)
engine.apply_dq(df, rules)
# ... handle errors, stats, logging, etc.

# Simplified facade (QualityStage)
stage = QualityStage(context, stats)
result = stage.execute(df)  # All complexity hidden
```

### 5. **Functional Composition - Validation Rules**

Validation rules return composable boolean expressions.

```python
# Each rule returns a boolean expression
def rule_not_null(self, df, rule):
    return F.col(rule["column"]).isNull()

def rule_is_email(self, df, rule):
    return ~F.col(rule["column"]).rlike(email_pattern)

# Compose into array of failures
failure_structs = []
for rule in rules:
    expr = handler(df, rule)  # Boolean: True = fail
    failure_structs.append(
        F.when(expr, F.struct(...))  # Only add if failed
    )

# Create array column
df = df.withColumn("_dq_failures", F.array(*failure_structs))
```

**Benefits:**
- Lazy evaluation (Spark optimization)
- Composable expressions
- Single-pass execution

---

## Integration Points

### 1. **Contract System Integration**

```python
# Contract loading
from nova_framework.contract import DataContract

contract = DataContract.load(contract_name, env)

# DQ rules accessed from contract
cleansing_rules = contract.cleansing_rules  # List[Dict]
quality_rules = contract.quality_rules      # List[Dict]
```

**Contract Schema:**
```yaml
# Standard contract sections
name: customer_data
schema:
  columns: [...]

# DQ sections
cleansing_rules:
  - rule: trim
    columns: [name]

quality_rules:
  - rule: not_null
    column: customer_id
    weight: 2
```

### 2. **Pipeline Integration**

```python
# Pipeline strategy (e.g., IngestionPipeline)
def build_stages(self):
    return [
        ReadStage(self.context, self.stats),
        LineageStage(self.context, self.stats),
        HashingStage(self.context, self.stats),
        DeduplicationStage(self.context, self.stats),
        QualityStage(self.context, self.stats),  # ← DQ integration
        WriteStage(self.context, self.stats),
        AccessControlStage(self.context, self.stats)
    ]
```

**Key Integration Points:**
- **Input:** DataFrame from DeduplicationStage
- **Output:** DataFrame with DQ columns to WriteStage
- **Side Effects:** Writes to `dq_errors_table`

### 3. **Observability Integration**

```python
# Statistics tracking
self.stats.log_stat("dq_total_rows", total_rows)
self.stats.log_stat("dq_failed_rows", failed_rows)
self.stats.log_stat("dq_failed_pct", failed_pct)

# Violation tracking
df_dq_errors.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{catalog}.{schema}.dq_errors")
```

**Observability Tables:**

**`pipeline_stats` table:**
```
process_queue_id | stat_name        | stat_value
1234             | dq_total_rows    | 10000
1234             | dq_failed_rows   | 150
1234             | dq_failed_pct    | 1.5
```

**`dq_errors` table:**
```
process_queue_id | natural_key_hash | rule      | column      | failed_value | weight | dq_score
1234             | abc123           | not_null  | email       | NULL         | 2      | 66.67
1234             | def456           | is_email  | email       | bad@invalid  | 1      | 75.00
```

---

## Extension & Customization

### Adding Custom Cleansing Rules

**Step 1:** Add method to `DQEngine`

```python
# nova_framework/quality/dq.py

class DQEngine:
    def clean_custom_phone_format(self, df, rule):
        """
        Format phone numbers to (XXX) XXX-XXXX.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' key

        Returns:
            DataFrame with formatted phone column
        """
        col = rule.get("column")
        if not col:
            logger.warning("clean_custom_phone_format: missing 'column'")
            return df

        # Remove all non-digits
        digits_only = F.regexp_replace(F.col(col), "[^0-9]", "")

        # Format as (XXX) XXX-XXXX
        formatted = F.concat(
            F.lit("("),
            F.substring(digits_only, 1, 3),
            F.lit(") "),
            F.substring(digits_only, 4, 3),
            F.lit("-"),
            F.substring(digits_only, 7, 4)
        )

        return df.withColumn(col, formatted)
```

**Step 2:** Use in contract

```yaml
cleansing_rules:
  - rule: custom_phone_format
    column: phone_number
```

**That's it!** The Strategy pattern automatically picks up the new rule.

### Adding Custom Validation Rules

**Step 1:** Add method to `DQEngine`

```python
# nova_framework/quality/dq.py

class DQEngine:
    def rule_is_valid_credit_card(self, df, rule):
        """
        Validate credit card number using Luhn algorithm.

        Args:
            df: Input DataFrame
            rule: Dict with 'column' and 'weight' keys

        Returns:
            Boolean expression (True = failure)
        """
        col = rule["column"]

        # Simplified: check length and digits only
        # Real implementation would use Luhn algorithm
        return (F.length(F.col(col)) != 16) | ~F.col(col).rlike("^[0-9]{16}$")
```

**Step 2:** Use in contract

```yaml
quality_rules:
  - rule: is_valid_credit_card
    column: card_number
    weight: 3
```

### Extending for Multi-Column Rules

**Pattern for rules that operate on multiple columns:**

```python
def rule_sum_equals(self, df, rule):
    """
    Validate that sum of columns equals expected value.

    Example:
        {"rule": "sum_equals", "columns": ["qty1", "qty2"], "expected": 100}
    """
    cols = rule["columns"]
    expected = rule["expected"]

    # Sum all specified columns
    sum_expr = reduce(lambda a, b: a + b, [F.col(c) for c in cols])

    # Check if sum != expected
    return sum_expr != F.lit(expected)
```

**Usage:**
```yaml
quality_rules:
  - rule: sum_equals
    columns: [debit_amount, credit_amount]
    expected: 0  # Debits and credits must balance
    weight: 3
```

---

## Performance Considerations

### 1. **Single-Pass Execution**

All validation rules execute in a single DataFrame pass.

```python
# EFFICIENT: Single pass through data
df_with_dq = df.withColumn("_dq_failures", F.array(
    F.when(expr1, struct1),  # Rule 1
    F.when(expr2, struct2),  # Rule 2
    F.when(expr3, struct3)   # Rule 3
))
```

**Not:**
```python
# INEFFICIENT: Multiple passes (DON'T DO THIS)
df = df.withColumn("rule1_failed", expr1)
df = df.withColumn("rule2_failed", expr2)
df = df.withColumn("rule3_failed", expr3)
```

### 2. **Lazy Evaluation**

Spark optimizes the entire DQ expression tree before execution.

```python
# All expressions are lazy until action (.count(), .write(), etc.)
df_clean = apply_cleansing(df, rules)           # Lazy
df_annotated = apply_dq(df_clean, rules)        # Lazy
df_annotated.write.saveAsTable("target")        # Action - executes all
```

### 3. **Predicate Pushdown**

Filter on `dq_score` early to reduce data volume.

```python
# EFFICIENT: Filter before expensive operations
df_high_quality = df_with_dq.filter("dq_score >= 80")
df_high_quality.write.saveAsTable("gold_table")

# Downstream queries
SELECT * FROM gold_table
WHERE dq_score = 100  -- Pushes down to Parquet
```

### 4. **Column Pruning**

Drop DQ columns if not needed downstream.

```python
# Keep only business columns for final output
df_final = df_with_dq.select(
    "customer_id", "name", "email", "dq_score"  # Drop _dq_failures, etc.
)
```

### 5. **Rule Ordering**

Put cheap validations first, expensive ones last.

```yaml
quality_rules:
  # Cheap - just null checks
  - rule: not_null
    column: customer_id
    weight: 2

  # Medium - simple regex
  - rule: is_email
    column: email
    weight: 1

  # Expensive - complex regex or UDFs
  - rule: custom_complex_validation
    column: data_field
    weight: 1
```

### 6. **Batch Size Considerations**

DQ operates row-by-row (vectorized), so batch size doesn't matter much. However:

- **Large rules list** - More expressions to evaluate per row
- **Complex regex** - Can be expensive for large datasets
- **Window functions** (unique, composite_unique) - Require shuffle

**Optimization:**
- Keep rule count reasonable (<50 rules)
- Use simple patterns when possible
- Avoid uniqueness checks on large datasets if not critical

---

## Observability

### Metrics Tracked

#### Pipeline Stats

```python
# Automatically logged by QualityStage
self.stats.log_stat("dq_total_rows", 10000)
self.stats.log_stat("dq_failed_rows", 150)
self.stats.log_stat("dq_failed_pct", 1.5)
```

**Query Example:**
```sql
SELECT
    process_queue_id,
    stat_value as total_rows
FROM pipeline_stats
WHERE stat_name = 'dq_total_rows'
```

#### DQ Violations

```sql
-- Find most common DQ failures
SELECT
    rule,
    column,
    COUNT(*) as failure_count
FROM dq_errors
WHERE process_queue_id = 1234
GROUP BY rule, column
ORDER BY failure_count DESC
```

### Monitoring Queries

**DQ Health Dashboard:**

```sql
-- Overall DQ health by data contract
SELECT
    data_contract_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN dq_score < 100 THEN 1 ELSE 0 END) as failed_rows,
    AVG(dq_score) as avg_dq_score,
    MIN(dq_score) as worst_dq_score
FROM target_table
GROUP BY data_contract_name
```

**Trending:**

```sql
-- DQ score trend over time
SELECT
    DATE(processed_at) as date,
    AVG(dq_score) as avg_score,
    PERCENTILE(dq_score, 0.95) as p95_score
FROM target_table
GROUP BY DATE(processed_at)
ORDER BY date
```

**Alerting:**

```sql
-- Alert if DQ score drops below threshold
SELECT
    process_queue_id,
    COUNT(*) as bad_rows
FROM target_table
WHERE dq_score < 80
HAVING COUNT(*) > 100  -- Alert if >100 bad rows
```

### Logging

**QualityStage logs:**
```
[QualityStage] Applying 3 cleansing rules
[QualityStage] Applying 5 validation rules
[QualityStage] DQ validation complete: 10000 rows, 150 failed (1.50%)
[QualityStage] Writing 150 DQ errors to violations table
```

**DQEngine logs:**
```python
logger.info(f"Applying {len(rules)} cleansing rules")
logger.debug(f"Rule: {rule['rule']}, Column: {rule.get('column')}")
logger.warning("Unknown cleansing rule: custom_rule")
logger.error(f"Cleansing rule {rule['rule']} failed: {str(e)}")
```

---

## Best Practices

### Contract Design

✅ **DO:**
- Group related rules together
- Use meaningful weights (critical=3, important=2, nice-to-have=1)
- Put cleansing before validation
- Document complex rules with comments

❌ **DON'T:**
- Define 100+ rules in one contract (split into multiple stages if needed)
- Use weight=1 for everything (defeats weighted scoring)
- Duplicate rules across contracts (use inheritance or templates)

### Rule Design

✅ **DO:**
- Use `columns` (list) instead of multiple rules with same logic
- Keep rules simple and focused
- Test rules on sample data first
- Add descriptive comments in contracts

❌ **DON'T:**
- Create overly complex regex patterns (hard to maintain)
- Use UDFs in rules (breaks optimization)
- Have rules with side effects (only cleansing should modify)

### Performance

✅ **DO:**
- Use DQ score to filter downstream
- Drop internal DQ columns (`_dq_*`) if not needed
- Order rules by cost (cheap → expensive)
- Monitor DQ execution time

❌ **DON'T:**
- Run DQ on data already validated (cache scores if reusing)
- Use window functions (unique) on huge datasets
- Ignore DQ performance metrics

### Observability

✅ **DO:**
- Monitor DQ trends over time
- Set up alerts for DQ score drops
- Review DQ errors regularly
- Track rule effectiveness

❌ **DON'T:**
- Ignore DQ violations table
- Set and forget DQ rules
- Alert on every single violation

---

## Schema Consistency Pattern

### The Problem

Without DQ columns always present:

```
Day 1 (no rules):     [id, name, email]
Day 2 (added rules):  [id, name, email, dq_score]  ❌ SCHEMA DRIFT
```

### The Solution

Always add DQ columns, even with empty rules:

```python
# DQEngine.apply_dq()
if rules:
    # Evaluate rules and build dq_score
    ...
else:
    # No rules? Still add dq_score = 100
    df = df.withColumn("dq_score", F.lit(100.0))
    df = df.withColumn("_dq_failures", F.array().cast("array<struct<...>>"))
    ...
```

**Result:**
```
Day 1 (no rules):     [id, name, email, dq_score=100]  ✅
Day 2 (added rules):  [id, name, email, dq_score]      ✅ CONSISTENT
```

### QualityStage Never Skips

```python
def skip_condition(self) -> bool:
    """Never skip - always add dq_score for schema consistency."""
    return False
```

**Benefit:** Downstream tables always have `dq_score` column.

---

## Coding Standards

### Naming Conventions

**Cleansing Rules:**
```python
def clean_<action>(self, df, rule):
    """clean_trim, clean_upper, clean_standardize_email"""
```

**Validation Rules:**
```python
def rule_<validation>(self, df, rule):
    """rule_not_null, rule_is_email, rule_min"""
```

**Return Values:**
- Cleansing: `DataFrame` (modified)
- Validation: `Boolean Expression` (True = failure)

### Error Handling

```python
def clean_example(self, df, rule):
    col = rule.get("column")
    if not col:
        logger.warning("clean_example: missing 'column' in rule")
        return df  # Return unchanged on error

    try:
        # Rule logic
        return df.withColumn(col, ...)
    except Exception as e:
        logger.error(f"Rule failed: {e}")
        return df  # Don't fail pipeline
```

**Principle:** Individual rule failures should not crash the pipeline.

### Documentation

```python
def clean_example(self, df, rule):
    """
    One-line description.

    Args:
        df: Input DataFrame
        rule: Dict with 'column' and 'param' keys

    Returns:
        DataFrame with transformed column

    Example:
        {"rule": "example", "column": "field", "param": "value"}
    """
```

---

## Advanced Patterns

### Conditional Rule Application

Apply rules based on data characteristics:

```yaml
quality_rules:
  - rule: conditional
    expression: "CASE WHEN country = 'US' THEN LENGTH(postal_code) = 5 ELSE TRUE END"
    weight: 1
```

### Composite Validations

Multiple columns working together:

```yaml
quality_rules:
  - rule: composite_unique
    columns: [customer_id, order_date, product_id]
    weight: 3
```

### Dynamic Weights

Weight based on data contract metadata:

```python
# In contract
customProperties:
  critical_columns: [customer_id, email]

# In code
weight = 3 if column in contract.critical_columns else 1
```

---

## Troubleshooting

### Common Issues

**Issue:** Rules not applied
- **Check:** Rule name spelling (case-sensitive)
- **Check:** Required parameters present
- **Check:** Contract YAML syntax valid

**Issue:** Low DQ scores unexpectedly
- **Check:** DQ errors table for specific failures
- **Check:** Rule weights - are they too high?
- **Check:** Sample data vs rule expectations

**Issue:** Performance degradation
- **Check:** Number of rules (>50 is high)
- **Check:** Regex complexity
- **Check:** Window functions (unique rules)

**Issue:** Schema drift
- **Check:** QualityStage.skip_condition() returns False
- **Check:** DQEngine always adds columns

---

## References

- **Rule Reference:** `/docs/DQ_Rules_Reference.md`
- **Source Code:** `nova_framework/quality/dq.py`
- **Stage Implementation:** `nova_framework/pipeline/stages/quality_stage.py`
- **Sample Contracts:** `/samples/data.*.yaml`

---

## Appendix: DQ Columns Reference

### Columns Added by DQEngine

| Column | Type | Purpose | Visibility |
|--------|------|---------|------------|
| `dq_score` | double | Quality score 0-100 (higher=better) | Public - use in queries |
| `_dq_failures` | array\<struct\> | Array of failure details | Internal - for debugging |
| `_dq_fail_weight` | int | Sum of failed rule weights | Internal - calculation |
| `_dq_error` | boolean | Quick flag: any failures? | Internal - filtering |

### DQ Violations Table Schema

```sql
CREATE TABLE dq_errors (
    process_queue_id BIGINT,
    natural_key_hash STRING,
    processed_at TIMESTAMP,
    rule STRING,
    column STRING,
    failed_value STRING,
    weight INT,
    dq_score DOUBLE,
    _dq_error BOOLEAN,
    data_contract_name STRING
)
```

---

**Document Version:** 1.0
**Last Updated:** 2025-12-14
**Maintained By:** Data Platform Team
