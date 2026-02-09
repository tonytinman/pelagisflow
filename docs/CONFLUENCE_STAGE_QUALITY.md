# NovaFlow Pipeline Stage: Quality (Data Quality)

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**: [Pipeline Stages Overview](CONFLUENCE_PIPELINE_STAGES_OVERVIEW.md)

---

## Overview

The **QualityStage** applies data quality rules in two phases: **cleansing** (modifying data to fix known issues) and **validation** (checking data against quality rules and scoring each row). Validation errors are written to a separate DQ violations table for monitoring.

| Property | Value |
|----------|-------|
| **Class** | `QualityStage` |
| **File** | `nova_framework/pipeline/stages/quality_stage.py` |
| **Stage Name** | `DataQuality` |
| **Pipeline Position** | After Deduplication (before Write) |
| **Skip Condition** | No `cleansing_rules` AND no `quality_rules` in contract |
| **Modifies DataFrame** | Yes — cleanses data and adds `dq_score` column |

---

## Two-Phase Execution

### Phase 1: Cleansing (Data Modification)

Cleansing rules **modify** the data to fix known issues. Runs first so validation operates on clean data.

```yaml
cleansing_rules:
  - rule: trim
    columns: [customer_name, email]
  - rule: uppercase
    columns: [country_code]
  - rule: replace_null
    columns: [phone]
    replacement: "UNKNOWN"
```

### Phase 2: Validation (Quality Scoring)

Validation rules **check** data quality and assign a `dq_score` to each row.

```yaml
quality_rules:
  - rule: not_null
    severity: critical
    columns: [customer_id, customer_name]
  - rule: unique
    severity: warning
    columns: [email]
  - rule: range
    severity: critical
    columns: [age]
    min: 0
    max: 150
```

---

## How It Works

```
Input DataFrame
        │
        ▼
┌─────────────────────┐
│ Phase 1: Cleansing  │ ← Modifies data (trim, uppercase, etc.)
│ engine.apply_       │
│ cleansing()         │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Phase 2: Validation │ ← Checks quality rules
│ engine.apply_dq()   │
│                     │ Returns:
│ → summary dict      │   - summary statistics
│ → df_with_quality   │   - DataFrame + dq_score
│ → df_dq_errors      │   - DQ error rows
└─────────┬───────────┘
          │
          ├──► DQ errors → violations table (side-effect, non-fatal)
          │
          ▼
  Return df_with_quality (with dq_score column)
```

### Columns Added

| Column | Type | Description |
|--------|------|-------------|
| `dq_score` | `integer` | 100 if the row passes all rules, < 100 proportional to failures |

---

## DQ Error Writing

When validation errors are found, they are written to a separate DQ violations table:

```
{catalog}.{config.observability.dq_errors_table}
```

Each error row includes the `data_contract_name` for traceability.

**This write is non-fatal**: if writing DQ errors fails, the error is logged but the pipeline continues. The main DataFrame is not affected.

---

## Statistics Logged

| Metric | Description |
|--------|-------------|
| `dq_total_rows` | Total rows checked |
| `dq_failed_rows` | Rows that failed at least one rule |
| `dq_failed_pct` | Percentage of failed rows |
| `stage_DataQuality_duration_seconds` | Execution time |

---

## Contract Configuration

```yaml
# Cleansing (optional)
cleansing_rules:
  - rule: trim
    columns: [name, email]
  - rule: uppercase
    columns: [country_code]

# Validation (optional)
quality_rules:
  - rule: not_null
    severity: critical
    columns: [customer_id, customer_name]
  - rule: unique
    severity: warning
    columns: [email]
```

If neither `cleansing_rules` nor `quality_rules` are defined, the stage is skipped.

---

## Example Log Output

```
[DataQuality] Starting...
Applying 2 cleansing rules
Applying 3 validation rules
DQ validation complete: 10000 rows, 150 failed (1.50%)
Writing 150 DQ errors to violations table
Successfully wrote 150 DQ errors
[DataQuality] Completed in 3.21s
```

### When No Rules Defined

```
[DataQuality] Skipped (skip condition met)
Skipping QualityStage - no cleansing or validation rules defined
```

---

## Error Handling

| Scenario | Behaviour |
|----------|-----------|
| Cleansing rule fails | Exception propagates — pipeline fails |
| Validation rule fails | DQ score reflects failure, but pipeline continues |
| Writing DQ errors to violations table fails | Error logged, pipeline continues |

---

_This is a living document. Last updated: February 2026_
