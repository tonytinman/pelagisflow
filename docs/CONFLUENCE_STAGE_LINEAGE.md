# NovaFlow Pipeline Stage: Lineage

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**: [Pipeline Stages Overview](CONFLUENCE_PIPELINE_STAGES_OVERVIEW.md)

---

## Overview

The **LineageStage** adds provenance tracking columns to every row in the DataFrame. These columns enable full traceability of where each row originated and when it was processed.

| Property | Value |
|----------|-------|
| **Class** | `LineageStage` |
| **File** | `nova_framework/pipeline/stages/lineage_stage.py` |
| **Stage Name** | `Lineage` |
| **Pipeline Position** | After Read (ingestion) or after Transformation |
| **Skip Condition** | Never skipped |
| **Modifies DataFrame** | Yes — adds 5 columns |

---

## Columns Added

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `source_file_path` | `string` | `_metadata.file_path` | Full path to the source file the row came from |
| `source_file_name` | `string` | `_metadata.file_name` | Filename only (without directory path) |
| `process_queue_id` | `integer` | `context.process_queue_id` | Unique identifier for this pipeline execution |
| `processed_at` | `timestamp` | `current_timestamp()` | When the row was processed |
| `processed_by` | `string` | Literal `"nova_framework"` | System that processed the data |

---

## How It Works

1. Delegates to `LineageProcessor.add_standard_lineage(df, process_queue_id)`
2. The processor adds all 5 columns via Spark column expressions
3. Returns the enriched DataFrame

---

## Why Lineage Matters

Lineage columns answer critical questions during debugging and auditing:

| Question | Column |
|----------|--------|
| Which file did this row come from? | `source_file_path`, `source_file_name` |
| When was this row loaded? | `processed_at` |
| Which pipeline run loaded this row? | `process_queue_id` |
| Which system produced this row? | `processed_by` |

### Example Query

```sql
-- Find all rows loaded from a specific file
SELECT * FROM bronze_sales.customers
WHERE source_file_name = 'customers_20260209.parquet'

-- Find all rows loaded in a specific pipeline run
SELECT * FROM bronze_sales.customers
WHERE process_queue_id = 12345
```

---

## Example Log Output

```
[Lineage] Starting...
Adding lineage tracking columns
Lineage columns added successfully
[Lineage] Completed in 0.12s
```

---

_This is a living document. Last updated: February 2026_
