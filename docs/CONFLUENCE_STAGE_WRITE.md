# NovaFlow Pipeline Stage: Write

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**: [Pipeline Stages Overview](CONFLUENCE_PIPELINE_STAGES_OVERVIEW.md) | [Write Patterns Overview](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md)

---

## Overview

The **WriteStage** persists the DataFrame to a target table (or file) using the write strategy specified in the data contract. It delegates to `IOFactory` to create the appropriate writer and logs strategy-specific metrics.

| Property | Value |
|----------|-------|
| **Class** | `WriteStage` |
| **File** | `nova_framework/pipeline/stages/write_stage.py` |
| **Stage Name** | `Write` |
| **Pipeline Position** | Last data stage |
| **Skip Condition** | Never skipped |
| **Modifies DataFrame** | No — returns the same DataFrame (write is a side-effect) |

---

## How It Works

1. Creates a writer via `IOFactory.create_writer_from_contract(context, stats)`
2. Reads `customProperties.writeStrategy` from contract (default: `type_2_change_log`)
3. Calls `writer.write(df)` which returns a statistics dictionary
4. Stores stats in `context.state["write_stats"]`
5. Logs `rows_written` and strategy-specific metrics
6. Returns the DataFrame unchanged (for potential downstream chaining)

---

## Write Strategies

The write strategy is determined by the `writeStrategy` field in the data contract:

| Contract Value | Writer | Documentation |
|---------------|--------|---------------|
| `type_2_change_log` (default) | `T2CLWriter` | [T2CL Pattern](CONFLUENCE_WRITE_PATTERN_T2CL.md) |
| `scd2` | `SCD2Writer` | [SCD2 Pattern](CONFLUENCE_WRITE_PATTERN_SCD2.md) |
| `scd4` | `SCD4Writer` | [SCD4 Pattern](CONFLUENCE_WRITE_PATTERN_SCD4.md) |
| `overwrite` | `OverwriteWriter` | [Overwrite Pattern](CONFLUENCE_WRITE_PATTERN_OVERWRITE.md) |
| `append` | `AppendWriter` | [Append Pattern](CONFLUENCE_WRITE_PATTERN_APPEND.md) |
| `file_export` | `FileExportWriter` | [File Export Pattern](CONFLUENCE_WRITE_PATTERN_FILE_EXPORT.md) |

---

## Context State

| Key | Written By | Type | Contents |
|-----|-----------|------|----------|
| `write_stats` | WriteStage | `dict` | Strategy-specific statistics (see write pattern pages) |

---

## Strategy-Specific Logging

The stage logs different details depending on the strategy used:

### T2CL / SCD2

```
T2CL write complete: 1780 records written (250 new, 1500 changed, 30 deleted)
to catalog.bronze_sales.customers
```

### SCD4

```
SCD4 write complete: 50000 rows to catalog.bronze_sales.suppliers_current,
history maintained in catalog.bronze_sales.suppliers_history
```

### Append

```
Append write complete: 25000 rows written to catalog.bronze_clickstream.page_views
(150 duplicates removed before append)
```

### Overwrite

```
Overwrite complete: 500 rows written to catalog.bronze_reference.country_codes (optimized)
```

### File Export

```
File export complete: 25000 rows exported to /mnt/exports/data.csv as csv
```

---

## Statistics Logged

| Metric | Description |
|--------|-------------|
| `rows_written` | Total rows persisted |
| `t2cl_new_records` | New records (T2CL strategy only) |
| `t2cl_changed_records` | Changed records (T2CL strategy only) |
| `t2cl_soft_deleted` | Soft-deleted records (T2CL strategy only) |
| `stage_Write_duration_seconds` | Execution time |

---

## Contract Configuration

```yaml
customProperties:
  writeStrategy: type_2_change_log  # or overwrite, append, scd2, scd4, file_export
  softDelete: true                  # Enable soft delete detection (T2CL, SCD2)
```

If `writeStrategy` is not specified, it defaults to `type_2_change_log`.

For detailed documentation on each write strategy, see [Write Patterns Overview](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md).

---

## Example Log Output

```
[Write] Starting...
Starting write operation
Using writer: T2CLWriter
T2CL merge to catalog.bronze_sales.customers (process_date=2026-02-09)
About to insert: new=250, changed=1500, deleted=30, total=1780
T2CL merge complete: new=250, changed=1500, deleted=30
Creating/refreshing materialized view: catalog.mv_bronze_sales.customers_curr
T2CL write complete: 1780 records written (250 new, 1500 changed, 30 deleted)
to catalog.bronze_sales.customers
Write operation completed successfully
[Write] Completed in 5.67s
```

---

_This is a living document. Last updated: February 2026_
