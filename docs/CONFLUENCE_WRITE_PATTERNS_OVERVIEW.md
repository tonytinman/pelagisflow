# NovaFlow Write Patterns

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**:
- [NovaFlow Overview](#)
- [Data Contracts Guide](#)
- [Overwrite Write Pattern](CONFLUENCE_WRITE_PATTERN_OVERWRITE.md)
- [Append Write Pattern](CONFLUENCE_WRITE_PATTERN_APPEND.md)
- [Type 2 Change Log (T2CL) Write Pattern](CONFLUENCE_WRITE_PATTERN_T2CL.md)
- [SCD Type 2 Write Pattern](CONFLUENCE_WRITE_PATTERN_SCD2.md)
- [SCD Type 4 Write Pattern](CONFLUENCE_WRITE_PATTERN_SCD4.md)
- [File Export Write Pattern](CONFLUENCE_WRITE_PATTERN_FILE_EXPORT.md)

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Strategy Comparison](#strategy-comparison)
4. [How Write Strategy Selection Works](#how-write-strategy-selection-works)
5. [Data Preparation Requirements](#data-preparation-requirements)
6. [Decision Guide: Which Pattern to Use](#decision-guide)
7. [Contract Configuration](#contract-configuration)
8. [Observability and Statistics](#observability-and-statistics)
9. [Getting Help](#getting-help)

---

## Overview {#overview}

NovaFlow provides **six write patterns** that cover every data persistence scenario in a modern lakehouse. Each pattern is implemented as a pluggable strategy selected through the data contract, ensuring teams configure their write behaviour declaratively rather than writing custom persistence logic.

### Key Principles

- **Contract-Driven**: The `writeStrategy` field in the data contract determines how data is written. No code changes needed.
- **Factory Pattern**: `IOFactory` creates the correct writer at runtime based on contract configuration.
- **Statistics Built-In**: Every writer returns a structured statistics dictionary and logs metrics to `PipelineStats`.
- **Delta Lake Native**: All table-based patterns use Delta Lake format on Databricks for ACID transactions, time travel, and schema evolution.
- **Idempotent by Design**: Each pattern is designed to produce correct results when re-run with the same input.

---

## Architecture {#architecture}

```
                    ┌───────────────────────────┐
                    │       Data Contract        │
                    │  writeStrategy: scd2       │
                    │  softDelete: true          │
                    └─────────┬─────────────────┘
                              │
                    ┌─────────▼─────────────────┐
                    │       WriteStage           │
                    │  (Pipeline Orchestration)  │
                    └─────────┬─────────────────┘
                              │
                    ┌─────────▼─────────────────┐
                    │       IOFactory            │
                    │  create_writer_from_       │
                    │  contract()                │
                    └─────────┬─────────────────┘
                              │
          ┌───────┬───────┬───┴───┬───────┬───────┐
          ▼       ▼       ▼       ▼       ▼       ▼
     Overwrite Append   T2CL    SCD2    SCD4   FileExport
      Writer   Writer  Writer  Writer  Writer   Writer
          │       │       │       │       │       │
          ▼       ▼       ▼       ▼       ▼       ▼
       Delta   Delta   Delta   Delta   Delta    File
       Table   Table   Table   Table   Tables   System
```

### Base Writer Contract

All writers extend `AbstractWriter` and implement a single method:

```python
def write(self, df: DataFrame, **kwargs) -> Dict[str, Any]
```

Every writer receives:
- `ExecutionContext` — contains the data contract, catalog, and pipeline state
- `PipelineStats` — metrics collector for observability

Every writer returns a **statistics dictionary** with at minimum `strategy` and row count fields.

---

## Strategy Comparison {#strategy-comparison}

| Strategy | Contract Value | Use Case | History | Surrogate Key | Materialized View | Tables Created | Performance |
|----------|---------------|----------|---------|---------------|-------------------|----------------|-------------|
| **Overwrite** | `overwrite` | Reference data, full refresh | No | No | No | 1 | Fast |
| **Append** | `append` | Fact tables, event logs | Implicit | No | No | 1 | Fast |
| **T2CL** | `type_2_change_log` | Dimensions with full history | Yes | No | Yes (`_curr`) | 1 + MV | Medium |
| **SCD2** | `scd2` | Star schema dimensions | Yes | Yes (BIGINT IDENTITY) | Yes (`_curr`) | 1 + MV | Medium |
| **SCD4** | `scd4` | Large dimensions needing fast current queries | Yes | No | Yes (via T2CL) | 2 + MV | Medium |
| **File Export** | `file_export` | External system integration | N/A | No | No | 0 (file only) | Varies |

### Metadata Columns Added

| Strategy | effective_from | effective_to | is_current | deletion_flag | sk (surrogate) |
|----------|---------------|-------------|------------|---------------|----------------|
| Overwrite | — | — | — | — | — |
| Append | — | — | — | — | — |
| T2CL | DATE | DATE | BOOLEAN | BOOLEAN | — |
| SCD2 | DATE | DATE | BOOLEAN | BOOLEAN | BIGINT (IDENTITY) |
| SCD4 | DATE (history only) | DATE (history only) | BOOLEAN (history only) | BOOLEAN (history only) | — |
| File Export | — | — | — | — | — |

---

## How Write Strategy Selection Works {#how-write-strategy-selection-works}

### Contract-Driven (Recommended)

The `WriteStage` in the pipeline reads `customProperties.writeStrategy` from the data contract and delegates to `IOFactory`:

```yaml
# Data contract
customProperties:
  writeStrategy: type_2_change_log   # ← determines the writer
  softDelete: true                   # ← passed to the writer
```

```python
# Pipeline code — no strategy logic needed
writer = IOFactory.create_writer_from_contract(context, stats)
write_stats = writer.write(df)
```

If `writeStrategy` is not specified, it **defaults to `type_2_change_log`**.

### Explicit Selection

For ad-hoc or testing scenarios, a writer can be created directly:

```python
writer = IOFactory.create_writer("overwrite", context, stats)
write_stats = writer.write(df, optimize=True)
```

### Available Strategy Values

| Contract Value | Writer Class |
|----------------|-------------|
| `overwrite` | `OverwriteWriter` |
| `append` | `AppendWriter` |
| `type_2_change_log` | `T2CLWriter` |
| `scd2` | `SCD2Writer` |
| `scd4` | `SCD4Writer` |
| `file_export` | `FileExportWriter` |

---

## Data Preparation Requirements {#data-preparation-requirements}

### Hash Columns (Required for T2CL, SCD2, SCD4)

Before writing with any history-tracking pattern, the DataFrame must contain three columns produced by the `HashingStage`:

| Column | Purpose | Source |
|--------|---------|--------|
| `natural_key_hash` | Identifies unique entities across loads | SHA-256 of all `isPrimaryKey: true` columns |
| `change_key_hash` | Detects attribute changes between loads | SHA-256 of all `isChangeTracking: true` columns |
| `partition_key` | Distributes data across partitions | Modulo hash of natural key |

These are configured in the data contract schema:

```yaml
schema:
  properties:
    - name: customer_id
      type: bigint
      isPrimaryKey: true           # → included in natural_key_hash
    - name: customer_name
      type: string
      isChangeTracking: true       # → included in change_key_hash
    - name: email
      type: string
      isChangeTracking: true       # → included in change_key_hash
```

### Overwrite, Append, and File Export

These patterns have **no special column requirements**. They write the DataFrame as-is.

---

## Decision Guide: Which Pattern to Use {#decision-guide}

```
Is this a full refresh with no history needed?
  ├─ YES → Overwrite
  └─ NO
      │
      Is this append-only (events, facts, logs)?
        ├─ YES → Append
        └─ NO
            │
            Do you need change history with effective dates?
              ├─ YES
              │   │
              │   Do you need surrogate keys for star schema joins?
              │     ├─ YES → SCD2
              │     └─ NO
              │         │
              │         Is the dimension large and needs fast current-state queries?
              │           ├─ YES → SCD4 (current + history tables)
              │           └─ NO  → T2CL (single table with history)
              └─ NO
                  │
                  Is this an export to files for external systems?
                    ├─ YES → File Export
                    └─ NO  → T2CL (default, safest choice)
```

### Quick Reference

| Scenario | Pattern |
|----------|---------|
| Small lookup table refreshed daily | Overwrite |
| Transaction/event log growing over time | Append |
| Customer dimension with audit history | T2CL |
| Product dimension in a star schema (needs SK for joins) | SCD2 |
| Large supplier dimension (millions of rows, need fast current queries) | SCD4 |
| Daily CSV extract for a downstream system | File Export |

---

## Contract Configuration {#contract-configuration}

### Minimal Contract (Defaults to T2CL)

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.sales.customers

schema:
  name: bronze_sales
  table: customers
  properties:
    - name: customer_id
      type: bigint
      isPrimaryKey: true
    - name: customer_name
      type: string
      isChangeTracking: true

customProperties:
  pipelineType: ingestion
  # writeStrategy defaults to type_2_change_log
```

### Full Contract with All Write Options

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.sales.customers

schema:
  name: bronze_sales
  table: customers
  properties:
    - name: customer_id
      type: bigint
      isPrimaryKey: true
    - name: customer_name
      type: string
      isChangeTracking: true
    - name: email
      type: string
      isChangeTracking: true

customProperties:
  pipelineType: ingestion
  writeStrategy: scd2             # overwrite | append | type_2_change_log | scd2 | scd4 | file_export
  softDelete: true                # Enable soft delete detection (T2CL, SCD2)
  volume: M                       # Affects partition count
```

---

## Observability and Statistics {#observability-and-statistics}

Every writer logs metrics to `PipelineStats` and returns a statistics dictionary. The `WriteStage` also logs strategy-specific messages.

### Common Metrics (All Writers)

| Metric | Description |
|--------|-------------|
| `rows_written` | Total rows persisted |
| `write_strategy_{name}` | Row count tagged by strategy |

### Strategy-Specific Metrics

| Strategy | Additional Metrics |
|----------|-------------------|
| T2CL | `t2cl_new`, `t2cl_changed`, `t2cl_deleted` |
| SCD2 | `scd2_new`, `scd2_changed`, `scd2_deleted`, `scd2_max_sk` |
| Append | `rows_deduped` |
| Overwrite | — |
| SCD4 | Inherits T2CL metrics for historical table |
| File Export | — |

### Example: Accessing Write Stats in Pipeline

```python
# After WriteStage.execute(df)
write_stats = context.get_state("write_stats")

print(f"Strategy: {write_stats['strategy']}")
print(f"New records: {write_stats.get('new_records', 'N/A')}")
print(f"Changed records: {write_stats.get('changed_records', 'N/A')}")
```

---

## Getting Help {#getting-help}

### Documentation

- [Overwrite Pattern — Full Details](CONFLUENCE_WRITE_PATTERN_OVERWRITE.md)
- [Append Pattern — Full Details](CONFLUENCE_WRITE_PATTERN_APPEND.md)
- [T2CL Pattern — Full Details](CONFLUENCE_WRITE_PATTERN_T2CL.md)
- [SCD2 Pattern — Full Details](CONFLUENCE_WRITE_PATTERN_SCD2.md)
- [SCD4 Pattern — Full Details](CONFLUENCE_WRITE_PATTERN_SCD4.md)
- [File Export Pattern — Full Details](CONFLUENCE_WRITE_PATTERN_FILE_EXPORT.md)
- [Data Privacy Control](CONFLUENCE_DATA_PRIVACY.md)
- [Engineering Standards Checklist](ENGINEERING_STANDARDS_CHECKLIST.md)

### Support Channels

- **Slack**: #novaflow-support
- **Email**: novaflow-team@company.com
- **Office Hours**: Tuesdays & Thursdays 2-3pm GMT

---

_This is a living document. Last updated: February 2026_
