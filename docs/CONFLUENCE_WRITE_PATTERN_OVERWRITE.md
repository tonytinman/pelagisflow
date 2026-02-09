# NovaFlow Write Pattern: Overwrite

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**:
- [Write Patterns Overview](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md)
- [Append Write Pattern](CONFLUENCE_WRITE_PATTERN_APPEND.md)
- [T2CL Write Pattern](CONFLUENCE_WRITE_PATTERN_T2CL.md)
- [Data Contracts Guide](#)

---

## Table of Contents

1. [Overview](#overview)
2. [When to Use](#when-to-use)
3. [How It Works](#how-it-works)
4. [Contract Configuration](#contract-configuration)
5. [Parameters](#parameters)
6. [Return Statistics](#return-statistics)
7. [Schema Evolution](#schema-evolution)
8. [Examples](#examples)
9. [FAQs](#faqs)

---

## Overview {#overview}

The **Overwrite** pattern performs a full table replacement. Every execution drops the existing data and writes the entire incoming DataFrame as the new table content.

| Property | Value |
|----------|-------|
| **Contract Value** | `overwrite` |
| **Writer Class** | `OverwriteWriter` |
| **Delta Write Mode** | `overwrite` |
| **History Tracking** | No |
| **Surrogate Key** | No |
| **Materialized View** | No |
| **Metadata Columns Added** | None |
| **Required Hash Columns** | None |

### When to Use {#when-to-use}

- Reference/lookup tables that are fully refreshed from source each run
- Small dimension tables that can be cheaply rebuilt
- Aggregated summary tables produced by transformation pipelines
- Any table where historical versions are not needed

### When NOT to Use

- Tables where you need to track what changed between loads
- Large fact tables where re-writing all data is expensive
- Any dimension requiring audit history or effective dating

---

## How It Works {#how-it-works}

```
Incoming DataFrame
        │
        ▼
┌───────────────────┐
│ Count rows        │
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│ Write as Delta    │
│ mode: overwrite   │
│ (optional         │
│  partitioning)    │
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│ OPTIMIZE table    │  ← optional, enabled by default
│ (compacts files)  │
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│ Log statistics    │
│ Return stats dict │
└───────────────────┘
```

### Step-by-Step

1. **Count** — Counts incoming rows for statistics
2. **Write** — Writes DataFrame to Delta table using `mode("overwrite")`, replacing all existing data
3. **Partition** — If `partition_cols` are specified, data is partitioned accordingly. If not, falls back to `partition_key` column if present
4. **Optimize** — Runs `OPTIMIZE` on the table to compact small files (enabled by default, can be disabled)
5. **Log** — Records `rows_written` and strategy name to `PipelineStats`

---

## Contract Configuration {#contract-configuration}

### Minimal Contract

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.reference.country_codes

schema:
  name: bronze_reference
  table: country_codes
  properties:
    - name: country_code
      type: string
      isPrimaryKey: true
    - name: country_name
      type: string

customProperties:
  pipelineType: ingestion
  writeStrategy: overwrite
```

### Transformation Contract

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.analytics.customer_summary

schema:
  name: silver_analytics
  table: customer_summary
  properties:
    - name: customer_id
      type: bigint
      isPrimaryKey: true
    - name: total_orders
      type: integer
    - name: total_revenue
      type: double

customProperties:
  pipelineType: transformation
  writeStrategy: overwrite
  transformationSql: |
    SELECT
      c.customer_id,
      COUNT(o.order_id) AS total_orders,
      SUM(o.order_total) AS total_revenue
    FROM bronze_sales.customers c
    LEFT JOIN bronze_sales.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
```

---

## Parameters {#parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | *required* | The DataFrame to write |
| `target_table` | `str` or `None` | `None` (from contract) | Fully qualified target table name. If `None`, uses `{catalog}.{schema}.{table}` from the contract |
| `partition_cols` | `list` or `None` | `None` | Columns to partition by. If `None` and `partition_key` column exists, partitions by `partition_key` |
| `optimize` | `bool` | `True` | Run `OPTIMIZE` after write to compact small files |

---

## Return Statistics {#return-statistics}

```python
{
    "strategy": "overwrite",
    "target_table": "catalog.schema.table_name",
    "rows_written": 15000,
    "partitioned": True,
    "optimized": True
}
```

| Field | Type | Description |
|-------|------|-------------|
| `strategy` | `str` | Always `"overwrite"` |
| `target_table` | `str` | Fully qualified table name written to |
| `rows_written` | `int` | Number of rows in the new table |
| `partitioned` | `bool` | Whether partitioning was applied |
| `optimized` | `bool` | Whether OPTIMIZE was run after write |

---

## Schema Evolution {#schema-evolution}

The Overwrite pattern uses `mergeSchema: false` (implicit — not set). Each overwrite replaces the table entirely, so the schema of the new DataFrame becomes the table schema. If columns are added or removed in the source, the table schema will reflect the change on the next run.

> **Note**: This means downstream consumers may see schema changes between runs. If schema stability is important, consider adding schema validation in the pipeline before writing.

---

## Examples {#examples}

### Example 1: Basic Overwrite via Factory

```python
from nova_framework.io.factory import IOFactory

writer = IOFactory.create_writer("overwrite", context, stats)
write_stats = writer.write(df)

print(f"Wrote {write_stats['rows_written']} rows to {write_stats['target_table']}")
```

### Example 2: Overwrite with Custom Partitioning

```python
writer = IOFactory.create_writer("overwrite", context, stats)
write_stats = writer.write(
    df,
    partition_cols=["region", "date"],
    optimize=True
)
```

### Example 3: Overwrite without Optimization

```python
# Skip OPTIMIZE for small tables where compaction adds unnecessary overhead
writer = IOFactory.create_writer("overwrite", context, stats)
write_stats = writer.write(df, optimize=False)
```

### Example 4: Contract-Driven (Recommended)

```yaml
# Contract
customProperties:
  writeStrategy: overwrite
```

```python
# Pipeline — no strategy logic needed
writer = IOFactory.create_writer_from_contract(context, stats)
write_stats = writer.write(df)
```

---

## FAQs {#faqs}

**Q: Does overwrite preserve Delta Lake history?**
A: Yes. Delta Lake keeps previous versions for time travel. The overwrite creates a new version, but old versions remain accessible via `VERSION AS OF` or `TIMESTAMP AS OF` until the retention period expires.

**Q: Should I enable OPTIMIZE?**
A: For tables larger than a few hundred files, yes. For very small tables (< 10 files), the overhead of OPTIMIZE may not be worthwhile — set `optimize=False`.

**Q: Can I overwrite a partitioned table with different partition columns?**
A: No. Once a Delta table is created with a partition scheme, it cannot be changed. To change partitioning, drop and recreate the table.

**Q: What happens if the write fails mid-way?**
A: Delta Lake provides ACID transactions. If the write fails, the table remains in its previous state — no partial writes are committed.

---

_This is a living document. Last updated: February 2026_
