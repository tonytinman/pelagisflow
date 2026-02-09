# NovaFlow Write Pattern: Append

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**:
- [Write Patterns Overview](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md)
- [Overwrite Write Pattern](CONFLUENCE_WRITE_PATTERN_OVERWRITE.md)
- [T2CL Write Pattern](CONFLUENCE_WRITE_PATTERN_T2CL.md)
- [Data Contracts Guide](#)

---

## Table of Contents

1. [Overview](#overview)
2. [When to Use](#when-to-use)
3. [How It Works](#how-it-works)
4. [Contract Configuration](#contract-configuration)
5. [Parameters](#parameters)
6. [Deduplication](#deduplication)
7. [Return Statistics](#return-statistics)
8. [Examples](#examples)
9. [FAQs](#faqs)

---

## Overview {#overview}

The **Append** pattern adds new rows to an existing table without modifying existing data. It optionally deduplicates the incoming DataFrame before appending.

| Property | Value |
|----------|-------|
| **Contract Value** | `append` |
| **Writer Class** | `AppendWriter` |
| **Delta Write Mode** | `append` |
| **History Tracking** | Implicit (rows accumulate) |
| **Surrogate Key** | No |
| **Materialized View** | No |
| **Metadata Columns Added** | None |
| **Required Hash Columns** | None |

### When to Use {#when-to-use}

- Fact tables that grow over time (orders, transactions, clicks)
- Event logs and audit trails
- Append-only data feeds where each batch adds new records
- Any table where rows are inserted but never updated or deleted

### When NOT to Use

- Dimensions that need change tracking or versioning
- Tables where you need to update existing rows
- Scenarios requiring deduplication against the existing table (append only deduplicates within the incoming batch)

---

## How It Works {#how-it-works}

```
Incoming DataFrame
        │
        ▼
┌───────────────────┐
│ Deduplicate?      │  ← optional
│ dropDuplicates()  │
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│ Count rows        │
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│ Write as Delta    │
│ mode: append      │
│ (optional         │
│  partitioning)    │
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│ Log statistics    │
│ Return stats dict │
└───────────────────┘
```

### Step-by-Step

1. **Deduplicate (optional)** — If `deduplicate=True`, removes duplicate rows from the incoming batch
   - With `dedup_cols`: deduplicates on specified columns only
   - Without `dedup_cols`: deduplicates on all columns (exact row match)
2. **Count** — Counts rows after deduplication for statistics
3. **Write** — Appends DataFrame to Delta table using `mode("append")`
4. **Partition** — If `partition_cols` are specified, data is partitioned accordingly
5. **Log** — Records `rows_written`, `rows_deduped`, and strategy name to `PipelineStats`

> **Important**: Deduplication only removes duplicates *within the incoming batch*. It does not check for duplicates against existing rows in the target table. If cross-batch deduplication is needed, use T2CL or a custom pre-write step.

---

## Contract Configuration {#contract-configuration}

### Event Log Contract

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.clickstream.page_views

schema:
  name: bronze_clickstream
  table: page_views
  properties:
    - name: event_id
      type: string
      isPrimaryKey: true
    - name: user_id
      type: bigint
    - name: page_url
      type: string
    - name: event_timestamp
      type: timestamp

customProperties:
  pipelineType: ingestion
  writeStrategy: append
```

### Transaction Fact Table Contract

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.sales.order_lines

schema:
  name: bronze_sales
  table: order_lines
  properties:
    - name: order_line_id
      type: bigint
      isPrimaryKey: true
    - name: order_id
      type: bigint
    - name: product_id
      type: bigint
    - name: quantity
      type: integer
    - name: unit_price
      type: decimal(18,2)

customProperties:
  pipelineType: ingestion
  writeStrategy: append
  volume: L
```

---

## Parameters {#parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | *required* | The DataFrame to append |
| `target_table` | `str` or `None` | `None` (from contract) | Fully qualified target table name |
| `partition_cols` | `list` or `None` | `None` | Columns to partition by |
| `deduplicate` | `bool` | `False` | Remove duplicates from incoming batch before appending |
| `dedup_cols` | `list` or `None` | `None` | Columns to use for deduplication. If `None` and `deduplicate=True`, deduplicates on all columns |

---

## Deduplication {#deduplication}

### How Deduplication Works

When `deduplicate=True`:

| Scenario | Behaviour |
|----------|-----------|
| `dedup_cols=None` | Calls `df.dropDuplicates()` — removes rows that are identical across **all** columns |
| `dedup_cols=["order_id", "product_id"]` | Calls `df.dropDuplicates(["order_id", "product_id"])` — keeps first row per unique combination of specified columns |

### Deduplication Statistics

The number of removed duplicates is tracked:

```python
# In PipelineStats
pipeline_stats.log_stat("rows_deduped", dedup_count)

# In return dictionary
write_stats["dedup_removed"]  # e.g., 150
```

### Example: Dedup on Specific Columns

```python
writer = IOFactory.create_writer("append", context, stats)
write_stats = writer.write(
    df,
    deduplicate=True,
    dedup_cols=["event_id"]  # Keep one row per event_id
)

print(f"Rows written: {write_stats['rows_written']}")
print(f"Duplicates removed: {write_stats['dedup_removed']}")
```

---

## Return Statistics {#return-statistics}

```python
{
    "strategy": "append",
    "target_table": "catalog.bronze_clickstream.page_views",
    "rows_written": 50000,
    "deduplicated": True,
    "dedup_removed": 150
}
```

| Field | Type | Description |
|-------|------|-------------|
| `strategy` | `str` | Always `"append"` |
| `target_table` | `str` | Fully qualified table name |
| `rows_written` | `int` | Number of rows appended (after dedup) |
| `deduplicated` | `bool` | Whether deduplication was applied |
| `dedup_removed` | `int` | Number of duplicate rows removed (0 if no dedup) |

---

## Examples {#examples}

### Example 1: Simple Append

```python
writer = IOFactory.create_writer("append", context, stats)
write_stats = writer.write(df)
```

### Example 2: Append with Deduplication

```python
writer = IOFactory.create_writer("append", context, stats)
write_stats = writer.write(
    df,
    deduplicate=True,
    dedup_cols=["event_id"]
)
```

### Example 3: Partitioned Append

```python
writer = IOFactory.create_writer("append", context, stats)
write_stats = writer.write(
    df,
    partition_cols=["event_date"],
    deduplicate=True
)
```

### Example 4: Contract-Driven (Recommended)

```yaml
customProperties:
  writeStrategy: append
```

```python
writer = IOFactory.create_writer_from_contract(context, stats)
write_stats = writer.write(df)
```

---

## FAQs {#faqs}

**Q: Does Append protect against duplicate rows across batches?**
A: No. Deduplication only applies within the current incoming DataFrame. If the same row was appended in a previous run, it will not be detected. For cross-batch deduplication, use T2CL or add a pre-write step that checks against the existing table.

**Q: What happens if the schema of the incoming data changes?**
A: Delta Lake's append mode will fail if the incoming schema doesn't match the existing table schema (unless `mergeSchema` is enabled). Add schema validation in the pipeline before the write stage.

**Q: Is there a risk of data loss?**
A: No. Append mode only adds rows — it never modifies or deletes existing data. If the write fails, Delta Lake's ACID transactions ensure no partial data is committed.

**Q: Should I partition my append table?**
A: Yes, if the table is queried with a common filter (e.g., date). Partitioning improves query performance by enabling partition pruning. Common choices: `event_date`, `region`, `source_system`.

---

_This is a living document. Last updated: February 2026_
