# NovaFlow Write Pattern: Type 2 Change Log (T2CL)

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**:
- [Write Patterns Overview](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md)
- [SCD2 Write Pattern](CONFLUENCE_WRITE_PATTERN_SCD2.md)
- [SCD4 Write Pattern](CONFLUENCE_WRITE_PATTERN_SCD4.md)
- [Data Contracts Guide](#)

---

## Table of Contents

1. [Overview](#overview)
2. [When to Use](#when-to-use)
3. [How It Works](#how-it-works)
4. [Change Detection Algorithm](#change-detection)
5. [Soft Delete Detection](#soft-delete)
6. [Materialized View](#materialized-view)
7. [Contract Configuration](#contract-configuration)
8. [Parameters](#parameters)
9. [Return Statistics](#return-statistics)
10. [Schema Evolution](#schema-evolution)
11. [Examples](#examples)
12. [T2CL vs SCD2: Key Differences](#t2cl-vs-scd2)
13. [FAQs](#faqs)

---

## Overview {#overview}

The **Type 2 Change Log (T2CL)** pattern maintains a full history of every change to every record. When an attribute changes, the current row is closed (marked `is_current = false`) and a new row is inserted with the updated values. This creates a complete audit trail with effective dating.

T2CL is the **default write strategy** in NovaFlow when no `writeStrategy` is specified.

| Property | Value |
|----------|-------|
| **Contract Value** | `type_2_change_log` |
| **Writer Class** | `T2CLWriter` |
| **Delta Write Mode** | `overwrite` (first load), `append` (subsequent) |
| **History Tracking** | Yes — full change history with effective dates |
| **Surrogate Key** | No (use SCD2 if needed) |
| **Materialized View** | Yes — `mv_{schema}.{table}_curr` |
| **Metadata Columns Added** | `effective_from`, `effective_to`, `is_current`, `deletion_flag` |
| **Required Hash Columns** | `natural_key_hash`, `change_key_hash`, `partition_key` |

### When to Use {#when-to-use}

- Customer, product, employee, or any dimension that changes over time
- When you need to know what the data looked like at any point in history
- Regulatory or compliance scenarios requiring a full audit trail
- When surrogate keys are not required (no star schema join dependency)

### When NOT to Use

- Append-only data (use Append)
- Full refresh with no history (use Overwrite)
- When surrogate keys are needed for star schema joins (use SCD2)
- Very large dimensions where querying `is_current = true` is too slow (use SCD4)

---

## How It Works {#how-it-works}

### First Load

```
Incoming DataFrame
        │
        ▼
┌──────────────────────────┐
│ Add metadata columns:    │
│  effective_from = today  │
│  effective_to = 9999-12-31│
│  is_current = true       │
│  deletion_flag = false   │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│ Create Delta table       │
│ mode: overwrite          │
│ partitionBy: partition_key│
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│ Create materialized view │
│ mv_{schema}.{table}_curr │
└──────────────────────────┘
```

### Subsequent Loads

```
Incoming DataFrame                Current Active Records
        │                                  │
        ▼                                  ▼
┌──────────────────────────────────────────────┐
│              LEFT JOIN                        │
│  ON incoming.natural_key_hash =               │
│     current.natural_key_hash                  │
└───────┬──────────────┬──────────────┬────────┘
        │              │              │
   ┌────▼────┐   ┌─────▼─────┐  ┌────▼─────┐
   │   NEW   │   │  CHANGED  │  │ UNCHANGED│
   │ (null   │   │ (key match│  │ (both    │
   │  join)  │   │  hash diff)│  │  match)  │
   └────┬────┘   └─────┬─────┘  └──────────┘
        │              │              (no action)
        │              │
        │         ┌────▼──────────┐
        │         │ Close old rows│
        │         │ is_current =  │
        │         │   false       │
        │         │ effective_to =│
        │         │   process_date│
        │         └────┬──────────┘
        │              │
        ▼              ▼
┌──────────────────────────┐
│   UNION new + changed    │
│   (+ soft deletes if     │
│    enabled)              │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│ INSERT new rows          │
│ mode: append             │
│ mergeSchema: true        │
└──────────┬───────────────┘
           │
           ▼
┌──────────────────────────┐
│ Refresh materialized view│
└──────────────────────────┘
```

---

## Change Detection Algorithm {#change-detection}

T2CL uses two hash columns to detect changes efficiently:

### natural_key_hash

Identifies **which entity** a row represents. Built from all columns marked `isPrimaryKey: true` in the contract.

```yaml
# Example: customer_id is the natural key
- name: customer_id
  type: bigint
  isPrimaryKey: true
```

### change_key_hash

Identifies **what the entity's attributes look like**. Built from all columns marked `isChangeTracking: true` in the contract.

```yaml
# Example: name and email are tracked for changes
- name: customer_name
  type: string
  isChangeTracking: true
- name: email
  type: string
  isChangeTracking: true
```

### Classification Logic

| Condition | Classification | Action |
|-----------|---------------|--------|
| `natural_key_hash` not in current table | **NEW** | Insert with `is_current = true` |
| `natural_key_hash` matches, `change_key_hash` differs | **CHANGED** | Close old row, insert new row |
| Both hashes match | **UNCHANGED** | No action |

### Example

| Load | customer_id | name | email | natural_key_hash | change_key_hash |
|------|------------|------|-------|-----------------|----------------|
| Day 1 | 101 | Alice | alice@old.com | `abc123` | `def456` |
| Day 2 | 101 | Alice | alice@new.com | `abc123` | `ghi789` |

Day 2 result: `natural_key_hash` matches (`abc123`), but `change_key_hash` differs (`def456` vs `ghi789`). Record is classified as **CHANGED**.

**Table after Day 2:**

| natural_key_hash | name | email | effective_from | effective_to | is_current |
|-----------------|------|-------|---------------|-------------|------------|
| abc123 | Alice | alice@old.com | 2026-02-01 | 2026-02-02 | false |
| abc123 | Alice | alice@new.com | 2026-02-02 | 9999-12-31 | true |

---

## Soft Delete Detection {#soft-delete}

When `soft_delete=True`, T2CL detects records that were present in the previous load but are **missing** from the current load.

### How Soft Delete Works

1. **Identify missing keys**: Find `natural_key_hash` values in current active records that are NOT in the incoming data
2. **Close existing rows**: Mark as `is_current = false`, `effective_to = process_date`, `deletion_flag = true`
3. **Create tombstone rows**: Insert new rows with `is_current = true` and `deletion_flag = true` to record the deletion event

### Soft Delete Example

| Time | Event | Result |
|------|-------|--------|
| Day 1 | Customer 101 arrives | Row inserted: `is_current=true, deletion_flag=false` |
| Day 2 | Customer 101 missing from source | Old row closed. Tombstone inserted: `is_current=true, deletion_flag=true` |
| Day 3 | Customer 101 reappears | Tombstone closed. New row: `is_current=true, deletion_flag=false` |

### Enabling Soft Delete

```yaml
customProperties:
  writeStrategy: type_2_change_log
  softDelete: true
```

> **Note**: Soft deletes are excluded from the materialized view. The view filters `WHERE is_current = true AND deletion_flag = false`, so deleted records do not appear in the current position view.

---

## Materialized View {#materialized-view}

After every write (first load or subsequent), T2CL creates or refreshes a **materialized view** that exposes only the current active records without the T2CL metadata columns.

### Naming Convention

```
{catalog}.mv_{schema}.{table}_curr
```

**Examples:**
- Table: `prod_catalog.bronze_sales.customers` → View: `prod_catalog.mv_bronze_sales.customers_curr`
- Table: `bronze_hr.employees` → View: `mv_bronze_hr.employees_curr`

### View Definition

```sql
CREATE OR REPLACE MATERIALIZED VIEW {catalog}.mv_{schema}.{table}_curr AS
SELECT {all columns except effective_from, effective_to, is_current, deletion_flag}
FROM {target_table}
WHERE is_current = true AND deletion_flag = false
```

### Why a Materialized View?

- **Performance**: Downstream consumers query the MV without needing to filter on `is_current`
- **Simplicity**: Consumers see a clean table without T2CL metadata columns
- **Auto-Refresh**: The view is refreshed after every pipeline run

---

## Contract Configuration {#contract-configuration}

### Standard T2CL Contract

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
    - name: phone
      type: string
      isChangeTracking: true

customProperties:
  pipelineType: ingestion
  writeStrategy: type_2_change_log
  softDelete: true
  volume: M
```

### T2CL with Minimal Configuration (Default Strategy)

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.sales.products

schema:
  name: bronze_sales
  table: products
  properties:
    - name: product_id
      type: bigint
      isPrimaryKey: true
    - name: product_name
      type: string
      isChangeTracking: true
    - name: price
      type: decimal(18,2)
      isChangeTracking: true

customProperties:
  pipelineType: ingestion
  # writeStrategy defaults to type_2_change_log
```

---

## Parameters {#parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | *required* | Incoming DataFrame (must include hash columns) |
| `target_table` | `str` or `None` | `None` (from contract) | Fully qualified target table name |
| `natural_key_col` | `str` | `"natural_key_hash"` | Column containing the natural key hash |
| `change_key_col` | `str` | `"change_key_hash"` | Column containing the change tracking hash |
| `partition_col` | `str` | `"partition_key"` | Column to partition by |
| `process_date` | `str` or `None` | `None` (today's date) | Effective date for this load in `YYYY-MM-DD` format |
| `soft_delete` | `bool` | `False` | Enable soft delete detection |

---

## Return Statistics {#return-statistics}

### First Load

```python
{
    "strategy": "type_2_change_log",
    "first_load": True,
    "target_table": "catalog.bronze_sales.customers",
    "current_view": "catalog.mv_bronze_sales.customers_curr",
    "records_inserted": 10000
}
```

### Subsequent Loads

```python
{
    "strategy": "type_2_change_log",
    "target_table": "catalog.bronze_sales.customers",
    "current_view": "catalog.mv_bronze_sales.customers_curr",
    "new_records": 250,
    "changed_records": 1500,
    "soft_deleted": 30,
    "records_inserted": 1780,
    "process_date": "2026-02-09"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `strategy` | `str` | Always `"type_2_change_log"` |
| `first_load` | `bool` | Present only on initial load |
| `target_table` | `str` | Fully qualified table name |
| `current_view` | `str` | Name of the materialized view |
| `new_records` | `int` | Records with new natural keys |
| `changed_records` | `int` | Records with changed attributes |
| `soft_deleted` | `int` | Records detected as deleted |
| `records_inserted` | `int` | Total rows inserted (new + changed + deleted) |
| `process_date` | `str` | Effective date used for this load |

### PipelineStats Metrics

| Metric | Description |
|--------|-------------|
| `t2cl_new` | Count of new records |
| `t2cl_changed` | Count of changed records |
| `t2cl_deleted` | Count of soft-deleted records |
| `rows_written` | Total rows inserted |

---

## Schema Evolution {#schema-evolution}

T2CL uses `mergeSchema: true` on append writes, which means:

- **New columns** in the incoming DataFrame will be added to the table automatically
- **Missing columns** in the incoming DataFrame will be filled with `NULL` in the new rows
- **Type changes** are not supported and will cause an error

This is intentional — as source systems evolve, new columns can flow through without manual DDL changes.

---

## Examples {#examples}

### Example 1: Standard T2CL Write

```python
writer = IOFactory.create_writer("type_2_change_log", context, stats)
write_stats = writer.write(df, soft_delete=True)

print(f"New: {write_stats['new_records']}")
print(f"Changed: {write_stats['changed_records']}")
print(f"Deleted: {write_stats['soft_deleted']}")
```

### Example 2: T2CL with Custom Process Date

```python
writer = IOFactory.create_writer("type_2_change_log", context, stats)
write_stats = writer.write(
    df,
    process_date="2026-01-15",
    soft_delete=True
)
```

### Example 3: Query the Current Position View

```sql
-- Fast query for current data (no filtering needed)
SELECT * FROM prod_catalog.mv_bronze_sales.customers_curr

-- Historical query (point-in-time)
SELECT *
FROM prod_catalog.bronze_sales.customers
WHERE effective_from <= '2026-01-15'
  AND effective_to > '2026-01-15'

-- Full audit trail for a specific customer
SELECT *
FROM prod_catalog.bronze_sales.customers
WHERE natural_key_hash = 'abc123'
ORDER BY effective_from
```

---

## T2CL vs SCD2: Key Differences {#t2cl-vs-scd2}

| Aspect | T2CL | SCD2 |
|--------|------|------|
| Surrogate Key | None | BIGINT IDENTITY (`sk` column) |
| Primary Use | General-purpose history tracking | Star schema dimensions |
| View Includes SK | N/A | Yes (for joins) |
| Key Generation | N/A | Delta Lake IDENTITY (cluster-safe) |
| Insert Method | DataFrame append | SQL INSERT (to trigger IDENTITY) |
| Metadata Columns | 4 (effective_from/to, is_current, deletion_flag) | 5 (same 4 + sk) |
| Performance | Slightly faster (no IDENTITY overhead) | Slightly slower (IDENTITY generation) |

**Rule of thumb**: Use T2CL unless you need surrogate keys for star schema joins.

---

## FAQs {#faqs}

**Q: What happens if I re-run the same data?**
A: If the data hasn't changed (same `change_key_hash`), all records are classified as UNCHANGED and nothing is written. This makes T2CL safe to re-run.

**Q: What if the hash columns are missing?**
A: The write will fail. Ensure the `HashingStage` runs before the `WriteStage` in your pipeline. The hashing stage generates `natural_key_hash`, `change_key_hash`, and `partition_key` from contract column definitions.

**Q: Can I query historical data at a specific point in time?**
A: Yes. Filter on effective dates:
```sql
WHERE effective_from <= '2026-01-15' AND effective_to > '2026-01-15'
```

**Q: How does soft delete interact with re-appearing records?**
A: If a record disappears (soft deleted) and then reappears in a later load, it is treated as a NEW record. The tombstone row is closed, and a fresh row is inserted.

**Q: Is the materialized view automatically refreshed?**
A: Yes. The view is refreshed at the end of every T2CL write operation.

**Q: What is the `9999-12-31` date?**
A: It represents "no end date" — the record is currently active. When a record is closed, `effective_to` is updated to the process date.

---

_This is a living document. Last updated: February 2026_
