# NovaFlow Write Pattern: SCD Type 4 (Current + Historical)

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**:
- [Write Patterns Overview](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md)
- [T2CL Write Pattern](CONFLUENCE_WRITE_PATTERN_T2CL.md)
- [SCD2 Write Pattern](CONFLUENCE_WRITE_PATTERN_SCD2.md)
- [Data Contracts Guide](#)

---

## Table of Contents

1. [Overview](#overview)
2. [When to Use](#when-to-use)
3. [How It Works](#how-it-works)
4. [Two-Table Architecture](#two-table-architecture)
5. [Contract Configuration](#contract-configuration)
6. [Parameters](#parameters)
7. [Return Statistics](#return-statistics)
8. [Query Patterns](#query-patterns)
9. [Examples](#examples)
10. [FAQs](#faqs)

---

## Overview {#overview}

The **SCD Type 4** pattern splits data into two tables: a **current table** containing only the latest version of each record (fast queries, no filtering needed), and a **historical table** maintaining the full T2CL change log. This pattern optimises for the common case where most queries only need current data, while still preserving complete history.

| Property | Value |
|----------|-------|
| **Contract Value** | `scd4` |
| **Writer Class** | `SCD4Writer` |
| **Delta Write Mode** | T2CL for history, overwrite for current |
| **History Tracking** | Yes — in the historical table |
| **Surrogate Key** | No |
| **Materialized View** | Yes — created by T2CL on the historical table |
| **Tables Created** | 2: `{table}_current` and `{table}_history` |
| **Metadata Columns** | Historical table: `effective_from`, `effective_to`, `is_current`, `deletion_flag`. Current table: none |
| **Required Hash Columns** | `natural_key_hash`, `change_key_hash`, `partition_key` |

### When to Use {#when-to-use}

- **Large dimensions** (millions of rows) where filtering `is_current = true` on every query is expensive
- When **95%+ of queries** only need current data but history must still be preserved
- When downstream consumers need a **clean, metadata-free** current-state table
- When you want the **performance of overwrite** for current queries with the **auditability of T2CL**

### When NOT to Use

- Small to medium dimensions where filtering on `is_current` is negligible (use T2CL)
- When you need surrogate keys (use SCD2)
- Append-only data (use Append)
- Full refresh scenarios (use Overwrite)

---

## How It Works {#how-it-works}

```
Incoming DataFrame
        │
        ▼
┌────────────────────────────────────┐
│         SCD4Writer                 │
│                                    │
│  ┌──────────────────────────────┐  │
│  │ Step 1: Write to HISTORY    │  │
│  │ table using T2CLWriter      │  │
│  │                             │  │
│  │ → Full change detection     │  │
│  │ → Effective dating          │  │
│  │ → Soft deletes (if enabled) │  │
│  │ → Materialized view         │  │
│  └─────────────┬───────────────┘  │
│                │                   │
│  ┌─────────────▼───────────────┐  │
│  │ Step 2: Build CURRENT table │  │
│  │                             │  │
│  │ → Read history WHERE        │  │
│  │   is_current = true         │  │
│  │ → Drop T2CL metadata cols  │  │
│  │ → Overwrite current table   │  │
│  └─────────────────────────────┘  │
└────────────────────────────────────┘
        │
        ▼
   Two tables:
   ┌─────────────────┐  ┌─────────────────────┐
   │ {table}_current  │  │ {table}_history      │
   │ (clean, fast)    │  │ (full T2CL change   │
   │                  │  │  log with metadata)  │
   └─────────────────┘  └─────────────────────┘
```

### Step-by-Step

1. **Write to historical table** — Delegates to `T2CLWriter`, which performs full change detection, closes old rows, inserts new/changed rows, and handles soft deletes
2. **Read current records** — Filters the historical table for `is_current = true`
3. **Strip metadata** — Removes `effective_from`, `effective_to`, `is_current`, and `deletion_flag` columns
4. **Overwrite current table** — Writes the clean DataFrame to the current table using `mode("overwrite")`

---

## Two-Table Architecture {#two-table-architecture}

### Table Naming

If not provided explicitly, table names are auto-generated from the contract:

```
Base: {catalog}.{schema}.{table}

Current:    {catalog}.{schema}.{table}_current
Historical: {catalog}.{schema}.{table}_history
```

**Example**:
- Contract: `prod_catalog.bronze_sales.suppliers`
- Current table: `prod_catalog.bronze_sales.suppliers_current`
- Historical table: `prod_catalog.bronze_sales.suppliers_history`

### Current Table

| Property | Detail |
|----------|--------|
| **Contains** | Latest version of every active record |
| **Metadata Columns** | None — clean business data only |
| **Write Mode** | Full overwrite on each pipeline run |
| **Query Performance** | Optimal — no filtering required |
| **Use For** | Dashboards, reports, application queries |

### Historical Table

| Property | Detail |
|----------|--------|
| **Contains** | Every version of every record (full T2CL) |
| **Metadata Columns** | `effective_from`, `effective_to`, `is_current`, `deletion_flag` |
| **Write Mode** | T2CL merge (append new rows, update existing) |
| **Query Performance** | Requires `WHERE` filters for point-in-time queries |
| **Use For** | Auditing, compliance, point-in-time analysis |

### Visual Comparison

```
suppliers_current (clean, fast)
┌────────────┬───────────────┬────────┐
│ supplier_id│ supplier_name │ rating │
├────────────┼───────────────┼────────┤
│ 1          │ Acme Corp     │ A      │
│ 2          │ Globex Inc    │ B+     │
│ 3          │ Initech       │ A-     │
└────────────┴───────────────┴────────┘

suppliers_history (full audit trail)
┌────────────┬───────────────┬────────┬────────────────┬──────────────┬────────────┬───────────────┐
│ supplier_id│ supplier_name │ rating │ effective_from │ effective_to │ is_current │ deletion_flag │
├────────────┼───────────────┼────────┼────────────────┼──────────────┼────────────┼───────────────┤
│ 1          │ Acme Corp     │ B      │ 2025-01-01     │ 2025-06-15   │ false      │ false         │
│ 1          │ Acme Corp     │ A      │ 2025-06-15     │ 9999-12-31   │ true       │ false         │
│ 2          │ Globex Inc    │ B+     │ 2025-03-01     │ 9999-12-31   │ true       │ false         │
│ 3          │ Initech       │ B      │ 2025-02-01     │ 2025-09-01   │ false      │ false         │
│ 3          │ Initech       │ A-     │ 2025-09-01     │ 9999-12-31   │ true       │ false         │
└────────────┴───────────────┴────────┴────────────────┴──────────────┴────────────┴───────────────┘
```

---

## Contract Configuration {#contract-configuration}

### Standard SCD4 Contract

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.procurement.suppliers

schema:
  name: bronze_procurement
  table: suppliers
  properties:
    - name: supplier_id
      type: bigint
      isPrimaryKey: true
    - name: supplier_name
      type: string
      isChangeTracking: true
    - name: contact_email
      type: string
      isChangeTracking: true
    - name: rating
      type: string
      isChangeTracking: true
    - name: country
      type: string
      isChangeTracking: true

customProperties:
  pipelineType: ingestion
  writeStrategy: scd4
  volume: L
```

> **Note**: Soft delete is controlled by the T2CL writer internally. To enable it, pass `soft_delete=True` in code or configure it in the pipeline.

---

## Parameters {#parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | *required* | Incoming DataFrame (must include hash columns) |
| `current_table` | `str` or `None` | `None` (auto: `{base}_current`) | Fully qualified current table name |
| `historical_table` | `str` or `None` | `None` (auto: `{base}_history`) | Fully qualified historical table name |
| `natural_key_col` | `str` | `"natural_key_hash"` | Column containing the natural key hash |
| `change_key_col` | `str` | `"change_key_hash"` | Column containing the change tracking hash |
| `partition_col` | `str` | `"partition_key"` | Column to partition by |
| `process_date` | `str` or `None` | `None` (today's date) | Effective date for this load |

---

## Return Statistics {#return-statistics}

```python
{
    "strategy": "scd4",
    "current_table": "catalog.bronze_procurement.suppliers_current",
    "historical_table": "catalog.bronze_procurement.suppliers_history",
    "current_rows": 50000,
    "historical_stats": {
        "strategy": "type_2_change_log",
        "target_table": "catalog.bronze_procurement.suppliers_history",
        "current_view": "catalog.mv_bronze_procurement.suppliers_history_curr",
        "new_records": 200,
        "changed_records": 800,
        "soft_deleted": 15,
        "records_inserted": 1015,
        "process_date": "2026-02-09"
    }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `strategy` | `str` | Always `"scd4"` |
| `current_table` | `str` | Fully qualified current table name |
| `historical_table` | `str` | Fully qualified historical table name |
| `current_rows` | `int` | Number of rows in the current table after refresh |
| `historical_stats` | `dict` | Full T2CL statistics dictionary from the historical table write |

---

## Query Patterns {#query-patterns}

### Current Data (Fast)

```sql
-- No filtering needed — this table only contains current records
SELECT *
FROM catalog.bronze_procurement.suppliers_current
WHERE country = 'UK'
```

### Full History

```sql
-- All versions of all suppliers
SELECT *
FROM catalog.bronze_procurement.suppliers_history
ORDER BY supplier_id, effective_from
```

### Point-in-Time Query

```sql
-- What did suppliers look like on January 15th?
SELECT *
FROM catalog.bronze_procurement.suppliers_history
WHERE effective_from <= '2026-01-15'
  AND effective_to > '2026-01-15'
```

### Change Audit for a Specific Record

```sql
SELECT *
FROM catalog.bronze_procurement.suppliers_history
WHERE natural_key_hash = 'abc123'
ORDER BY effective_from
```

---

## Examples {#examples}

### Example 1: Standard SCD4 Write

```python
writer = IOFactory.create_writer("scd4", context, stats)
write_stats = writer.write(df)

print(f"Current table: {write_stats['current_table']} ({write_stats['current_rows']} rows)")
print(f"History new: {write_stats['historical_stats']['new_records']}")
print(f"History changed: {write_stats['historical_stats']['changed_records']}")
```

### Example 2: Custom Table Names

```python
writer = IOFactory.create_writer("scd4", context, stats)
write_stats = writer.write(
    df,
    current_table="catalog.gold_procurement.suppliers_latest",
    historical_table="catalog.archive_procurement.suppliers_history"
)
```

### Example 3: Contract-Driven (Recommended)

```yaml
customProperties:
  writeStrategy: scd4
```

```python
writer = IOFactory.create_writer_from_contract(context, stats)
write_stats = writer.write(df)
```

---

## FAQs {#faqs}

**Q: Why not just use T2CL with a materialized view?**
A: For small to medium tables, T2CL with its materialized view is sufficient. SCD4 is designed for **large dimensions** where the historical table grows very large and filtering on `is_current = true` becomes a performance concern. The current table is a lean, overwritten table that's always fast to query.

**Q: Does the current table have Delta Lake history?**
A: Yes. Since it's a Delta table written with `mode("overwrite")`, Delta maintains previous versions. However, the intent is to query the current table for the latest data, not for time travel.

**Q: Can I use SCD4 with surrogate keys?**
A: Not directly. SCD4 uses T2CL internally (no surrogate keys). If you need surrogate keys with a dual-table pattern, you would need a custom solution combining SCD2 for the historical table and an overwrite for the current table.

**Q: What happens if the historical T2CL write fails?**
A: The current table is NOT updated if the historical write fails — the SCD4Writer writes to history first, then rebuilds current. If history fails, the entire operation fails and both tables remain in their previous state.

**Q: How much storage does SCD4 use compared to T2CL?**
A: SCD4 uses approximately 2x the storage of T2CL alone (current table + historical table). The current table is typically small (one row per entity), while the historical table is identical to what T2CL would produce.

---

_This is a living document. Last updated: February 2026_
