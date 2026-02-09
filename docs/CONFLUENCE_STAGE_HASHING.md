# NovaFlow Pipeline Stage: Hashing

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**: [Pipeline Stages Overview](CONFLUENCE_PIPELINE_STAGES_OVERVIEW.md) | [T2CL Write Pattern](CONFLUENCE_WRITE_PATTERN_T2CL.md) | [SCD2 Write Pattern](CONFLUENCE_WRITE_PATTERN_SCD2.md)

---

## Overview

The **HashingStage** generates hash columns used for change detection and data distribution. These columns are required by the T2CL, SCD2, and SCD4 write strategies.

| Property | Value |
|----------|-------|
| **Class** | `HashingStage` |
| **File** | `nova_framework/pipeline/stages/hashing_stage.py` |
| **Stage Name** | `Hashing` |
| **Pipeline Position** | After Lineage |
| **Skip Condition** | No `natural_key_columns` in contract |
| **Modifies DataFrame** | Yes — adds up to 3 columns |

---

## Columns Added

| Column | Source | Purpose | Used By |
|--------|--------|---------|---------|
| `natural_key_hash` | SHA-256 of all `isPrimaryKey: true` columns | Identifies unique entities across loads | T2CL, SCD2, SCD4, Dedup |
| `change_key_hash` | SHA-256 of all `isChangeTracking: true` columns | Detects attribute changes between loads | T2CL, SCD2, SCD4 |
| `partition_key` | `abs(natural_key_hash) % num_partitions` | Distributes data evenly across partitions | T2CL, SCD2, SCD4, Overwrite |

---

## How It Works

1. Reads `natural_key_columns` from contract (columns with `isPrimaryKey: true`)
2. Calls `HashingProcessor.add_hash_column(df, "natural_key_hash", natural_key_cols)` to compute SHA-256
3. Reads `change_tracking_columns` from contract (columns with `isChangeTracking: true`)
4. Calls `HashingProcessor.add_hash_column(df, "change_key_hash", change_key_cols)`
5. Calls `HashingProcessor.add_partition_hash(df, num_partitions)` to compute modulo partition key
6. Returns enriched DataFrame

---

## Contract Configuration

The hash columns are derived from the `isPrimaryKey` and `isChangeTracking` flags in the schema:

```yaml
schema:
  properties:
    - name: customer_id
      type: bigint
      isPrimaryKey: true          # → natural_key_hash

    - name: customer_name
      type: string
      isChangeTracking: true      # → change_key_hash

    - name: email
      type: string
      isChangeTracking: true      # → change_key_hash

    - name: created_date
      type: date
      # Neither flag → excluded from both hashes
```

### Skip Condition

If no columns are marked `isPrimaryKey: true`, the stage is skipped entirely and the DataFrame passes through unchanged. This is expected for contracts that use the `overwrite` or `file_export` write strategies.

---

## Partition Count

The number of partitions is determined by `contract.recommended_partitions`, which is typically derived from the `volume` custom property:

| Volume | Recommended Partitions |
|--------|----------------------|
| `XXS` | 1 |
| `XS` | 2 |
| `S` | 4 |
| `M` | 8 |
| `L` | 16 |
| `XL` | 32 |
| `XXL` | 64 |

---

## Example Log Output

```
[Hashing] Starting...
Adding hash columns for change tracking
Adding natural_key_hash from columns: ['customer_id']
Adding change_key_hash from columns: ['customer_name', 'email']
Adding partition_key with 8 partitions
Hash columns added successfully
[Hashing] Completed in 0.45s
```

### When Skipped

```
[Hashing] Skipped (skip condition met)
Skipping HashingStage - no natural_key_columns defined in contract
```

---

## Downstream Dependencies

| Stage | Requires |
|-------|----------|
| DeduplicationStage | `natural_key_hash`, `change_key_hash` |
| WriteStage (T2CL) | `natural_key_hash`, `change_key_hash`, `partition_key` |
| WriteStage (SCD2) | `natural_key_hash`, `change_key_hash`, `partition_key` |
| WriteStage (SCD4) | `natural_key_hash`, `change_key_hash`, `partition_key` |

If the Hashing stage is skipped, the Deduplication stage will also skip (same skip condition).

---

_This is a living document. Last updated: February 2026_
