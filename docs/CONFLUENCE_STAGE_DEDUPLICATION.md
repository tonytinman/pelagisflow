# NovaFlow Pipeline Stage: Deduplication

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**: [Pipeline Stages Overview](CONFLUENCE_PIPELINE_STAGES_OVERVIEW.md) | [HashingStage](CONFLUENCE_STAGE_HASHING.md)

---

## Overview

The **DeduplicationStage** removes duplicate rows from the DataFrame using the hash columns produced by the Hashing stage. It ensures only one row per unique natural key + change key combination is written to the target.

| Property | Value |
|----------|-------|
| **Class** | `DeduplicationStage` |
| **File** | `nova_framework/pipeline/stages/deduplication_stage.py` |
| **Stage Name** | `Deduplication` |
| **Pipeline Position** | After Hashing |
| **Skip Condition** | No `natural_key_columns` in contract (mirrors Hashing skip) |
| **Modifies DataFrame** | Yes — removes duplicate rows |

---

## How It Works

1. Checks which hash columns are present in the DataFrame (`natural_key_hash`, `change_key_hash`)
2. If no hash columns exist, logs a warning and returns the DataFrame unchanged
3. Calls `DeduplicationProcessor.deduplicate(df, key_columns, stats)` using `dropDuplicates()`
4. Counts rows before and after to calculate `removed_count`
5. Returns the deduplicated DataFrame

### Deduplication Logic

| Columns Available | Behaviour |
|-------------------|-----------|
| Both `natural_key_hash` + `change_key_hash` | Deduplicates on both — only removes rows with identical entity AND attributes |
| Only `natural_key_hash` | Deduplicates on natural key — keeps first row per entity |
| Only `change_key_hash` | Deduplicates on change key — keeps first row per attribute set |
| Neither | Skips deduplication with warning |

> **Note**: This stage deduplicates within the incoming batch only. It does not check for duplicates against the existing target table. Cross-batch deduplication is handled by the write strategies (T2CL, SCD2).

---

## Skip Condition

The stage shares the same skip condition as HashingStage: if no `natural_key_columns` are defined in the contract, the stage is skipped. This is because without hash columns, there's nothing to deduplicate on.

---

## Example Log Output

```
[Deduplication] Starting...
Starting deduplication
Using natural_key_hash for deduplication
Using change_key_hash for deduplication
Deduplication complete: 10000 → 9800 rows (200 duplicates removed)
[Deduplication] Completed in 1.08s
```

### When No Duplicates Found

```
[Deduplication] Starting...
Starting deduplication
Using natural_key_hash for deduplication
Using change_key_hash for deduplication
Deduplication complete: 10000 → 10000 rows (0 duplicates removed)
[Deduplication] Completed in 0.92s
```

### When Skipped

```
[Deduplication] Skipped (skip condition met)
Skipping DeduplicationStage - no hash columns available
```

---

## When Duplicates Occur

Common sources of duplicates in ingestion pipelines:

| Source | Scenario | How Dedup Helps |
|--------|----------|-----------------|
| Re-delivered files | Same source file uploaded twice | Same natural + change key hash → removed |
| Overlapping file drops | Two files contain the same customer | Same natural + change key hash → removed |
| Source system errors | Duplicate rows in the source extract | Identical rows produce identical hashes → removed |

---

_This is a living document. Last updated: February 2026_
