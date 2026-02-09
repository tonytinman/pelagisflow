# NovaFlow Write Pattern: File Export

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**:
- [Write Patterns Overview](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md)
- [Overwrite Write Pattern](CONFLUENCE_WRITE_PATTERN_OVERWRITE.md)
- [Data Contracts Guide](#)

---

## Table of Contents

1. [Overview](#overview)
2. [When to Use](#when-to-use)
3. [How It Works](#how-it-works)
4. [Supported Formats](#supported-formats)
5. [Contract Configuration](#contract-configuration)
6. [Parameters](#parameters)
7. [Return Statistics](#return-statistics)
8. [CSV Options](#csv-options)
9. [Examples](#examples)
10. [FAQs](#faqs)

---

## Overview {#overview}

The **File Export** pattern writes a DataFrame to a file system path in a specified format. Unlike other write patterns which persist to Delta Lake tables, File Export writes to storage paths as files — suitable for data extracts, external system feeds, and cross-platform data exchange.

| Property | Value |
|----------|-------|
| **Contract Value** | `file_export` |
| **Writer Class** | `FileExportWriter` |
| **Write Mode** | Configurable (`overwrite` or `append`) |
| **History Tracking** | No |
| **Surrogate Key** | No |
| **Materialized View** | No |
| **Metadata Columns Added** | None |
| **Required Hash Columns** | None |
| **Target** | File system path (not a table) |

### When to Use {#when-to-use}

- Generating data extracts for downstream systems that consume files (CSV, Parquet, JSON)
- Delivering data to external partners via shared storage
- Exporting data for systems that cannot read Delta Lake tables
- Creating archive snapshots in standard file formats

### When NOT to Use

- Writing to Delta Lake tables (use Overwrite, Append, T2CL, SCD2, or SCD4)
- Internal data movement within the lakehouse (prefer table-based patterns)

---

## How It Works {#how-it-works}

```
Incoming DataFrame
        │
        ▼
┌───────────────────┐
│ Validate format   │
│ (parquet, csv,    │
│  json, delta)     │
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│ Count rows        │
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│ Apply format opts │
│ (CSV: header,     │
│  delimiter, etc.) │
└───────┬───────────┘
        │
        ▼
┌───────────────────┐
│ Write to path     │
│ mode: overwrite   │
│   or append       │
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

1. **Validate format** — Checks that the format is one of: `parquet`, `csv`, `json`, `delta`. Raises `ValueError` for unsupported formats
2. **Count rows** — Counts DataFrame rows for statistics
3. **Apply format options** — For CSV, adds `header: true` by default and applies any additional format-specific options (delimiter, quote character, etc.)
4. **Write** — Writes to the specified output path using the selected format and mode
5. **Partition (optional)** — If `partition_cols` are specified, output is partitioned into subdirectories
6. **Log** — Records statistics to `PipelineStats`

---

## Supported Formats {#supported-formats}

| Format | Extension | Use Case | Notes |
|--------|-----------|----------|-------|
| `parquet` | `.parquet` | High-performance columnar format | Default. Best for analytics and large datasets |
| `csv` | `.csv` | Universal text format | Supports header, custom delimiter, quoting |
| `json` | `.json` | Semi-structured data | One JSON object per line (JSON Lines format) |
| `delta` | N/A | Delta Lake format on file path | ACID transactions, time travel on file path |

---

## Contract Configuration {#contract-configuration}

### CSV Export Contract

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.exports.customer_feed

schema:
  name: exports
  table: customer_feed
  format: csv

customProperties:
  pipelineType: transformation
  writeStrategy: file_export
  exportPath: /mnt/exports/customer_feed
  exportFormat: csv
  csvOptions:
    header: "true"
    delimiter: "|"
```

### Parquet Export Contract

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.exports.transaction_archive

schema:
  name: exports
  table: transaction_archive
  format: parquet

customProperties:
  pipelineType: transformation
  writeStrategy: file_export
  exportPath: /mnt/archives/transactions/2026
  exportFormat: parquet
```

---

## Parameters {#parameters}

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | `DataFrame` | *required* | The DataFrame to export |
| `output_path` | `str` | *required* | Destination file system path (e.g., `/mnt/exports/data`) |
| `file_format` | `str` | `"parquet"` | Output format: `parquet`, `csv`, `json`, or `delta` |
| `mode` | `str` | `"overwrite"` | Write mode: `overwrite` or `append` |
| `partition_cols` | `list` or `None` | `None` | Columns to partition by (creates subdirectories) |
| `**format_options` | keyword args | — | Format-specific options passed to the writer (e.g., `delimiter="\|"` for CSV) |

---

## Return Statistics {#return-statistics}

```python
{
    "strategy": "file_export",
    "output_path": "/mnt/exports/customer_feed",
    "file_format": "csv",
    "rows_exported": 25000,
    "mode": "overwrite",
    "partitioned": False
}
```

| Field | Type | Description |
|-------|------|-------------|
| `strategy` | `str` | Always `"file_export"` |
| `output_path` | `str` | The path data was written to |
| `file_format` | `str` | The format used |
| `rows_exported` | `int` | Number of rows written |
| `mode` | `str` | Write mode used |
| `partitioned` | `bool` | Whether partitioning was applied |

---

## CSV Options {#csv-options}

When `file_format="csv"`, the writer automatically adds `header: true`. Additional options can be passed as keyword arguments:

| Option | Default | Description |
|--------|---------|-------------|
| `header` | `"true"` | Include column headers in the first row |
| `delimiter` | `","` | Field separator character |
| `quote` | `'"'` | Character used for quoting fields |
| `escape` | `'\'` | Escape character for special characters |
| `nullValue` | `""` | String representation of null values |
| `dateFormat` | `"yyyy-MM-dd"` | Format for date columns |
| `timestampFormat` | `"yyyy-MM-dd HH:mm:ss"` | Format for timestamp columns |

### Example: Pipe-Delimited CSV

```python
writer = IOFactory.create_writer("file_export", context, stats)
write_stats = writer.write(
    df,
    output_path="/mnt/exports/data.csv",
    file_format="csv",
    delimiter="|",
    nullValue="NULL"
)
```

---

## Examples {#examples}

### Example 1: Parquet Export (Default)

```python
writer = IOFactory.create_writer("file_export", context, stats)
write_stats = writer.write(
    df,
    output_path="/mnt/exports/customer_data_20260209.parquet",
    file_format="parquet"
)
```

### Example 2: CSV with Custom Delimiter

```python
writer = IOFactory.create_writer("file_export", context, stats)
write_stats = writer.write(
    df,
    output_path="/mnt/exports/daily_feed.csv",
    file_format="csv",
    delimiter="|",
    nullValue="N/A"
)
```

### Example 3: JSON Export

```python
writer = IOFactory.create_writer("file_export", context, stats)
write_stats = writer.write(
    df,
    output_path="/mnt/exports/events.json",
    file_format="json"
)
```

### Example 4: Partitioned Export

```python
writer = IOFactory.create_writer("file_export", context, stats)
write_stats = writer.write(
    df,
    output_path="/mnt/exports/transactions",
    file_format="parquet",
    partition_cols=["region", "date"]
)
# Creates: /mnt/exports/transactions/region=UK/date=2026-02-09/part-00000.parquet
```

### Example 5: Append Mode

```python
# Add new files without removing existing ones
writer = IOFactory.create_writer("file_export", context, stats)
write_stats = writer.write(
    df,
    output_path="/mnt/exports/event_log",
    file_format="parquet",
    mode="append"
)
```

---

## FAQs {#faqs}

**Q: Does File Export write a single file or multiple files?**
A: Spark writes one file per partition of the DataFrame. For a non-partitioned write, the number of output files equals the number of DataFrame partitions. Use `df.coalesce(1)` before writing if you need a single file.

**Q: Can I export to cloud storage (S3, ADLS, GCS)?**
A: Yes. The `output_path` can be any path accessible from your Spark cluster, including cloud storage paths like `abfss://container@account.dfs.core.windows.net/path` or `s3a://bucket/path`.

**Q: What happens if the output path already exists?**
A: With `mode="overwrite"` (default), existing data at the path is replaced. With `mode="append"`, new files are added alongside existing ones.

**Q: Can I use File Export for Delta table paths?**
A: Yes, by setting `file_format="delta"`. This writes Delta format to a file path (not registered as a table). Useful for portable Delta datasets.

**Q: Is there a file size limit?**
A: No hard limit from the writer. File size is determined by the data volume and Spark partition count. For very large exports, partitioning by a column helps manage file sizes.

---

_This is a living document. Last updated: February 2026_
