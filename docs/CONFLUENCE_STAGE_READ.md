# NovaFlow Pipeline Stage: Read

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**: [Pipeline Stages Overview](CONFLUENCE_PIPELINE_STAGES_OVERVIEW.md)

---

## Overview

The **ReadStage** is the entry point for data pipelines. It reads data from source files or existing tables using `IOFactory` and produces the initial DataFrame that flows through subsequent stages.

| Property | Value |
|----------|-------|
| **Class** | `ReadStage` |
| **File** | `nova_framework/pipeline/stages/read_stage.py` |
| **Stage Name** | `Read` |
| **Pipeline Position** | First |
| **Skip Condition** | Never skipped |
| **Modifies DataFrame** | N/A — produces the initial DataFrame |

---

## How It Works

1. Creates a reader via `IOFactory.create_reader(reader_type, context, stats)`
2. Calls `reader.read(verbose=True)` which returns `(DataFrame, read_report)`
3. Stores the `read_report` dictionary in `context.state["read_report"]`
4. Logs `rows_read` and `rows_invalid` to `PipelineStats`
5. Returns the DataFrame

### Reader Types

| Type | Used In | Reads From |
|------|---------|-----------|
| `"file"` | Ingestion pipelines | Source files (Parquet, CSV, JSON) at `data_file_path` |
| `"table"` | Validation pipelines | Existing Delta tables via `spark.table()` |

---

## Parameters

```python
ReadStage(context, stats, reader_type="file")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `context` | `ExecutionContext` | *required* | Pipeline execution context |
| `stats` | `PipelineStats` | *required* | Statistics tracker |
| `reader_type` | `str` | `"file"` | `"file"` for source files, `"table"` for existing tables |

---

## Context State

| Key | Written By | Type | Contents |
|-----|-----------|------|----------|
| `read_report` | ReadStage | `dict` | `total_rows`, `valid_rows`, `invalid_rows`, `rejection_rate_pct` |

---

## Statistics Logged

| Metric | Description |
|--------|-------------|
| `rows_read` | Total rows read from source |
| `rows_invalid` | Invalid rows detected during read (if any) |
| `stage_Read_duration_seconds` | Execution time |

---

## Example Log Output

```
[Read] Starting...
Reading data using file reader
Read complete: 10000 total rows, 9500 valid, 500 invalid
[Read] Completed in 2.34s
```

---

## Contract Configuration

The reader's behaviour is configured through the data contract schema and custom properties:

```yaml
schema:
  name: bronze_sales
  table: customers
  format: parquet          # File format for file reader

customProperties:
  pipelineType: ingestion  # Uses reader_type="file"
  volume: M                # Affects partitioning
```

For validation pipelines, the reader reads from the target table itself:

```yaml
customProperties:
  pipelineType: validation  # Uses reader_type="table"
```

---

_This is a living document. Last updated: February 2026_
