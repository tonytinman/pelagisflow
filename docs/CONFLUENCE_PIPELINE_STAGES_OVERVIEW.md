# NovaFlow Pipeline Stages

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**:
- [NovaFlow Overview](#)
- [Write Patterns Overview](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md)
- [Data Privacy Control](CONFLUENCE_DATA_PRIVACY.md)
- [ReadStage](CONFLUENCE_STAGE_READ.md) | [LineageStage](CONFLUENCE_STAGE_LINEAGE.md) | [HashingStage](CONFLUENCE_STAGE_HASHING.md) | [DeduplicationStage](CONFLUENCE_STAGE_DEDUPLICATION.md) | [QualityStage](CONFLUENCE_STAGE_QUALITY.md) | [TransformationStage](CONFLUENCE_STAGE_TRANSFORMATION.md) | [WriteStage](CONFLUENCE_STAGE_WRITE.md) | [PrivacyStage](CONFLUENCE_STAGE_PRIVACY.md) | [AccessControlStage](CONFLUENCE_STAGE_ACCESS_CONTROL.md)

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Pipeline Types](#pipeline-types)
4. [Stage Reference](#stage-reference)
5. [Stage Execution Flow](#stage-execution-flow)
6. [Skip Conditions](#skip-conditions)
7. [Inter-Stage Communication](#inter-stage-communication)
8. [Error Handling](#error-handling)
9. [Observability](#observability)
10. [Extending the Framework](#extending-the-framework)
11. [Getting Help](#getting-help)

---

## Overview {#overview}

NovaFlow pipelines are composed of **stages** — discrete, single-responsibility processing steps that are executed sequentially. Each stage receives a DataFrame from the previous stage, transforms or acts on it, and passes the result to the next.

### Key Principles

- **Single Responsibility**: Each stage does one thing well
- **Contract-Driven**: Stage behaviour is configured through data contracts, not code
- **Conditional Execution**: Stages can be skipped when their prerequisites are not met
- **Template Method Pattern**: `BasePipeline` defines the execution skeleton; subclasses define the stage list
- **Factory Pattern**: `PipelineFactory` creates the correct pipeline based on contract `pipelineType`

### The 9 Stages

| # | Stage | Purpose | Modifies Data | Required |
|---|-------|---------|:------------:|:--------:|
| 1 | [ReadStage](CONFLUENCE_STAGE_READ.md) | Read data from source files or tables | — | Yes |
| 2 | [LineageStage](CONFLUENCE_STAGE_LINEAGE.md) | Add provenance tracking columns | Yes | Yes |
| 3 | [HashingStage](CONFLUENCE_STAGE_HASHING.md) | Add natural key, change key, and partition hashes | Yes | Conditional |
| 4 | [DeduplicationStage](CONFLUENCE_STAGE_DEDUPLICATION.md) | Remove duplicate rows using hash columns | Yes | Conditional |
| 5 | [QualityStage](CONFLUENCE_STAGE_QUALITY.md) | Cleanse data and validate quality rules | Yes | Conditional |
| 6 | [TransformationStage](CONFLUENCE_STAGE_TRANSFORMATION.md) | Execute SQL, Python, or Scala transformations | Yes | Transformation pipelines |
| 7 | [WriteStage](CONFLUENCE_STAGE_WRITE.md) | Persist data using configured write strategy | No (side-effect) | Yes |
| 8 | [PrivacyStage](CONFLUENCE_STAGE_PRIVACY.md) | Apply column-level masking in Unity Catalog | No (UC metadata) | Optional |
| 9 | [AccessControlStage](CONFLUENCE_STAGE_ACCESS_CONTROL.md) | Enforce table-level GRANT/REVOKE in Unity Catalog | No (UC metadata) | Optional |

---

## Architecture {#architecture}

```
┌───────────────────────────────────────────────────────────────────┐
│                     Pipeline Orchestrator                         │
│  Pipeline.run(process_queue_id, contract_name, source_ref, env)  │
└─────────────────────────┬─────────────────────────────────────────┘
                          │
                          ▼
┌───────────────────────────────────────────────────────────────────┐
│                     ExecutionContext                               │
│  process_queue_id, contract, catalog, state: Dict[str, Any]      │
└─────────────────────────┬─────────────────────────────────────────┘
                          │
                          ▼
┌───────────────────────────────────────────────────────────────────┐
│                     PipelineFactory                                │
│  contract.pipelineType → IngestionPipeline                        │
│                        → TransformationPipeline                    │
│                        → ValidationPipeline                        │
└─────────────────────────┬─────────────────────────────────────────┘
                          │
                          ▼
┌───────────────────────────────────────────────────────────────────┐
│                     BasePipeline.execute()                         │
│                                                                    │
│  for stage in self.build_stages():                                │
│      if stage.skip_condition():                                   │
│          continue                                                  │
│      df = stage.run(df)  ← logging, timing, error handling        │
└───────────────────────────────────────────────────────────────────┘
```

### AbstractStage Base Class

Every stage inherits from `AbstractStage` and implements `execute()`:

```python
class AbstractStage(ABC):
    def __init__(self, context: ExecutionContext, stats: PipelineStats, stage_name: str)

    @abstractmethod
    def execute(self, df: Optional[DataFrame]) -> DataFrame
        # Subclass implements transformation logic

    def run(self, df: Optional[DataFrame]) -> DataFrame
        # Template wrapper: logs start → calls execute() → logs duration → handles errors

    def skip_condition(self) -> bool
        # Override to conditionally skip (default: False)
```

The `run()` method wraps `execute()` with:
- Stage start/completion logging
- Duration timing recorded to `PipelineStats`
- Exception logging with stage name context

---

## Pipeline Types {#pipeline-types}

### Ingestion Pipeline

**Contract value**: `pipelineType: ingestion`
**Use case**: Loading raw data from external source files into bronze-layer Delta tables.

```
Read(file) → Lineage → Hashing → Deduplication → Quality → Write
```

```yaml
customProperties:
  pipelineType: ingestion
  writeStrategy: type_2_change_log
  softDelete: true
```

### Transformation Pipeline

**Contract value**: `pipelineType: transformation`
**Use case**: Transforming bronze data into silver/gold curated datasets.

```
Transformation → Lineage → Hashing → Deduplication → Quality → Write
```

```yaml
customProperties:
  pipelineType: transformation
  transformationSql: |
    SELECT customer_id, COUNT(*) as total_orders
    FROM bronze_sales.orders GROUP BY customer_id
  writeStrategy: overwrite
```

### Validation Pipeline

**Contract value**: `pipelineType: validation`
**Use case**: Ad-hoc data quality audits on existing tables. Does NOT write to the target table.

```
Read(table) → Quality
```

```yaml
customProperties:
  pipelineType: validation
```

### Custom Pipelines

Teams can register custom pipeline types:

```python
class MyCustomPipeline(BasePipeline):
    def build_stages(self):
        return [
            ReadStage(self.context, self.stats),
            TransformationStage(self.context, self.stats),
            WriteStage(self.context, self.stats)
        ]

PipelineFactory.register_pipeline("my_custom", MyCustomPipeline)
```

---

## Stage Reference {#stage-reference}

### Quick Reference Table

| Stage | Class | File | Columns Added | Context State Written | Skip When |
|-------|-------|------|---------------|-----------------------|-----------|
| Read | `ReadStage` | `read_stage.py` | — | `read_report` | Never |
| Lineage | `LineageStage` | `lineage_stage.py` | `source_file_path`, `source_file_name`, `process_queue_id`, `processed_at`, `processed_by` | — | Never |
| Hashing | `HashingStage` | `hashing_stage.py` | `natural_key_hash`, `change_key_hash`, `partition_key` | — | No `natural_key_columns` in contract |
| Dedup | `DeduplicationStage` | `deduplication_stage.py` | — (removes rows) | — | No `natural_key_columns` in contract |
| Quality | `QualityStage` | `quality_stage.py` | `dq_score` | — | No cleansing or quality rules |
| Transform | `TransformationStage` | `transformation_stage.py` | Replaces DataFrame entirely | — | Never (transform pipelines) |
| Write | `WriteStage` | `write_stage.py` | — | `write_stats` | Never |
| Privacy | `PrivacyStage` | `privacy_stage.py` | — | — | Not in default pipelines |
| Access | `AccessControlStage` | `access_control_stage.py` | — | — | `accessControl.enabled == false` |

### Stage Dependency Chain

```
                    ┌───────────────────────────────────────────┐
                    │            Data Dependencies              │
                    └───────────────────────────────────────────┘

Read ──────► Lineage ──────► Hashing ──────► Dedup ──────► Quality ──────► Write
  │                            │                │                            │
  │ Produces DF               │ Produces       │ Requires                  │ Requires
  │ from source               │ hash cols      │ hash cols                 │ hash cols
  │                            │                │                          │ (for T2CL/
  │                            │                │                          │  SCD2/SCD4)
  └────────────────────────────┴────────────────┘

Transform ──► Lineage ──► Hashing ──► Dedup ──► Quality ──► Write
  │
  │ Produces new DF
  │ from SQL/Python/Scala
```

---

## Stage Execution Flow {#stage-execution-flow}

### Per-Stage Lifecycle

```
┌─────────────────────────────────────────────────────┐
│ BasePipeline.execute()                               │
│                                                      │
│  for each stage:                                     │
│    1. Check skip_condition() ──── true ──► skip      │
│                                    │                 │
│                                  false               │
│                                    ▼                 │
│    2. stage.run(df)                                  │
│       ├── Log "[Stage] Starting..."                  │
│       ├── Record start time                          │
│       ├── Call stage.execute(df)                     │
│       ├── Record duration                            │
│       ├── Log "[Stage] Completed in X.XXs"           │
│       └── Write duration to PipelineStats            │
│                │                                     │
│                ▼                                     │
│    3. df = result (pass to next stage)               │
└─────────────────────────────────────────────────────┘
```

### Full Ingestion Pipeline Execution

```
Pipeline Start
    │
    ▼
[Read] ──► df = {raw rows from source files}
    │       context.state["read_report"] = { total_rows, valid_rows, invalid_rows }
    ▼
[Lineage] ──► df += source_file_path, source_file_name, process_queue_id,
    │                 processed_at, processed_by
    ▼
[Hashing] ──► df += natural_key_hash (SHA-256 of isPrimaryKey columns)
    │               + change_key_hash (SHA-256 of isChangeTracking columns)
    │               + partition_key (modulo hash for distribution)
    ▼
[Dedup] ──► df -= duplicate rows (by natural_key_hash + change_key_hash)
    │
    ▼
[Quality] ──► df cleansed (trim, uppercase, etc.)
    │          df += dq_score column
    │          DQ errors → violations table (side-effect)
    ▼
[Write] ──► df → Delta table (using writeStrategy from contract)
    │       context.state["write_stats"] = { strategy, rows_written, ... }
    ▼
Pipeline Complete → "SUCCESS"
```

---

## Skip Conditions {#skip-conditions}

Stages can be conditionally skipped without failing the pipeline. The `skip_condition()` method is checked before `execute()` is called.

| Stage | Skips When | Reason |
|-------|-----------|--------|
| **Read** | Never | First stage — always runs |
| **Lineage** | Never | Always adds tracking columns |
| **Hashing** | `contract.natural_key_columns` is empty | No columns to hash |
| **Dedup** | `contract.natural_key_columns` is empty | No hash columns for deduplication |
| **Quality** | No `cleansing_rules` AND no `quality_rules` in contract | Nothing to validate |
| **Transform** | Never | Core of transformation pipeline |
| **Write** | Never | Must persist data |
| **Privacy** | Not included in default pipelines (add-on) | N/A |
| **Access** | `contract.accessControl.enabled == false` | Explicitly disabled |

When a stage is skipped, the pipeline logs `[StageName] Skipped (skip condition met)` and passes the DataFrame unchanged to the next stage.

---

## Inter-Stage Communication {#inter-stage-communication}

### 1. DataFrame Piping (Primary)

The main communication mechanism. Each stage receives a DataFrame, transforms it, and returns a new DataFrame:

```python
df = ReadStage.execute(None)         # → raw DF
df = LineageStage.execute(df)        # → DF + lineage cols
df = HashingStage.execute(df)        # → DF + hash cols
df = DeduplicationStage.execute(df)  # → DF with dupes removed
df = QualityStage.execute(df)        # → DF + dq_score
WriteStage.execute(df)               # → DF written to table
```

### 2. Context State (Secondary)

Stages can store and retrieve key-value state via `ExecutionContext`:

```python
# Writing state
context.set_state("read_report", {"total_rows": 10000, "valid_rows": 9500})

# Reading state (in a later stage or post-pipeline)
report = context.get_state("read_report")
```

| Key | Set By | Contains |
|-----|--------|----------|
| `read_report` | ReadStage | `total_rows`, `valid_rows`, `invalid_rows` |
| `write_stats` | WriteStage | `strategy`, `rows_written`, `new_records`, `changed_records`, etc. |

### 3. PipelineStats (Metrics)

All stages log metrics to the shared `PipelineStats` tracker:

```python
self.stats.log_rows_read(10000)
self.stats.log_rows_written(9500)
self.stats.log_stat("dq_failed_pct", 2.5)
self.stats.log_stat("stage_Read_duration_seconds", 3.21)
```

---

## Error Handling {#error-handling}

### Fatal Errors (Fail Pipeline)

Exceptions raised in `execute()` propagate through `run()` and fail the entire pipeline:

| Stage | Fatal Error Scenarios |
|-------|-----------------------|
| Read | Source files not found, reader creation fails |
| Lineage | Spark operation failure |
| Hashing | Invalid column references |
| Dedup | Spark operation failure |
| Quality | N/A (DQ error writing is non-fatal) |
| Transform | Invalid SQL, missing module, execution failure |
| Write | Table creation failure, Delta write failure |
| Privacy | Enforcement failure when `allowPartialFailure == false` |
| Access | UC privilege query failure |

### Non-Fatal Errors (Log and Continue)

| Stage | Non-Fatal Scenario | Behaviour |
|-------|-------------------|-----------|
| Quality | DQ error writing to violations table fails | Logged as error, pipeline continues |
| Hashing | No change_tracking_columns defined | Logged as warning, skips change_key_hash |
| Dedup | No hash columns found in DataFrame | Logged as warning, returns DF unchanged |
| Privacy | Enforcement fails when `allowPartialFailure == true` | Logged as error, pipeline continues |
| Access | Metadata loading fails | Logged as warning, returns DF unchanged |

### Error Output

```
[Read] Starting...
[Read] Completed in 2.34s
[Lineage] Starting...
[Lineage] Completed in 0.12s
[Hashing] Starting...
[Hashing] Failed: Column 'customer_id' not found in DataFrame
[IngestionPipeline] Pipeline failed: Column 'customer_id' not found in DataFrame
```

---

## Observability {#observability}

### Timing Metrics

Every stage records its duration:

```
stage_Read_duration_seconds: 2.34
stage_Lineage_duration_seconds: 0.12
stage_Hashing_duration_seconds: 0.45
stage_Deduplication_duration_seconds: 1.08
stage_DataQuality_duration_seconds: 3.21
stage_Write_duration_seconds: 5.67
```

### Stage-Specific Metrics

| Stage | Metrics Logged |
|-------|---------------|
| Read | `rows_read`, `rows_invalid` |
| Hashing | — |
| Dedup | Duplicate count in log messages |
| Quality | `dq_total_rows`, `dq_failed_rows`, `dq_failed_pct` |
| Transform | `rows_transformed`, `transformation_type` |
| Write | `rows_written`, strategy-specific (see [Write Patterns](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md)) |
| Privacy | `policies_created`, `policies_dropped`, `policies_failed`, `execution_time_seconds` |
| Access | `access_grants_attempted/succeeded/failed`, `access_revokes_attempted/succeeded/failed`, `access_execution_seconds` |

### Logging Convention

All stages use structured loggers: `pipeline.stages.{StageName}`

```python
# Example log output
pipeline.stages.Read    [Read] Starting...
pipeline.stages.Read    Reading data using file reader
pipeline.stages.Read    Read complete: 10000 total rows, 9500 valid, 500 invalid
pipeline.stages.Read    [Read] Completed in 2.34s
```

---

## Extending the Framework {#extending-the-framework}

### Creating a Custom Stage

```python
from nova_framework.pipeline.stages.base import AbstractStage

class MyCustomStage(AbstractStage):
    def __init__(self, context, stats):
        super().__init__(context, stats, "MyCustom")

    def execute(self, df):
        self.logger.info("Doing custom work")
        # Transform df...
        return df

    def skip_condition(self):
        # Optional: return True to skip
        return False
```

### Adding a Stage to a Pipeline

```python
class CustomIngestionPipeline(BasePipeline):
    def build_stages(self):
        return [
            ReadStage(self.context, self.stats, reader_type="file"),
            LineageStage(self.context, self.stats),
            MyCustomStage(self.context, self.stats),   # ← added
            HashingStage(self.context, self.stats),
            DeduplicationStage(self.context, self.stats),
            QualityStage(self.context, self.stats),
            WriteStage(self.context, self.stats)
        ]

PipelineFactory.register_pipeline("custom_ingestion", CustomIngestionPipeline)
```

### Registering a Custom Pipeline

```python
PipelineFactory.register_pipeline("my_type", MyCustomPipeline)
# Now usable in contracts:
# customProperties:
#   pipelineType: my_type
```

---

## Getting Help {#getting-help}

### Individual Stage Documentation

- [ReadStage](CONFLUENCE_STAGE_READ.md) — Source data ingestion from files and tables
- [LineageStage](CONFLUENCE_STAGE_LINEAGE.md) — Provenance tracking columns
- [HashingStage](CONFLUENCE_STAGE_HASHING.md) — Natural key, change key, and partition hashing
- [DeduplicationStage](CONFLUENCE_STAGE_DEDUPLICATION.md) — Duplicate row removal
- [QualityStage](CONFLUENCE_STAGE_QUALITY.md) — Data cleansing and validation
- [TransformationStage](CONFLUENCE_STAGE_TRANSFORMATION.md) — SQL, Python, and Scala transformations
- [WriteStage](CONFLUENCE_STAGE_WRITE.md) — Data persistence with write strategies
- [PrivacyStage](CONFLUENCE_STAGE_PRIVACY.md) — Column-level masking enforcement
- [AccessControlStage](CONFLUENCE_STAGE_ACCESS_CONTROL.md) — Table-level GRANT/REVOKE enforcement

### Related Documentation

- [Write Patterns Overview](CONFLUENCE_WRITE_PATTERNS_OVERVIEW.md)
- [Data Privacy Control](CONFLUENCE_DATA_PRIVACY.md)
- [Engineering Standards Checklist](ENGINEERING_STANDARDS_CHECKLIST.md)

### Support Channels

- **Slack**: #novaflow-support
- **Email**: novaflow-team@company.com
- **Office Hours**: Tuesdays & Thursdays 2-3pm GMT

---

_This is a living document. Last updated: February 2026_
