# NovaFlow — Data Platform Governance Review

**Document Purpose:** Pre-production governance review covering core functionality, solution architecture, and principles of how NovaFlow implements data management, quality, security, lineage, and observability.

**API Version:** ODCS v3.0.2
**Target Platform:** Databricks (Unity Catalog, Delta Lake, Spark)

---

## 1. Executive Summary

NovaFlow is a contract-driven data pipeline framework for Databricks. Every pipeline execution is governed by a declarative YAML data contract that defines the output schema, transformation logic, data quality rules, write strategy, and access control policy for a single data asset. The framework enforces these contracts at runtime — there is no pathway for data to be written without passing through the contract-defined quality and lineage stages.

The framework supports the medallion architecture (Bronze / Silver / Gold) and provides standardised handling for ingestion, transformation, and validation workloads. Pipeline type is derived from the contract's schema naming convention, ensuring consistent classification without manual configuration.

### Key Governance Properties

| Property | How NovaFlow Addresses It |
|---|---|
| **Data Quality** | Weighted, row-level quality scoring with configurable cleansing and validation rules defined in the contract |
| **Data Lineage** | Automatic lineage columns on every row; per-run statistics persisted to a central table |
| **Access Control** | Desired-state privilege reconciliation against Unity Catalog; differential GRANT/REVOKE |
| **Schema Governance** | Contract-defined schemas validated at read time; breaking changes detected before processing |
| **Auditability** | Structured logging to Delta, pipeline statistics, and data quality error records all keyed by a unique process identifier |
| **Change History** | SCD Type 2 write strategy preserves full history with effective dating and soft delete |

---

## 2. Solution Architecture

### 2.1 High-Level Data Flow

```
  Data Contract (YAML)
         │
         ▼
  ┌─────────────┐
  │  Pipeline    │  Orchestrator creates ExecutionContext,
  │  Orchestrator│  selects pipeline type from contract,
  │              │  emits telemetry
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  Pipeline    │  Factory selects: Ingestion, Transformation,
  │  Factory     │  or Validation pipeline
  └──────┬──────┘
         │
         ▼
  ┌──────────────────────────────────────────────────┐
  │  Stage Sequence (varies by pipeline type)         │
  │                                                   │
  │  Read/Transform → Lineage → Hashing → Dedup      │
  │       → Data Quality → Write → Access Control     │
  └──────────────────────────────────────────────────┘
         │
         ▼
  Pipeline Statistics persisted to Delta
```

### 2.2 Contract-Driven Execution

All runtime behaviour is determined by the data contract. The framework does not accept ad-hoc configuration at execution time beyond four parameters: process queue ID, contract name, source reference, and environment. This constrains the blast radius of any single execution and ensures that all behaviour is auditable via the contract definition.

**Pipeline entry point signature:**

```
Pipeline.run(process_queue_id, data_contract_name, source_ref, env)
```

No additional parameters are accepted. All transformation logic, quality rules, write strategy, and access control policy are read from the named contract.

### 2.3 Pipeline Types

Pipeline type is inferred from the contract's schema name prefix. This enforces a consistent relationship between naming convention and pipeline behaviour.

| Schema Prefix | Inferred Pipeline Type | Stage Sequence |
|---|---|---|
| `bronze_` | Ingestion | Read → Lineage → Hashing → Dedup → Quality → Write |
| `silver_` | Transformation | Transform → Lineage → Hashing → Dedup → Quality → Write |
| `gold_` | Transformation | Transform → Lineage → Hashing → Dedup → Quality → Write |

### 2.4 Environment Isolation

The framework uses an environment parameter (`dev`, `test`, `prod`) to construct all catalog and storage paths. This produces physically separate catalogs per environment (e.g., `cluk_dev_nova`, `cluk_test_nova`, `cluk_prod_nova`), ensuring that development workloads cannot inadvertently access or modify production data.

---

## 3. Data Contract Structure

Each data asset is defined by a single YAML contract aligned to the Open Data Contract Standard (ODCS) v3.0.2. The contract is the single source of truth for:

- **Identity:** Name, version, domain, data product
- **Ownership:** Data owner, senior manager, data steward
- **Schema:** Column names, types, nullability, primary keys, change-tracking columns
- **Transformation:** Type (SQL / Python / Scala), module, class/function, runtime configuration
- **Write Strategy:** How data is persisted (overwrite, append, SCD2, SCD4, file export)
- **Data Quality:** Cleansing rules (pre-processing) and validation rules (post-processing with severity and weighting)
- **Access Control:** Privilege policies for the output asset
- **Governance Metadata:** Data classification, privacy tags, volume sizing

### 3.1 Contract Example (Abridged)

```yaml
apiVersion: v3.0.2
kind: DataContract
name: product_sales_summary
version: "1.0.0"
domain: sales
dataProduct: product_analytics

team:
  dataOwner: owner@example.com
  seniorManager: manager@example.com
  dataSteward: steward@example.com

schema:
  name: gold_sales.product_sales_summary
  table: product_sales_summary
  format: delta
  properties:
    - name: product_id
      type: string
      isPrimaryKey: true
      isNullable: false
    - name: total_revenue
      type: decimal(18,2)
      isNullable: false

customProperties:
  pipelineType: transformation
  writeStrategy: overwrite
  transformationType: python
  transformationModule: product_sales_summary
  transformationClass: ProductSalesSummary
  transformationConfig:
    source_catalog: bronze
    lookback_days: 30

quality:
  - type: transformation
    rule: trim_string_fields
  - rule: not_null
    column: product_id
    severity: critical
  - rule: min
    column: total_revenue
    value: 0
    severity: error
```

### 3.2 Contract Storage and Loading

Contracts are stored as YAML files in Unity Catalog Volumes at a deterministic path:

```
/Volumes/cluk_{env}_nova/nova_framework/data_contracts/{contract_name}.yaml
```

The framework raises a `FileNotFoundError` if the named contract does not exist, preventing execution against undefined assets.

---

## 4. Data Quality

### 4.1 Two-Phase Quality Processing

Data quality is applied in two distinct phases, both defined declaratively in the contract:

**Phase 1 — Cleansing (mutating).** Standardisation rules applied before validation. These modify the data to bring it into a consistent form. Supported operations:

| Rule | Behaviour |
|---|---|
| `trim_string_fields` | Remove leading/trailing whitespace from string columns |
| `uppercase` / `lowercase` | Case normalisation |
| `replace_empty_strings_with_null` | Convert empty strings to NULL |
| `nullify_empty_strings` | Convert empty strings to NULL |
| `collapse_whitespace` | Replace multiple whitespace characters with a single space |
| `normalize_boolean_values` | Map truthy/falsy string forms to canonical True/False |
| `remove_control_characters` | Strip non-printable characters |

Cleansing rules support column wildcards (`columns: ["*"]`) with optional type filtering, allowing a single rule to apply across all string columns.

**Phase 2 — Validation (non-destructive).** Quality rules that annotate rows with scores and failure details without modifying the original data. Supported checks:

| Rule | Parameters |
|---|---|
| `not_null` | — |
| `not_blank` | — |
| `unique` | — |
| `regex` | `pattern` |
| `allowed_values` | `values` (list) |
| `min` / `max` | `value` |
| `between` | `min`, `max` |
| `min_length` / `max_length` | `value` |
| `digits_only` / `letters_only` | — |
| `is_number` | — |
| `is_date` | `format` (default `yyyy-MM-dd`) |
| `composite_unique` | Multiple columns |
| `conditional` | Custom SQL expression |

### 4.2 Weighted Quality Scoring

Each validation rule carries a severity level mapped to a numeric weight:

| Severity | Weight |
|---|---|
| `critical` | 5 |
| `warning` | 3 |
| `info` | 1 |

The framework computes a per-row quality score:

```
dq_score = ((total_weight − failed_weight) / total_weight) × 100
```

This score is added as a column to the output data, enabling downstream consumers to filter or flag rows based on their own quality thresholds. The original data columns are never modified — quality metadata is appended as additional columns (`_dq_failures`, `_dq_fail_weight`, `dq_score`, `_dq_error`).

### 4.3 Quality Error Persistence

Rows that fail any validation rule are extracted and persisted to a dedicated error table (`{catalog}.nova_framework.dq_errors`) with full failure detail: rule name, column, weight, and the value that failed. This write is non-blocking — if the error table write fails, the pipeline continues and the failure is logged but does not halt processing.

---

## 5. Schema Validation and Evolution

### 5.1 Contract-Driven Schema Enforcement

The expected output schema is defined in the data contract's `schema.properties` section. At read time, the framework:

1. Reads a sample from the source file to determine the actual schema
2. Compares actual vs. expected column names and types
3. Detects **missing columns** (in contract but not in source) and **extra columns** (in source but not in contract)
4. Classifies missing columns as **breaking changes**
5. Returns a validation report with a `has_breaking_changes` boolean

The pipeline can be configured to fail on schema changes (`fail_on_schema_change=True`) or to continue with a warning.

### 5.2 Bad Row Isolation

For semi-structured formats (CSV, JSON), the framework uses Spark's corrupt record handling to identify rows that cannot be parsed against the expected schema. These rows are:

1. Separated from valid data before any processing occurs
2. Enriched with metadata (timestamp, source file, process queue ID)
3. Written to a quarantine location (`/Volumes/{catalog}/nova_framework/quarantine/{date}/`)
4. Counted and reported in the pipeline statistics

A configurable rejection threshold (`invalid_row_threshold`, default 10%) can optionally halt the pipeline if the proportion of bad rows exceeds acceptable limits.

---

## 6. Write Strategies and Change History

The contract's `writeStrategy` property determines how data is persisted. All strategies write to Delta Lake tables in Unity Catalog.

### 6.1 Available Strategies

| Strategy | Use Case | History Preservation |
|---|---|---|
| **SCD Type 2** | Slowly changing dimensions | Full history with effective dating |
| **SCD Type 4** | Dimensions with separate history | Current + historical tables |
| **Overwrite** | Reference data, full refreshes | No history (replaced each run) |
| **Append** | Facts, events, logs | Additive only |
| **File Export** | External system delivery | N/A |

### 6.2 SCD Type 2 — Change Detection

SCD2 is the default write strategy when none is specified. Change detection uses hash-based comparison:

1. **Natural Key Hash** — Computed from the contract's `isPrimaryKey` columns. Identifies the same logical entity across loads.
2. **Change Key Hash** — Computed from the contract's `isChangeTracking` columns. Detects whether an entity's attributes have changed.

The merge logic categorises incoming rows into three groups:

| Category | Condition | Action |
|---|---|---|
| **New** | Natural key not in current table | Insert with `is_current=true`, `effective_from=today`, `effective_to=9999-12-31` |
| **Changed** | Natural key exists, change hash differs | Close existing row (`is_current=false`, `effective_to=today`); insert new row |
| **Deleted** | Natural key in current but absent from incoming (soft delete enabled) | Mark existing row `is_current=false`, `deletion_flag=true` |

This approach preserves a complete audit trail of all changes to dimension data, with the ability to query the state of any record at any point in time via effective date filtering.

### 6.3 Soft Delete

When enabled via the contract (`softDelete: true`), records present in the current table but absent from the incoming dataset are flagged rather than physically removed. The `deletion_flag` column is set to `true` and `is_current` is set to `false`, preserving the record for audit purposes.

---

## 7. Data Lineage

### 7.1 Row-Level Lineage

Every row passing through the framework is automatically enriched with lineage columns by the Lineage Stage:

| Column | Content |
|---|---|
| `source_file_path` | Full path of the source file |
| `source_file_name` | File name only |
| `process_queue_id` | Unique execution identifier |
| `processed_at` | Timestamp of processing |
| `processed_by` | System identifier |

These columns are added before the data quality and write stages, ensuring that lineage is preserved even for rows that fail quality checks.

### 7.2 Execution-Level Lineage

Each pipeline execution persists a statistics record to `{catalog}.nova_framework.pipeline_stats` containing:

- Process queue ID (links to source system orchestrator)
- Execution start and end timestamps
- Duration in seconds
- Row counts: read, written, invalid
- Custom statistics (JSON): stage durations, strategy-specific metrics (e.g., SCD2 new/changed/deleted counts), throughput

### 7.3 Quality Lineage

Failed quality checks are persisted to `{catalog}.nova_framework.dq_errors` with the rule name, column, failed value, and quality score, linked back to the execution via process queue ID.

---

## 8. Access Control

### 8.1 Desired-State Reconciliation

NovaFlow implements access control as a declarative desired-state reconciliation against Unity Catalog privileges. The approach is modelled on infrastructure-as-code principles:

1. **Intended privileges** are loaded from metadata (what SHOULD exist based on policy)
2. **Actual privileges** are queried from Unity Catalog (what IS currently granted)
3. **Deltas** are computed using set arithmetic:
   - GRANTs needed = Intended − Actual
   - REVOKEs needed = Actual − Intended
   - No change = Intended ∩ Actual

This ensures that privilege drift is automatically corrected on each pipeline run, and that revoked access is enforced without manual intervention.

### 8.2 Privilege Model

Privileges are managed at the table level using Unity Catalog's native privilege model:

| Privilege | Grants |
|---|---|
| `SELECT` | Read access |
| `MODIFY` | Write access |
| `ALL_PRIVILEGES` | Full access |

Privileges are assigned to AD groups, not individual users, aligning with organisational identity management.

### 8.3 Dry Run Support

The access control stage supports a `dry_run` mode that computes and logs all privilege deltas without executing them. This allows review of proposed changes before application.

### 8.4 Non-Blocking Execution

Access control failures (metadata load errors, Unity Catalog query failures, individual GRANT/REVOKE failures) are logged and tracked but do not fail the pipeline. This prevents access control issues from blocking data delivery while still surfacing problems for remediation.

---

## 9. Transformation Governance

### 9.1 Supported Transformation Types

The framework supports three transformation languages through a unified execution interface:

| Type | How Code is Provided | Execution Method |
|---|---|---|
| **SQL** | Inline in the data contract | Spark SQL engine |
| **Python** | Module file in the transformations directory | Dynamic module loading via `importlib` |
| **Scala** | Precompiled JAR | Py4J bridge to JVM |

### 9.2 Python Transformation Constraints

Python transformations must conform to one of two contracts:

- **Function-based:** `def transform(spark, input_df=None, **kwargs) -> DataFrame`
- **Class-based:** Inherit from `AbstractTransformation`, implement `run(self, input_df=None, **kwargs) -> DataFrame`

Both forms must return a PySpark DataFrame. Configuration is passed exclusively through `**kwargs` sourced from the data contract's `transformationConfig` section — transformations cannot access arbitrary external configuration.

### 9.3 Transformation Registry

An optional YAML-based registry provides centralised metadata for transformations:

- Version tracking
- Author and description
- Dependency declarations
- Tag-based categorisation for discovery
- Configuration schema definitions

This enables governance teams to audit which transformations are available, who owns them, and what versions are in use.

---

## 10. Observability and Auditability

### 10.1 Structured Logging

The framework uses a centralised logging system that writes structured log records to both the console and a Delta table (`{catalog}.nova_framework.logs`). Each log record includes:

- Timestamp, log level, logger name
- Module, function, and line number
- Formatted message

Logging to Delta is non-blocking — failures in log persistence do not affect pipeline execution.

### 10.2 Pipeline Statistics

Every pipeline execution produces a statistics record persisted to `{catalog}.nova_framework.pipeline_stats`:

| Field | Description |
|---|---|
| `process_queue_id` | Unique execution identifier |
| `execution_start` / `execution_end` | Timestamps |
| `execution_time_seconds` | Duration |
| `rows_read` / `rows_written` / `rows_invalid` | Row counts |
| `custom_stats` | JSON blob with stage timings, strategy metrics, throughput |

### 10.3 Telemetry Events

The pipeline orchestrator emits telemetry events at execution boundaries (START, END, ERROR) to a dedicated telemetry table. These events enable operational monitoring and alerting independent of the pipeline's own logging.

### 10.4 Per-Stage Timing

Each pipeline stage automatically records its execution duration in the statistics, enabling performance analysis at the stage level without instrumentation in user code.

---

## 11. Error Handling Principles

The framework applies different error handling strategies depending on the criticality of the operation:

| Category | Examples | Behaviour |
|---|---|---|
| **Pipeline-critical** | Missing contract, missing SparkSession, invalid transformation configuration | Fail immediately with descriptive error |
| **Data-critical (configurable)** | Schema breaking changes, bad row threshold exceeded | Fail or warn based on contract configuration |
| **Observability** | Stats persistence failure, log table write failure, DQ error table write failure | Log warning, continue pipeline |
| **Access control** | Metadata load failure, UC query failure, individual GRANT/REVOKE failure | Log error, skip stage, continue pipeline |

This tiered approach ensures that data processing is resilient to infrastructure issues in supporting systems (logging, metrics, access control) while maintaining strict enforcement of data integrity constraints.

---

## 12. Data Classification

### 12.1 Column-Level Privacy Tagging

Data contracts support column-level privacy classification via the `privacy` field on each column definition. Supported classifications:

| Tag | Description |
|---|---|
| `special` | Special category data (ethnicity, health, religion) |
| `criminal` | Criminal conviction or offence data |
| `child` | Data relating to children |
| `pci` | Payment Card Industry data |
| `auth` | Authentication or credential data |
| `financial_pii` | Financial PII (bank account, salary) |
| `pii` | Personally Identifiable Information |
| `quasi_pii` | Indirect identifiers (postcode, date of birth) |
| `commercial` | Commercially sensitive data |
| `public` | Publicly available data |

### 12.2 Asset-Level Classification Derivation

The enterprise data classification for the entire asset is derived automatically from the highest-sensitivity privacy tag present across all columns:

| Highest Sensitivity Present | Enterprise Classification |
|---|---|
| `special`, `criminal`, `child`, `pci`, `auth` | Restricted / Secret |
| `financial_pii`, `pii`, `commercial` | Confidential |
| `quasi_pii` | Internal |
| `public` | Public |
| No tags | Internal (default) |

This classification is written to `customProperties.governance.dataClassification` and can be consumed by downstream governance tooling.

---

## 13. Operational Configuration

### 13.1 Feature Flags

The framework exposes global feature flags for operational control:

| Flag | Default | Effect |
|---|---|---|
| `enable_soft_delete` | `true` | Controls whether SCD2 marks missing records as deleted |
| `enable_schema_evolution` | `true` | Controls schema evolution behaviour |
| `enable_data_quality` | `true` | Controls whether quality rules are applied |

### 13.2 Volume-Based Partitioning

The contract's `volume` property maps to a recommended partition count for write operations, allowing data teams to right-size output without manual tuning:

| Volume | Partitions |
|---|---|
| XXS | 1 |
| XS | 10 |
| S | 25 |
| M (default) | 50 |
| L | 100 |
| XL | 200 |
| XXL | 500 |
| XXXL | 1,000 |

---

## 14. Extensibility

### 14.1 Custom Pipeline Types

The pipeline factory supports runtime registration of custom pipeline types. New pipeline classes extending `BasePipeline` can be registered without modifying the framework:

```python
PipelineFactory.register_pipeline("my_pipeline", MyPipelineClass)
```

The factory validates that registered classes extend the base pipeline class.

### 14.2 Custom Transformation Patterns

Python transformations support three levels of abstraction:

1. **Function-based** — Single function for simple transformations
2. **Class-based** — Full class with helper methods for complex logic
3. **Base class inheritance** — Reusable templates for common patterns (e.g., aggregation, snapshot, CDC)

All patterns are interchangeable from the framework's perspective — the data contract determines which pattern is used.

---

## 15. Risk Mitigations Through Design

| Risk | Mitigation |
|---|---|
| Uncontrolled schema changes breaking downstream consumers | Schema validation at read time with breaking change detection; contract-defined expected schema |
| Data quality issues propagating to gold layer | Two-phase quality enforcement (cleansing then validation) with weighted scoring; quality errors persisted for audit |
| Privilege drift from intended access policies | Desired-state reconciliation on every run; automatic REVOKE of unintended privileges |
| Loss of change history for auditable dimensions | SCD2 as default write strategy; soft delete preserves records with deletion flag |
| Bad source data corrupting production tables | Bad row isolation to quarantine; configurable rejection threshold to halt processing |
| Pipeline failures losing observability data | Non-blocking persistence for logs, stats, and DQ errors — observability failures never halt data processing |
| Ad-hoc configuration bypassing governance controls | Four-parameter entry point; all behaviour derived from versioned contract YAML |
| Cross-environment data contamination | Environment parameter drives catalog and storage path construction; physically separate catalogs per environment |
| Untraceable data provenance | Automatic row-level lineage columns; execution-level statistics; process queue ID linking across all observability tables |
