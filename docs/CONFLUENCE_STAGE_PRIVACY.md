# NovaFlow Pipeline Stage: Privacy

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**: [Pipeline Stages Overview](CONFLUENCE_PIPELINE_STAGES_OVERVIEW.md) | [Data Privacy Control](CONFLUENCE_DATA_PRIVACY.md)

---

## Overview

The **PrivacyStage** enforces column-level masking policies in Unity Catalog. It reads privacy classifications from the data contract and role-based sensitivity access from domain configuration, then applies or removes masking policies as needed.

This stage operates on **UC metadata**, not on the DataFrame itself. The DataFrame passes through unchanged.

| Property | Value |
|----------|-------|
| **Class** | `PrivacyStage` |
| **File** | `nova_framework/pipeline/stages/privacy_stage.py` |
| **Stage Name** | `Privacy` |
| **Pipeline Position** | After Write (add-on) |
| **Skip Condition** | Not included in default pipelines — must be explicitly added |
| **Modifies DataFrame** | No — operates on Unity Catalog metadata |
| **Design Pattern** | Facade/Adapter over `PrivacyEngine` |

---

## How It Works

```
Data Contract                     Domain Roles
(privacy per column)              (sensitive_access per role)
        │                                  │
        ▼                                  ▼
┌──────────────────────────────────────────────┐
│              PrivacyEngine                    │
│                                              │
│  1. Extract privacy metadata from contract   │
│  2. Determine exempt AD groups from roles    │
│  3. Query current masking state from UC      │
│  4. Calculate deltas (CREATE/DROP masking)   │
│  5. Execute changes via ALTER TABLE SET MASK │
│  6. Log results                              │
└──────────────────────────────────────────────┘
```

### Differential Enforcement

The stage compares **intended** masking state (from contract + roles) against **actual** masking state (from Unity Catalog) and only changes what is needed. This makes it:

- **Idempotent** — running it twice produces the same result
- **Efficient** — only altered columns are modified
- **Safe** — no unnecessary UC operations

---

## Parameters

```python
PrivacyStage(context, stats, environment, registry_path=None, dry_run=False)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `context` | `ExecutionContext` | *required* | Pipeline execution context |
| `stats` | `PipelineStats` | *required* | Statistics tracker |
| `environment` | `str` | *required* | Environment name (`dev`, `test`, `prod`) |
| `registry_path` | `str` or `None` | Auto-detected: `/Volumes/{catalog}/contract_registry` | Path to contract registry |
| `dry_run` | `bool` | `False` | If `True`, previews changes without executing |

---

## Statistics Logged

| Metric | Description |
|--------|-------------|
| `policies_created` | Number of new masking policies applied |
| `policies_dropped` | Number of masking policies removed |
| `policies_failed` | Number of failed policy operations |
| `execution_time_seconds` | Time taken for enforcement |

---

## Error Handling

The stage's failure behaviour is configurable:

| Contract Setting | Behaviour on Failure |
|------------------|---------------------|
| `privacy.allowPartialFailure: false` (default) | Pipeline fails with `RuntimeError` |
| `privacy.allowPartialFailure: true` | Error logged, pipeline continues |

```yaml
customProperties:
  privacy:
    allowPartialFailure: true  # Don't fail pipeline on masking errors
```

---

## Adding to a Pipeline

The PrivacyStage is not included in the default ingestion or transformation pipelines. Add it explicitly:

```python
class IngestionWithPrivacyPipeline(BasePipeline):
    def build_stages(self):
        stages = [
            ReadStage(self.context, self.stats, reader_type="file"),
            LineageStage(self.context, self.stats),
            HashingStage(self.context, self.stats),
            DeduplicationStage(self.context, self.stats),
            QualityStage(self.context, self.stats),
            WriteStage(self.context, self.stats),
        ]

        # Add privacy enforcement
        stages.append(PrivacyStage(
            self.context, self.stats,
            environment=self.context.env
        ))

        return stages
```

---

## Example Log Output

```
[Privacy] Starting...
Starting privacy enforcement
Enforcing privacy for prod_catalog.bronze_finance.employee_salary
Privacy enforcement completed: Created=3, Dropped=0, Failed=0, Success=100.0%
Columns with privacy: ['EMAIL', 'SALARY', 'DISABILITY_FLAG']
Masking intents: {'EMAIL': 'mask_email', 'SALARY': 'hash', 'DISABILITY_FLAG': 'redact'}
Current masked columns: []
No change needed: 0
[Privacy] Completed in 2.15s
```

For full details on privacy classifications, role-based access, and masking strategies, see [Data Privacy Control](CONFLUENCE_DATA_PRIVACY.md).

---

_This is a living document. Last updated: February 2026_
