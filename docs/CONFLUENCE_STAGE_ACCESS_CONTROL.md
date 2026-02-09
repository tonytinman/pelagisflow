# NovaFlow Pipeline Stage: Access Control

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**: [Pipeline Stages Overview](CONFLUENCE_PIPELINE_STAGES_OVERVIEW.md) | [Data Privacy Control](CONFLUENCE_DATA_PRIVACY.md)

---

## Overview

The **AccessControlStage** enforces table-level access control by comparing intended privileges (from domain role metadata) against actual privileges (from Unity Catalog) and executing GRANTs and REVOKEs to reconcile any differences.

This stage operates on **UC metadata**, not on the DataFrame itself. The DataFrame passes through unchanged.

| Property | Value |
|----------|-------|
| **Class** | `AccessControlStage` |
| **File** | `nova_framework/pipeline/stages/access_control_stage.py` |
| **Stage Name** | `AccessControl` |
| **Pipeline Position** | After Write (add-on) |
| **Skip Condition** | `contract.accessControl.enabled == false` |
| **Modifies DataFrame** | No — operates on Unity Catalog metadata |
| **Design Pattern** | Facade/Adapter over 4 access control components |

---

## How It Works

The stage orchestrates a 4-step workflow using dedicated components:

```
Step 1                        Step 2
Domain Role Metadata          Unity Catalog
(intended privileges)         (actual privileges)
        │                            │
        ▼                            ▼
┌───────────────────┐  ┌────────────────────────┐
│ AccessMetadata    │  │ UCPrivilege            │
│ Loader            │  │ Inspector              │
│                   │  │                        │
│ get_intended_     │  │ get_actual_            │
│ privileges()      │  │ privileges()           │
└────────┬──────────┘  └──────────┬─────────────┘
         │                        │
         └──────────┬─────────────┘
                    ▼
          ┌─────────────────────┐
          │ PrivilegeDelta      │  Step 3
          │ Generator           │
          │                     │
          │ generate_deltas()   │  → GRANTs needed
          │ group_by_action()   │  → REVOKEs needed
          └─────────┬───────────┘  → No change count
                    │
                    ▼
          ┌─────────────────────┐
          │ GrantRevoker        │  Step 4
          │                     │
          │ apply_deltas()      │  → Execute GRANT/REVOKE SQL
          └─────────────────────┘
```

### Differential Enforcement

Like the PrivacyStage, access control uses differential enforcement:
- Privileges that exist and should exist → **no change**
- Privileges that should exist but don't → **GRANT**
- Privileges that exist but shouldn't → **REVOKE**

This minimises UC operations and ensures idempotency.

---

## Parameters

```python
AccessControlStage(context, stats, environment, registry_path=None, dry_run=False)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `context` | `ExecutionContext` | *required* | Pipeline execution context |
| `stats` | `PipelineStats` | *required* | Statistics tracker |
| `environment` | `str` | *required* | Environment name (`dev`, `test`, `prod`) |
| `registry_path` | `str` or `None` | Auto-detected: `/Volumes/{catalog}/contract_registry` | Path to contract registry |
| `dry_run` | `bool` | `False` | If `True`, previews changes without executing GRANT/REVOKE |

---

## Skip Condition

The stage skips when access control is explicitly disabled in the contract:

```yaml
accessControl:
  enabled: false  # Skip access control for this table
```

Default is `enabled: true` — if not specified, the stage runs.

---

## Statistics Logged

| Metric | Description |
|--------|-------------|
| `access_intended_count` | Number of intended privilege entries |
| `access_actual_count` | Number of actual privilege entries in UC |
| `access_no_change_count` | Privileges already correct |
| `access_grants_attempted` | GRANTs attempted |
| `access_grants_succeeded` | GRANTs that succeeded |
| `access_grants_failed` | GRANTs that failed |
| `access_revokes_attempted` | REVOKEs attempted |
| `access_revokes_succeeded` | REVOKEs that succeeded |
| `access_revokes_failed` | REVOKEs that failed |
| `access_execution_seconds` | Time taken for enforcement |

---

## Error Handling

| Scenario | Behaviour |
|----------|-----------|
| Metadata loading fails | Warning logged, DataFrame returned unchanged (pipeline continues) |
| UC privilege query fails | Error logged, DataFrame returned unchanged (pipeline continues) |
| No access rules defined | Info logged, DataFrame returned unchanged |
| GRANT/REVOKE fails | Counted in `*_failed` stats, errors logged as warnings |
| No changes needed | Info logged: "No changes needed - already correct" |

---

## Dry Run Mode

When `dry_run=True`, the stage calculates all deltas and logs what would change, but does not execute any GRANT or REVOKE statements:

```
[AccessControl] Starting...
Processing access control for catalog.bronze_sales.customers
Intended privileges: 5
Actual privileges: 3
Changes: 2 GRANTs, 0 REVOKEs, 3 correct
DRY RUN MODE - Changes not executed
[AccessControl] Completed in 1.02s
```

---

## Example Log Output

```
[AccessControl] Starting...
Processing access control for prod_catalog.bronze_sales.customers
Intended privileges: 5
Actual privileges: 3
Changes: 2 GRANTs, 0 REVOKEs, 3 correct
Result: GRANTs 2/2, REVOKEs 0/0, Success: 100.0%
[AccessControl] Completed in 1.45s
```

### When Already Correct

```
[AccessControl] Starting...
Processing access control for prod_catalog.bronze_sales.customers
Intended privileges: 5
Actual privileges: 5
Changes: 0 GRANTs, 0 REVOKEs, 5 correct
No changes needed - already correct
[AccessControl] Completed in 0.89s
```

---

## Adding to a Pipeline

```python
class IngestionWithAccessPipeline(BasePipeline):
    def build_stages(self):
        stages = [
            ReadStage(self.context, self.stats, reader_type="file"),
            LineageStage(self.context, self.stats),
            HashingStage(self.context, self.stats),
            DeduplicationStage(self.context, self.stats),
            QualityStage(self.context, self.stats),
            WriteStage(self.context, self.stats),
        ]

        # Add access control after write
        stages.append(AccessControlStage(
            self.context, self.stats,
            environment=self.context.env
        ))

        return stages
```

---

_This is a living document. Last updated: February 2026_
