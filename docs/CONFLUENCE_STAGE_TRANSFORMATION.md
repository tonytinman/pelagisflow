# NovaFlow Pipeline Stage: Transformation

---

**Page Owner**: Data Engineering Team
**Last Updated**: February 2026
**Version**: 1.0
**Related Pages**: [Pipeline Stages Overview](CONFLUENCE_PIPELINE_STAGES_OVERVIEW.md) | [Transformation Strategy](TRANSFORMATION_STRATEGY.md)

---

## Overview

The **TransformationStage** executes data transformations using a pluggable strategy pattern. It supports SQL, Python, and Scala transformations, all configured through the data contract.

| Property | Value |
|----------|-------|
| **Class** | `TransformationStage` |
| **File** | `nova_framework/pipeline/stages/transformation_stage.py` |
| **Stage Name** | `Transformation` |
| **Pipeline Position** | First stage in transformation pipelines |
| **Skip Condition** | Never skipped |
| **Modifies DataFrame** | Yes — produces an entirely new DataFrame |

---

## Supported Transformation Types

### 1. Inline SQL (Legacy Mode)

The simplest approach. SQL is embedded directly in the contract.

```yaml
customProperties:
  transformationSql: |
    SELECT
      customer_id,
      COUNT(order_id) AS total_orders,
      SUM(order_total) AS total_revenue
    FROM bronze_sales.orders
    GROUP BY customer_id
```

### 2. Registry-Based SQL

References a named transformation registered in the framework.

```yaml
customProperties:
  transformationName: customer_aggregation_v1
```

### 3. Python

Points to a Python module and function.

```yaml
customProperties:
  transformationType: python
  transformationModule: aggregations/customer_rollup
  transformationFunction: transform    # optional, defaults to 'transform'
```

### 4. Python Class-Based

Points to a Python class that extends a base transformation class.

```yaml
customProperties:
  transformationType: python
  transformationModule: customer_360_class
  transformationClass: Customer360Transformation
```

### 5. Scala

References a precompiled Scala class.

```yaml
customProperties:
  transformationType: scala
  transformationClass: com.pelagisflow.transformations.CustomerAggregation
  transformationJar: /path/to/transformations.jar   # optional if on classpath
```

---

## How It Works

```
Contract customProperties
        │
        ▼
┌─────────────────────────┐
│ _load_transformation_   │ ← Determines strategy type
│ strategy()              │    from contract config
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│ strategy.transform(     │ ← Executes SQL/Python/Scala
│   input_df=df           │
│ )                       │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│ Count rows + columns    │
│ Log statistics          │
└─────────────────────────┘
```

### Strategy Loading Priority

1. If `transformationSql` exists (and `transformationName` does not) → **Legacy SQL mode**
2. Otherwise → `TransformationLoader.load_from_contract()` resolves:
   - `transformationName` → Registry lookup
   - `transformationType: python` + `transformationModule` → Python module
   - `transformationType: scala` + `transformationClass` → Scala class

---

## Statistics Logged

| Metric | Description |
|--------|-------------|
| `rows_transformed` | Row count of the output DataFrame |
| `transformation_type` | Type of transformation executed (sql, python, scala) |
| `stage_Transformation_duration_seconds` | Execution time |

---

## Validation

The stage provides a `validate()` method to pre-check that the transformation configuration is valid without executing it:

```python
stage = TransformationStage(context, stats)
is_valid = stage.validate()
# Returns True if config is loadable, False otherwise
```

---

## Error Handling

| Scenario | Behaviour |
|----------|-----------|
| Invalid/missing transformation config | `ValueError` with helpful message listing valid options |
| SQL syntax error | Exception propagates with transformation metadata in error log |
| Python module not found | `ValueError` from loader |
| Transformation execution fails | Exception logged with metadata, then re-raised |

### Error Message Example

```
ValueError: Failed to load transformation strategy: ...
Contract: data.analytics.customer_summary

Expected one of:
  - transformationSql: <SQL query>
  - transformationName: <registry name>
  - transformationType + transformationModule (Python)
  - transformationType + transformationClass (Scala)
```

---

## Example Log Output

```
[Transformation] Starting...
Loading SQL transformation (legacy mode)
Executing sql transformation
Transformation completed: 50000 rows, 5 columns
Output columns: ['customer_id', 'total_orders', 'total_revenue', 'avg_order_value', 'last_order_date']
[Transformation] Completed in 8.45s
```

---

## Contract Examples

### SQL Transformation

```yaml
apiVersion: v3.0.2
kind: DataContract
name: data.analytics.customer_summary

schema:
  name: silver_analytics
  table: customer_summary
  properties:
    - name: customer_id
      type: bigint
      isPrimaryKey: true
    - name: total_orders
      type: integer
    - name: total_revenue
      type: double

customProperties:
  pipelineType: transformation
  writeStrategy: overwrite
  transformationSql: |
    SELECT
      c.customer_id,
      COUNT(o.order_id) AS total_orders,
      SUM(o.order_total) AS total_revenue
    FROM bronze_sales.customers c
    LEFT JOIN bronze_sales.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
```

### Python Transformation

```yaml
customProperties:
  pipelineType: transformation
  writeStrategy: type_2_change_log
  transformationType: python
  transformationModule: customer_360_class
  transformationClass: Customer360Transformation
```

---

_This is a living document. Last updated: February 2026_
