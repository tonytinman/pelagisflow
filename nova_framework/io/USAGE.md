# Nova Framework I/O Module - Usage Guide

## Installation

Place the `__init__.py` files in their respective directories:

```bash
# Copy files to your nova_framework/io/ directory
cp io__init__.py nova_framework/io/__init__.py
cp readers__init__.py nova_framework/io/readers/__init__.py
cp writers__init__.py nova_framework/io/writers/__init__.py
```

## Import Patterns

### Recommended: Use Factory

```python
from nova_framework.io.factory import IOFactory
from nova_framework.observability.context import PipelineContext
from nova_framework.observability.pipeline_stats import PipelineStats

# Create context
context = PipelineContext(
    process_queue_id=1,
    data_contract_name="customer_data",
    source_ref="2024-11-28",
    env="dev"
)
stats = PipelineStats()

# Create reader
reader = IOFactory.create_reader("file", context, stats)
df, report = reader.read()

# Create writer from contract
writer = IOFactory.create_writer_from_contract(context, stats)
write_stats = writer.write(df)
```

### Alternative: Direct Imports

```python
# Import specific classes
from nova_framework.io.readers import FileReader, TableReader
from nova_framework.io.writers import SCD2Writer, OverwriteWriter

# Use directly
reader = FileReader(context, stats)
df, report = reader.read()

writer = SCD2Writer(context, stats)
write_stats = writer.write(df, soft_delete=True)
```

### Alternative: Import from io module

```python
# Import from main io module
from nova_framework.io import IOFactory, FileReader, SCD2Writer

reader = FileReader(context, stats)
writer = SCD2Writer(context, stats)
```

## Reader Usage

### FileReader

```python
from nova_framework.io.factory import IOFactory

reader = IOFactory.create_reader("file", context, stats)

# Basic read
df, report = reader.read()

# With options
df, report = reader.read(
    file_path="/path/to/data.parquet",
    file_format="parquet",
    fail_on_schema_change=True,
    fail_on_bad_rows=False,
    invalid_row_threshold=0.1,
    verbose=True
)

# Check report
print(f"Rows read: {report['total_rows']}")
print(f"Valid rows: {report['valid_rows']}")
print(f"Invalid rows: {report['invalid_rows']}")
print(f"Rejection rate: {report['rejection_rate_pct']:.2f}%")
```

### TableReader

```python
from nova_framework.io.factory import IOFactory

reader = IOFactory.create_reader("table", context, stats)

# Read entire table
df, report = reader.read()

# With filter
df, report = reader.read(
    table_name="catalog.schema.table",
    filter_condition="date >= '2024-01-01' AND status = 'active'",
    columns=["id", "name", "date"]
)
```

## Writer Usage

### Contract-Driven (Recommended)

```yaml
# In your data contract YAML
customProperties:
  writeStrategy: scd2  # or overwrite, append, scd4, file_export
  softDelete: true
```

```python
# In your pipeline code
from nova_framework.io.factory import IOFactory

# Factory reads writeStrategy from contract
writer = IOFactory.create_writer_from_contract(context, stats)
write_stats = writer.write(df)

print(f"Strategy: {write_stats['strategy']}")
print(f"Rows written: {write_stats['rows_written']}")
```

### Explicit Writer Selection

#### OverwriteWriter

```python
from nova_framework.io.factory import IOFactory

writer = IOFactory.create_writer("overwrite", context, stats)

write_stats = writer.write(
    df,
    target_table=None,  # Uses contract table
    partition_cols=["date"],  # Optional
    optimize=True  # Run OPTIMIZE after write
)

print(f"Rows written: {write_stats['rows_written']}")
print(f"Optimized: {write_stats['optimized']}")
```

#### AppendWriter

```python
from nova_framework.io.factory import IOFactory

writer = IOFactory.create_writer("append", context, stats)

write_stats = writer.write(
    df,
    target_table=None,  # Uses contract table
    partition_cols=["date"],
    deduplicate=True,  # Remove duplicates before appending
    dedup_cols=["order_id", "customer_id"]  # Dedup on these columns
)

print(f"Rows written: {write_stats['rows_written']}")
if write_stats['deduplicated']:
    print(f"Duplicates removed: {write_stats['dedup_removed']}")
```

#### SCD2Writer

```python
from nova_framework.io.factory import IOFactory

writer = IOFactory.create_writer("scd2", context, stats)

write_stats = writer.write(
    df,
    target_table=None,  # Uses contract table
    natural_key_col="natural_key_hash",
    change_key_col="change_key_hash",
    partition_col="partition_key",
    process_date="2024-11-28",  # Optional, defaults to today
    soft_delete=True  # Detect and mark deleted records
)

print(f"New records: {write_stats['new_records']}")
print(f"Changed records: {write_stats['changed_records']}")
print(f"Soft deleted: {write_stats['soft_deleted']}")
print(f"Total inserted: {write_stats['records_inserted']}")
```

#### SCD4Writer

```python
from nova_framework.io.factory import IOFactory

writer = IOFactory.create_writer("scd4", context, stats)

# Creates two tables:
# - {table}_current (fast queries, latest version only)
# - {table}_history (full SCD2 history)
write_stats = writer.write(
    df,
    current_table=None,  # Auto-generated: {base}_current
    historical_table=None,  # Auto-generated: {base}_history
    natural_key_col="natural_key_hash",
    change_key_col="change_key_hash",
    partition_col="partition_key"
)

print(f"Current rows: {write_stats['current_rows']}")
print(f"Historical stats: {write_stats['historical_stats']}")
```

#### FileExportWriter

```python
from nova_framework.io.factory import IOFactory

writer = IOFactory.create_writer("file_export", context, stats)

write_stats = writer.write(
    df,
    output_path="/mnt/exports/customer_data_20241128.csv",
    file_format="csv",
    mode="overwrite",  # or "append"
    partition_cols=["date"],
    # CSV-specific options
    header="true",
    delimiter="|"
)

print(f"Exported {write_stats['rows_exported']} rows")
print(f"Output: {write_stats['output_path']}")
```

## Complete Pipeline Example

### Ingestion Pipeline

```python
from nova_framework.io.factory import IOFactory
from nova_framework.observability.context import PipelineContext
from nova_framework.observability.pipeline_stats import PipelineStats

def run_ingestion_pipeline(
    process_queue_id: int,
    data_contract_name: str,
    source_ref: str,
    env: str
):
    """
    Complete ingestion pipeline using refactored I/O module.
    """
    # Initialize context
    context = PipelineContext(
        process_queue_id=process_queue_id,
        data_contract_name=data_contract_name,
        source_ref=source_ref,
        env=env
    )
    
    stats = PipelineStats()
    
    try:
        # 1. Read data
        reader = IOFactory.create_reader("file", context, stats)
        df, read_report = reader.read(verbose=True)
        
        print(f"Read {read_report['total_rows']} rows")
        
        # 2. Apply transformations
        # (lineage, hashing, dedup, quality - your existing code)
        df_transformed = apply_transformations(df, context, stats)
        
        # 3. Write data using contract strategy
        writer = IOFactory.create_writer_from_contract(context, stats)
        write_stats = writer.write(df_transformed)
        
        print(f"Write complete: {write_stats}")
        
        # 4. Finalize
        stats.summary()
        stats.finalize(process_queue_id=process_queue_id)
        
        return "SUCCESS"
        
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise
```

### Transformation Pipeline

```python
from pyspark.sql import SparkSession

def run_transformation_pipeline(
    process_queue_id: int,
    data_contract_name: str,
    source_ref: str,
    env: str
):
    """
    Transformation pipeline using refactored I/O module.
    """
    context = PipelineContext(
        process_queue_id=process_queue_id,
        data_contract_name=data_contract_name,
        source_ref=source_ref,
        env=env
    )
    
    stats = PipelineStats()
    spark = SparkSession.getActiveSession()
    
    try:
        # 1. Execute transformation SQL
        transformation_sql = context.contract.get(
            "customProperties.transformationSql"
        )
        df_transformed = spark.sql(transformation_sql)
        
        # 2. Apply post-transformation steps
        df_final = apply_transformations(df_transformed, context, stats)
        
        # 3. Write using contract strategy
        writer = IOFactory.create_writer_from_contract(context, stats)
        write_stats = writer.write(df_final)
        
        print(f"Transformation complete: {write_stats}")
        
        stats.summary()
        stats.finalize(process_queue_id=process_queue_id)
        
        return "SUCCESS"
        
    except Exception as e:
        print(f"Transformation failed: {str(e)}")
        raise
```

## Contract Examples

### Ingestion Contract with SCD2

```yaml
apiVersion: v3.0.2
kind: DataContract
name: customer_master
version: 1.0.0
domain: sales
dataProduct: customer_data

schema:
  name: bronze_sales
  table: customers
  format: parquet
  properties:
    - name: customer_id
      type: integer
      isPrimaryKey: true
    - name: customer_name
      type: string
      isChangeTracking: true
    - name: email
      type: string
      isChangeTracking: true

customProperties:
  volume: M
  writeStrategy: scd2
  softDelete: true
  
quality:
  - rule: not_null
    severity: critical
    columns: [customer_id, customer_name]
```

### Transformation Contract with Overwrite

```yaml
apiVersion: v3.0.2
kind: DataContract
name: customer_summary
version: 1.0.0
domain: sales
dataProduct: customer_analytics

schema:
  name: silver_sales
  table: customer_summary
  format: delta
  properties:
    - name: customer_id
      type: integer
      isPrimaryKey: true
    - name: total_orders
      type: integer
    - name: total_revenue
      type: double

customProperties:
  writeStrategy: overwrite
  optimizeAfterWrite: true
  
  sourceTables:
    - bronze_sales.customers
    - bronze_sales.orders
  
  transformationSql: |
    SELECT 
      c.customer_id,
      COUNT(o.order_id) as total_orders,
      SUM(o.order_total) as total_revenue
    FROM bronze_sales.customers c
    LEFT JOIN bronze_sales.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
```

### File Export Contract

```yaml
apiVersion: v3.0.2
kind: DataContract
name: customer_export
version: 1.0.0

schema:
  name: exports
  table: customers
  format: csv

customProperties:
  writeStrategy: file_export
  exportPath: /mnt/exports/customers
  exportFormat: csv
  
  csvOptions:
    header: "true"
    delimiter: "|"
```

## Testing

### Unit Test Example

```python
import pytest
from unittest.mock import Mock, patch
from nova_framework.io.writers import SCD2Writer

def test_scd2_writer_first_load(spark_session):
    """Test SCD2 first load creates table correctly."""
    # Setup
    context = Mock()
    context.catalog = "test_catalog"
    context.contract.schema_name = "test_schema"
    context.contract.table_name = "test_table"
    
    stats = Mock()
    
    df = spark_session.createDataFrame([
        (1, "Alice", 100),
        (2, "Bob", 200)
    ], ["id", "name", "natural_key_hash"])
    
    # Execute
    writer = SCD2Writer(context, stats)
    result = writer.write(df, natural_key_col="natural_key_hash")
    
    # Assert
    assert result["first_load"] == True
    assert result["records_inserted"] == 2
```

### Integration Test Example

```python
def test_full_pipeline_with_io_module(spark_session):
    """Test complete pipeline using refactored I/O module."""
    # Setup
    context = create_test_context()
    stats = PipelineStats()
    
    # Create test data
    df_test = create_test_dataframe(spark_session)
    
    # Write test data to source
    df_test.write.format("parquet").save("/tmp/test_source")
    context.data_file_path = "/tmp/test_source"
    
    # Execute pipeline
    reader = IOFactory.create_reader("file", context, stats)
    df, report = reader.read()
    
    writer = IOFactory.create_writer("scd2", context, stats)
    write_stats = writer.write(df)
    
    # Verify
    assert report["total_rows"] > 0
    assert write_stats["records_inserted"] > 0
```

## Migration from Old Code

### Old Pattern

```python
# OLD: Direct use of PipelineFunctions
from nova_framework.pipeline.pipeline import PipelineFunctions

merge_stats = PipelineFunctions.merge_scd2(
    df_with_quality,
    fq_target_table,
    pipeline_stats=self.pipeline_stats
)
```

### New Pattern

```python
# NEW: Use IOFactory
from nova_framework.io.factory import IOFactory

writer = IOFactory.create_writer_from_contract(context, stats)
write_stats = writer.write(df_with_quality)
```

### Migration Checklist

- [ ] Replace `PipelineFunctions.merge_scd2()` calls with `IOFactory.create_writer()`
- [ ] Add `writeStrategy` to contracts that need non-SCD2 writes
- [ ] Update imports from `nova_framework.pipeline` to `nova_framework.io`
- [ ] Test each pipeline with new I/O module
- [ ] Remove old `PipelineFunctions.merge_scd2()` method
- [ ] Update documentation

## Troubleshooting

### Import Error: No module named 'nova_framework.io.factory'

**Solution:** Ensure `__init__.py` files are in place:
```bash
ls nova_framework/io/__init__.py
ls nova_framework/io/readers/__init__.py
ls nova_framework/io/writers/__init__.py
```

### ValueError: Unknown writer type

**Solution:** Check contract configuration:
```yaml
customProperties:
  writeStrategy: scd2  # Must be: overwrite, append, scd2, scd4, or file_export
```

### Writer not using contract configuration

**Solution:** Use `create_writer_from_contract()`:
```python
# Correct
writer = IOFactory.create_writer_from_contract(context, stats)

# Not this
writer = IOFactory.create_writer("scd2", context, stats)  # Ignores contract
```

### SCD2 not detecting changes

**Solution:** Ensure hash columns are present:
```python
# DataFrame must have these columns before writing:
- natural_key_hash
- change_key_hash
- partition_key
```

## Best Practices

1. **Use IOFactory** - Always use factory for consistency
2. **Contract-driven** - Specify `writeStrategy` in contracts
3. **Default to SCD2** - Use SCD2 for dimensions unless you have a reason not to
4. **Test each writer** - Unit test each write strategy independently
5. **Validate contracts** - Ensure contracts have required fields for each writer
6. **Log write stats** - Always check write stats for debugging
7. **Use soft_delete** - Enable soft delete for dimensions to track deletions

## Summary

Your refactored I/O module provides:

✅ **Strategy pattern** for readers and writers  
✅ **Contract-driven** configuration  
✅ **5 write strategies** available  
✅ **Factory-based** creation  
✅ **Easy to extend** with new strategies  
✅ **Testable** with mocks  
✅ **Clean separation** of concerns  

All write logic is now properly in the I/O module where it belongs!