# PelagisFlow Transformations

This directory contains custom transformation code for the PelagisFlow data framework.

## Directory Structure

```
transformations/
├── README.md                    # This file
├── __init__.py
├── python/                      # Python transformations
│   ├── __init__.py
│   ├── customer_aggregation.py # Example: Customer analytics with RFM
│   └── product_analytics/      # Example: Multi-file package
│       ├── __init__.py
│       ├── helpers.py
│       └── metrics.py
├── scala/                       # Scala transformations (precompiled JARs)
│   ├── README.md               # Scala development guide
│   └── __init__.py
└── registry/                    # Transformation registry
    ├── python_transformations.yaml
    └── scala_transformations.yaml
```

## Quick Start

### 1. Create a Simple Python Transformation

Create `python/my_transformation.py`:

```python
from pyspark.sql import DataFrame, SparkSession
from typing import Optional

def transform(
    spark: SparkSession,
    input_df: Optional[DataFrame] = None,
    **kwargs
) -> DataFrame:
    """
    My custom transformation.

    Args:
        spark: SparkSession
        input_df: Optional input DataFrame
        **kwargs: Configuration from data contract

    Returns:
        Transformed DataFrame
    """
    # Your transformation logic
    df = spark.table(kwargs.get('source_table', 'default.table'))
    return df.filter("status = 'active'")
```

### 2. Register the Transformation

Add to `registry/python_transformations.yaml`:

```yaml
- name: my_transformation_v1
  type: python
  version: "1.0.0"
  description: My custom transformation
  module_path: my_transformation
  function_name: transform
  author: Your Name
  tags:
    - custom
```

### 3. Use in Data Contract

```yaml
customProperties:
  # Option 1: Direct reference
  transformationType: python
  transformationModule: my_transformation

  # Option 2: Registry reference
  transformationName: my_transformation_v1

  # Pass configuration
  transformationConfig:
    source_table: bronze.my_table
```

## Transformation Types

### SQL Transformations

Use for simple, declarative transformations:

```yaml
customProperties:
  transformationSql: |
    SELECT customer_id, COUNT(*) as orders
    FROM bronze.orders
    GROUP BY customer_id
```

### Python Transformations

Use for complex logic with helpers:

- Single file: `python/simple_transform.py`
- Package: `python/complex_transform/__init__.py`
- Can include helper modules and classes

### Scala Transformations

Use for high-performance operations:

- Precompile to JAR
- Place in `scala/` directory
- Reference by class name

See `scala/README.md` for details.

## Examples

### Customer Analytics (Python)

Full example with helpers and RFM scoring:

```bash
python/customer_aggregation.py
```

Features:
- Multiple helper functions
- RFM score calculation
- Customer segmentation
- Configurable thresholds

### Product Analytics (Python Package)

Multi-file transformation with classes:

```bash
python/product_analytics/
├── __init__.py      # Main transform()
├── helpers.py       # Helper functions
└── metrics.py       # Metrics calculator class
```

Features:
- Trend detection
- Seasonality analysis
- Product enrichment
- Modular design

## Best Practices

1. **Function Signature**: Always use `def transform(spark, input_df=None, **kwargs)`
2. **Return Type**: Must return a Spark DataFrame
3. **Configuration**: Use `kwargs` for configuration parameters
4. **Documentation**: Add docstrings with Args and Returns
5. **Error Handling**: Validate inputs and provide clear error messages
6. **Testing**: Write unit tests for your transformations
7. **Versioning**: Use semantic versioning in registry

## Testing

Run transformation tests:

```bash
pytest tests/test_transformation_strategies.py -v
```

Test your custom transformation:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_my_transformation(spark):
    from nova_framework.transformations.python.my_transformation import transform

    # Create test data
    df = spark.createDataFrame([(1, "test")], ["id", "value"])

    # Execute
    result = transform(spark, df)

    # Assert
    assert result.count() > 0
```

## Sample Contracts

Example data contracts using transformations:

```bash
samples/transformation_examples/
├── sql_transformation_contract.yaml
├── python_transformation_contract.yaml
├── python_registry_based_contract.yaml
└── scala_transformation_contract.yaml
```

## Documentation

Full documentation: `docs/TRANSFORMATION_STRATEGY.md`

Topics covered:
- Architecture overview
- SQL transformations
- Python transformations
- Scala transformations
- Registry management
- Testing strategies
- Best practices
- Troubleshooting

## Getting Help

- Check documentation: `docs/TRANSFORMATION_STRATEGY.md`
- Review examples: `samples/transformation_examples/`
- Run tests: `pytest tests/test_transformation_strategies.py`
- Read code: Explore existing transformations in `python/`
