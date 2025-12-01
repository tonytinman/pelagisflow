# Transformation Strategy System

PelagisFlow provides a flexible transformation system that supports multiple transformation types with dynamic loading at runtime based on data contract specifications.

## Overview

The transformation strategy system enables you to:
- Write transformations in **SQL**, **Python**, or **Scala**
- Organize custom code with helper functions and classes
- Register transformations in a centralized registry
- Load transformations dynamically at runtime
- Version and manage transformations as code

## Table of Contents

1. [Transformation Types](#transformation-types)
2. [Architecture](#architecture)
3. [SQL Transformations](#sql-transformations)
4. [Python Transformations](#python-transformations)
5. [Scala Transformations](#scala-transformations)
6. [Transformation Registry](#transformation-registry)
7. [Data Contract Configuration](#data-contract-configuration)
8. [Best Practices](#best-practices)
9. [Testing](#testing)

## Transformation Types

### SQL Transformations

Simple SQL statements executed via Spark SQL engine.

**Use when:**
- Transformation logic is straightforward SQL
- Working with tables already in the catalog
- Need quick, declarative transformations

**Example:**
```sql
SELECT
  customer_id,
  COUNT(*) as order_count,
  SUM(order_total) as total_revenue
FROM bronze_sales.orders
GROUP BY customer_id
```

### Python Transformations

Custom Python code with full Spark DataFrame API access.

**Use when:**
- Need complex business logic
- Require helper functions or classes
- Want to encapsulate reusable logic
- Need advanced DataFrame operations

**Example:**
```python
def transform(spark, input_df=None, **kwargs):
    # Custom transformation logic
    customers = spark.table("bronze.customers")
    orders = spark.table("bronze.orders")

    # Complex aggregation with helpers
    result = calculate_customer_metrics(customers, orders)
    result = classify_customer_segments(result)

    return result
```

### Scala Transformations

Precompiled Scala code for high-performance transformations.

**Use when:**
- Need maximum performance
- Working with large-scale data
- Require complex type-safe operations
- Have existing Scala codebase

**Example:**
```scala
class MyTransformation {
  def transform(
    spark: SparkSession,
    inputDF: Option[DataFrame],
    config: Map[String, Any]
  ): DataFrame = {
    // High-performance transformation
    inputDF.getOrElse(spark.emptyDataFrame)
      .filter("status = 'active'")
      .groupBy("category").count()
  }
}
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│          Data Contract (YAML)                   │
│  - transformationType: python/sql/scala         │
│  - transformationModule/SQL/Class               │
│  - transformationConfig                         │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│         TransformationStage                     │
│  - Loads strategy based on contract             │
│  - Executes transformation                      │
│  - Tracks metrics                               │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│         TransformationLoader                    │
│  - Factory for creating strategies              │
│  - Loads from registry or direct config         │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────┐
│      TransformationStrategy                     │
│  (Unified class handling all types)             │
│                                                  │
│  ┌────────────────────────────────────────┐    │
│  │ SQL Execution                          │    │
│  │ - Execute SQL via Spark SQL engine     │    │
│  └────────────────────────────────────────┘    │
│                                                  │
│  ┌────────────────────────────────────────┐    │
│  │ Python Execution                       │    │
│  │ - Dynamic module loading               │    │
│  │ - Import helpers and classes           │    │
│  └────────────────────────────────────────┘    │
│                                                  │
│  ┌────────────────────────────────────────┐    │
│  │ Scala Execution                        │    │
│  │ - JAR loading                          │    │
│  │ - Py4J bridge to JVM                   │    │
│  └────────────────────────────────────────┘    │
└─────────────────────────────────────────────────┘
```

### Key Components

1. **TransformationStrategy**: Unified class that handles all transformation types
2. **TransformationType**: Enum for SQL, PYTHON, and SCALA
3. **TransformationRegistry**: Manages transformation metadata
4. **TransformationLoader**: Factory for creating transformation strategies

## SQL Transformations

### Direct SQL in Contract

```yaml
customProperties:
  transformationSql: |
    SELECT
      c.customer_id,
      c.customer_name,
      COUNT(o.order_id) as total_orders,
      SUM(o.order_total) as total_revenue
    FROM bronze_sales.customers c
    LEFT JOIN bronze_sales.orders o
      ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name
```

### Features

- Access to all catalog tables
- Full Spark SQL syntax support
- Can reference temp views
- Supports complex joins and aggregations

### Limitations

- Cannot include helper functions
- Limited to SQL operations
- No programmatic logic

## Python Transformations

### Simple Python Transformation

Create a file `transformations/python/simple_aggregation.py`:

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional

def transform(
    spark: SparkSession,
    input_df: Optional[DataFrame] = None,
    **kwargs
) -> DataFrame:
    """
    Simple aggregation transformation.

    Args:
        spark: Active SparkSession
        input_df: Optional input DataFrame
        **kwargs: Configuration parameters

    Returns:
        Transformed DataFrame
    """
    # Get config
    source_catalog = kwargs.get('source_catalog', 'bronze')

    # Read data
    df = spark.table(f"{source_catalog}.orders")

    # Transform
    result = df.groupBy("customer_id").agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("total_amount")
    )

    return result
```

### Python with Helper Functions

Create `transformations/python/customer_analytics.py`:

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional

def calculate_rfm_score(df: DataFrame) -> DataFrame:
    """Helper function to calculate RFM score."""
    from pyspark.sql.window import Window

    # Calculate recency quartile
    window = Window.orderBy(F.col("last_order_date").desc())
    df = df.withColumn("recency_quartile", F.ntile(4).over(window))

    # Calculate frequency quartile
    window = Window.orderBy(F.col("order_count").desc())
    df = df.withColumn("frequency_quartile", F.ntile(4).over(window))

    # Calculate monetary quartile
    window = Window.orderBy(F.col("total_revenue").desc())
    df = df.withColumn("monetary_quartile", F.ntile(4).over(window))

    # Composite score
    df = df.withColumn(
        "rfm_score",
        F.col("recency_quartile") * 100 +
        F.col("frequency_quartile") * 10 +
        F.col("monetary_quartile")
    )

    return df


def classify_segment(df: DataFrame) -> DataFrame:
    """Helper function to classify customer segment."""
    return df.withColumn(
        "segment",
        F.when(F.col("rfm_score") >= 400, "VIP")
         .when(F.col("rfm_score") >= 300, "High Value")
         .when(F.col("rfm_score") >= 200, "Medium Value")
         .otherwise("Low Value")
    )


def transform(
    spark: SparkSession,
    input_df: Optional[DataFrame] = None,
    **kwargs
) -> DataFrame:
    """Main transformation using helpers."""

    # Read data
    customers = spark.table("bronze.customers")
    orders = spark.table("bronze.orders")

    # Aggregate
    customer_metrics = orders.groupBy("customer_id").agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("total_revenue"),
        F.max("order_date").alias("last_order_date")
    )

    # Join with customer data
    result = customers.join(customer_metrics, "customer_id", "left")

    # Apply helper functions
    result = calculate_rfm_score(result)
    result = classify_segment(result)

    return result
```

### Python Package Structure

For complex transformations with multiple files:

```
transformations/python/product_analytics/
├── __init__.py          # Main transform() function
├── helpers.py           # Helper functions
├── metrics.py           # Metrics calculator classes
└── config.py            # Configuration and constants
```

**`__init__.py`:**
```python
from .helpers import calculate_velocity, identify_trends
from .metrics import ProductMetricsCalculator

def transform(spark, input_df=None, **kwargs):
    calculator = ProductMetricsCalculator()

    # Use helpers and classes
    metrics = calculator.calculate_all_metrics(spark, **kwargs)
    metrics = calculate_velocity(metrics)
    metrics = identify_trends(metrics)

    return metrics
```

### Data Contract Configuration

```yaml
customProperties:
  # Direct Python transformation
  transformationType: python
  transformationModule: customer_analytics  # or package like product_analytics
  transformationFunction: transform  # optional, defaults to 'transform'

  # Pass config to transformation
  transformationConfig:
    source_catalog: bronze_sales
    min_threshold: 100
```

## Scala Transformations

### Implementation

Create a Scala class in your Scala project:

```scala
package com.pelagisflow.transformations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class CustomerAggregation {

  def transform(
    spark: SparkSession,
    inputDF: Option[DataFrame],
    config: Map[String, Any]
  ): DataFrame = {

    // Get configuration
    val catalog = config.getOrElse("source_catalog", "bronze").toString
    val threshold = config.getOrElse("min_threshold", 0).toString.toInt

    // Read data
    val customers = spark.table(s"$catalog.customers")
    val orders = spark.table(s"$catalog.orders")

    // Transform
    orders
      .filter(col("amount") >= threshold)
      .groupBy("customer_id")
      .agg(
        count("order_id").as("order_count"),
        sum("amount").as("total_revenue")
      )
      .join(customers, Seq("customer_id"), "left")
  }
}
```

### Build and Deploy

**Using SBT (`build.sbt`):**
```scala
name := "pelagisflow-transformations"
version := "1.0.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided"
)
```

**Build:**
```bash
sbt clean package
```

**Deploy:**
```bash
cp target/scala-2.12/pelagisflow-transformations_2.12-1.0.0.jar \
   transformations/scala/
```

### Data Contract Configuration

```yaml
customProperties:
  # Direct Scala transformation
  transformationType: scala
  transformationClass: com.pelagisflow.transformations.CustomerAggregation
  transformationJar: transformations/scala/pelagisflow-transformations_2.12-1.0.0.jar

  # Configuration
  transformationConfig:
    source_catalog: bronze_sales
    min_threshold: 100
```

## Transformation Registry

The registry provides centralized management of transformations with versioning and metadata.

### Registry File Format

Create `transformations/registry/my_transformations.yaml`:

```yaml
---
- name: customer_aggregation_v1
  type: python
  version: "1.0.0"
  description: |
    Customer aggregation with RFM scoring and segmentation.
    Includes metrics calculation and segment classification.
  module_path: customer_analytics
  function_name: transform
  author: Data Engineering Team
  tags:
    - customer
    - rfm
    - analytics
  dependencies:
    - pyspark>=3.0.0
  config_schema:
    type: object
    properties:
      source_catalog:
        type: string
        default: bronze
      min_threshold:
        type: number
        default: 0

- name: product_analytics_v1
  type: python
  version: "1.0.0"
  description: Product analytics with trending detection
  module_path: product_analytics
  function_name: transform
  author: Product Team
  tags:
    - product
    - trending
```

### Using Registry-Based Transformations

**Data Contract:**
```yaml
customProperties:
  # Reference transformation by name
  transformationName: customer_aggregation_v1

  # Override configuration
  transformationConfig:
    source_catalog: bronze_sales
    min_threshold: 50
```

### Benefits

- **Versioning**: Track transformation versions
- **Documentation**: Centralized metadata and descriptions
- **Discovery**: Find available transformations by tags
- **Reusability**: Share transformations across contracts
- **Configuration Schema**: Validate transformation config

## Data Contract Configuration

### Configuration Methods

#### 1. Inline SQL (Legacy/Simple)

```yaml
customProperties:
  transformationSql: |
    SELECT * FROM bronze.table
    WHERE condition = true
```

#### 2. Direct Python

```yaml
customProperties:
  transformationType: python
  transformationModule: my_transformation
  transformationFunction: transform  # optional
  transformationConfig:
    param1: value1
```

#### 3. Direct Scala

```yaml
customProperties:
  transformationType: scala
  transformationClass: com.example.MyTransform
  transformationJar: path/to/jar.jar  # optional if on classpath
  transformationConfig:
    param1: value1
```

#### 4. Registry-Based (Recommended)

```yaml
customProperties:
  transformationName: my_transformation_v1
  transformationConfig:
    param1: value1
```

### Complete Example

```yaml
---
apiVersion: v3.0.2
kind: DataContract
name: customer_analytics
version: "1.0.0"

domain: sales
dataProduct: customer_insights

schema:
  name: gold_sales.customer_analytics
  table: customer_analytics
  format: delta
  description: Customer analytics with RFM scoring

  properties:
    - name: customer_id
      type: bigint
      isPrimaryKey: true
      isNullable: false

    - name: customer_segment
      type: string
      isNullable: false

    - name: rfm_score
      type: integer
      isNullable: false

customProperties:
  pipelineType: transformation
  writeStrategy: overwrite

  # Transformation configuration
  transformationName: customer_aggregation_v1

  transformationConfig:
    source_catalog: bronze_sales
    min_order_threshold: 100.0
    current_date: "2024-01-01"

quality:
  validation:
    - column: customer_id
      rule: unique
      severity: error
```

## Best Practices

### General

1. **Always return a DataFrame**: All transformations must return a Spark DataFrame
2. **Validate inputs**: Check for null DataFrames and required configuration
3. **Handle errors gracefully**: Provide clear error messages
4. **Use configuration**: Make transformations configurable via kwargs
5. **Document thoroughly**: Add docstrings and comments
6. **Test extensively**: Write unit tests for transformations

### SQL Transformations

```sql
-- Good: Clear, simple aggregation
SELECT
  customer_id,
  COUNT(*) as order_count,
  SUM(amount) as total_revenue
FROM bronze.orders
GROUP BY customer_id

-- Avoid: Overly complex SQL that would be better in Python/Scala
```

### Python Transformations

```python
# Good: Modular with helpers
def calculate_metrics(df):
    """Helper to calculate metrics."""
    return df.groupBy("id").agg(F.sum("amount"))

def transform(spark, input_df=None, **kwargs):
    df = spark.table(kwargs.get('table', 'default.table'))
    return calculate_metrics(df)

# Avoid: Everything in one function
def transform(spark, input_df=None, **kwargs):
    # 200 lines of transformation logic...
    pass
```

### Scala Transformations

```scala
// Good: Type-safe with error handling
def transform(
  spark: SparkSession,
  inputDF: Option[DataFrame],
  config: Map[String, Any]
): DataFrame = {

  val threshold = config.getOrElse("threshold", 0).toString.toInt

  inputDF match {
    case Some(df) => df.filter(col("amount") >= threshold)
    case None => throw new IllegalArgumentException("Input required")
  }
}

// Avoid: Unsafe operations without validation
```

### Registry Management

```yaml
# Good: Comprehensive metadata
- name: customer_analytics_v2
  type: python
  version: "2.0.0"
  description: |
    Comprehensive customer analytics with:
    - RFM scoring
    - Segment classification
    - Lifetime value calculation
  module_path: customer_analytics
  author: data-team@example.com
  tags: [customer, analytics, rfm]
  dependencies:
    - pyspark>=3.3.0

# Avoid: Minimal metadata
- name: transform1
  type: python
  module_path: t1
```

## Testing

### Unit Testing Python Transformations

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_customer_aggregation(spark):
    """Test customer aggregation transformation."""
    # Import transformation
    from transformations.python.customer_aggregation import transform

    # Create test data
    data = [("c1", "Alice", 100), ("c2", "Bob", 200)]
    df = spark.createDataFrame(data, ["id", "name", "amount"])
    df.createOrReplaceTempView("test_table")

    # Execute transformation
    result = transform(spark, source_catalog="test")

    # Assert results
    assert result.count() == 2
    assert "rfm_score" in result.columns
```

### Integration Testing

```python
def test_transformation_via_contract(spark):
    """Test transformation via contract loading."""
    from transformation.loader import TransformationLoader

    contract = {
        'customProperties': {
            'transformationName': 'customer_aggregation_v1',
            'transformationConfig': {
                'source_catalog': 'test'
            }
        }
    }

    loader = TransformationLoader(spark)
    strategy = loader.load_from_contract(contract)

    result = strategy.transform()
    assert result is not None
```

### Testing Scala Transformations

```scala
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class CustomerAggregationTest extends AnyFunSuite {

  test("aggregation produces correct results") {
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val transformation = new CustomerAggregation()
    val result = transformation.transform(
      spark,
      None,
      Map("source_catalog" -> "test")
    )

    assert(result.count() > 0)
  }
}
```

## Troubleshooting

### Common Issues

**Problem**: `Module not found` error for Python transformation

**Solution**: Verify module path is correct relative to `transformations/python/`
```python
# If file is transformations/python/my_transform.py
transformationModule: my_transform

# If package is transformations/python/my_package/__init__.py
transformationModule: my_package
```

**Problem**: Scala class not found

**Solution**: Ensure JAR is on classpath and class name is fully qualified
```yaml
transformationClass: com.pelagisflow.transformations.MyClass  # Full package name
transformationJar: transformations/scala/my-jar.jar  # Correct path
```

**Problem**: Configuration not passed to transformation

**Solution**: Use `transformationConfig` in contract
```yaml
customProperties:
  transformationName: my_transform
  transformationConfig:  # Must be under this key
    param1: value1
```

## Next Steps

- Review example transformations in `transformations/python/`
- Check sample contracts in `samples/transformation_examples/`
- Read Scala transformation guide in `transformations/scala/README.md`
- Explore registry files in `transformations/registry/`
- Run tests: `pytest tests/test_transformation_strategies.py`
