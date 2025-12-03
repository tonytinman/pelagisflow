# Python Transformations

PelagisFlow supports two patterns for Python transformations:
1. **Function-based** - Simple function with `transform()` signature
2. **Class-based** - Object-oriented classes inheriting from `AbstractTransformation`

## Pattern 1: Function-Based (Simple)

Best for simple transformations without much complexity.

### Example

```python
# transformations/python/simple_aggregation.py

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Optional

def transform(spark: SparkSession, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
    """
    Simple aggregation transformation.

    Args:
        spark: Active SparkSession
        input_df: Optional input DataFrame
        **kwargs: Configuration from data contract

    Returns:
        Transformed DataFrame
    """
    source_table = kwargs.get('source_table', 'bronze.orders')

    df = spark.table(source_table)

    result = df.groupBy("customer_id").agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("total_amount")
    )

    return result
```

### Data Contract

```yaml
customProperties:
  transformationType: python
  transformationModule: simple_aggregation
  # transformationFunction: transform  # Optional, defaults to 'transform'
  transformationConfig:
    source_table: bronze_sales.orders
```

## Pattern 2: Class-Based (Structured)

Best for complex transformations with multiple stages and reusable logic.

### Basic Example

```python
# transformations/python/customer_metrics.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional

from nova_framework.transformations.python.base import AbstractTransformation


class CustomerMetrics(AbstractTransformation):
    """
    Customer metrics transformation.

    This class-based approach provides:
    - Better organization for complex logic
    - Helper methods for stage separation
    - Reusable through inheritance
    """

    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Main entry point - must be implemented.

        Args:
            input_df: Optional input DataFrame
            **kwargs: Configuration from contract

        Returns:
            Final transformed DataFrame
        """
        # Get configuration
        source_catalog = kwargs.get('source_catalog', 'bronze')

        # Stage 1: Load data
        orders = self._load_orders(source_catalog)
        customers = self._load_customers(source_catalog)

        # Stage 2: Join and calculate
        metrics = self._calculate_metrics(orders, customers)

        # Stage 3: Add derived columns
        final = self._add_segments(metrics)

        return final

    def _load_orders(self, catalog: str) -> DataFrame:
        """Helper method: Load orders."""
        return self.spark.table(f"{catalog}.orders")

    def _load_customers(self, catalog: str) -> DataFrame:
        """Helper method: Load customers."""
        return self.spark.table(f"{catalog}.customers")

    def _calculate_metrics(self, orders: DataFrame, customers: DataFrame) -> DataFrame:
        """Helper method: Calculate customer metrics."""
        metrics = orders.groupBy("customer_id").agg(
            F.count("order_id").alias("order_count"),
            F.sum("amount").alias("total_revenue")
        )
        return customers.join(metrics, "customer_id", "left")

    def _add_segments(self, df: DataFrame) -> DataFrame:
        """Helper method: Add customer segments."""
        return df.withColumn(
            "segment",
            F.when(F.col("total_revenue") > 10000, "VIP")
             .otherwise("Regular")
        )
```

### Data Contract

```yaml
customProperties:
  transformationType: python
  transformationModule: customer_metrics
  transformationClass: CustomerMetrics  # Specify the class name
  transformationConfig:
    source_catalog: bronze_sales
```

## Pattern 3: Reusable Base Classes

Create base classes with common logic that can be reused across multiple transformations.

### Creating a Base Class

```python
# transformations/python/common/aggregation_base.py

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from nova_framework.transformations.python.base import AbstractTransformation


class AggregationBase(AbstractTransformation):
    """
    Reusable base class for aggregation transformations.

    Provides a template for common aggregation patterns.
    Subclasses only need to implement get_aggregation_expressions().
    """

    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        # Standard aggregation flow
        source_table = kwargs.get('source_table')
        df = self.spark.table(source_table)

        # Filter
        df = self.filter_data(df, **kwargs)

        # Aggregate
        group_by = kwargs.get('group_by_columns', [])
        agg_exprs = self.get_aggregation_expressions(**kwargs)
        result = df.groupBy(*group_by).agg(*agg_exprs)

        # Post-process
        return self.post_process(result, **kwargs)

    def filter_data(self, df: DataFrame, **kwargs) -> DataFrame:
        """Override to add custom filtering."""
        return df

    def get_aggregation_expressions(self, **kwargs) -> List:
        """Must be implemented by subclasses."""
        raise NotImplementedError()

    def post_process(self, df: DataFrame, **kwargs) -> DataFrame:
        """Override to add post-aggregation logic."""
        return df
```

### Using the Base Class

```python
# transformations/python/daily_sales.py

from pyspark.sql import functions as F
from nova_framework.transformations.python.common.aggregation_base import AggregationBase


class DailySales(AggregationBase):
    """
    Daily sales aggregation - inherits structure from AggregationBase.

    Only needs to implement specific aggregation logic.
    """

    def get_aggregation_expressions(self, **kwargs):
        """Define sales metrics."""
        return [
            F.count("order_id").alias("order_count"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_order_value")
        ]

    def filter_data(self, df, **kwargs):
        """Filter to last 90 days."""
        lookback = kwargs.get('lookback_days', 90)
        return df.filter(
            F.col("order_date") >= F.date_sub(F.current_date(), lookback)
        )
```

## Benefits of Class-Based Approach

### 1. **Organization**
- Clear separation of stages
- Helper methods for complex logic
- Easy to read and maintain

### 2. **Reusability**
- Create base classes for common patterns
- Share logic across transformations
- DRY (Don't Repeat Yourself)

### 3. **Testability**
- Test individual helper methods
- Mock dependencies easily
- Better unit test coverage

### 4. **Scalability**
- Add new stages without cluttering code
- Extract common logic to utilities
- Team-friendly structure

## Helper Classes and Utilities

You can create shared utilities that any transformation can use:

```python
# transformations/python/common/utils.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_date_columns(df: DataFrame, date_col: str) -> DataFrame:
    """Add common date dimension columns."""
    return df.withColumn("year", F.year(date_col)) \
             .withColumn("month", F.month(date_col)) \
             .withColumn("day", F.dayofmonth(date_col))


def classify_amount(df: DataFrame, amount_col: str) -> DataFrame:
    """Add amount classification."""
    return df.withColumn(
        "amount_tier",
        F.when(F.col(amount_col) > 1000, "High")
         .when(F.col(amount_col) > 100, "Medium")
         .otherwise("Low")
    )
```

Use in your transformations:

```python
from nova_framework.transformations.python.common.utils import add_date_columns, classify_amount

class MyTransformation(AbstractTransformation):
    def run(self, input_df=None, **kwargs):
        df = self.spark.table("bronze.orders")

        # Use shared utilities
        df = add_date_columns(df, "order_date")
        df = classify_amount(df, "order_total")

        return df
```

## Multi-Stage Transformations

Both patterns support multi-stage transformations naturally:

```python
class MultiStageTransformation(AbstractTransformation):
    def run(self, input_df=None, **kwargs):
        # Stage 1: Filter with SQL
        filtered = self.spark.sql("""
            SELECT * FROM bronze.orders
            WHERE status = 'completed'
        """)

        # Stage 2: Python transformation
        enriched = self._enrich_data(filtered)

        # Stage 3: More SQL
        enriched.createOrReplaceTempView("enriched_orders")
        aggregated = self.spark.sql("""
            SELECT customer_id, COUNT(*) as order_count
            FROM enriched_orders
            GROUP BY customer_id
        """)

        # Stage 4: Final Python logic
        final = self._add_segments(aggregated)

        return final
```

## Best Practices

1. **Use function-based for simple transformations** (<50 lines)
2. **Use class-based for complex transformations** (multiple stages, >50 lines)
3. **Create base classes for repeated patterns**
4. **Keep helper methods focused** (single responsibility)
5. **Document each stage** in docstrings
6. **Use meaningful method names** (`_calculate_metrics` not `_process1`)
7. **Test helper methods independently**

## Examples in This Directory

- `customer_aggregation.py` - Function-based with helpers
- `customer_360_class.py` - Class-based multi-stage
- `daily_sales_summary.py` - Using reusable base class
- `product_analytics/` - Package with multiple modules
- `common/` - Shared base classes and utilities
