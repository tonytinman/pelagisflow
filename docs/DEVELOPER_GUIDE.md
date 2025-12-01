# Python Transformation Developer Guide

## Overview

This guide explains **what you write** vs **what the framework provides**, with clear execution flow and examples.

---

## Quick Start: What You Need to Know

### ✅ Framework Provides (You Don't Write This)
- Base classes (`AbstractTransformation`)
- Loading and execution logic
- Integration with data contracts
- SparkSession management
- Error handling and validation

### ✅ You Write (Developer Code)
- Your transformation class (inherits from `AbstractTransformation`)
- The `run()` method (single entry point)
- Helper methods for your logic
- Business rules and transformations

---

## Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. DATA CONTRACT (YAML)                                         │
│    - Written by: Developer                                       │
│    - File: samples/transformation_examples/*.yaml                │
│                                                                  │
│    customProperties:                                             │
│      transformationType: python                                  │
│      transformationModule: my_transformation   # Your file      │
│      transformationClass: MyTransformation    # Your class      │
│      transformationConfig:                                       │
│        source_catalog: bronze                 # Your config     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. FRAMEWORK: TransformationStage                                │
│    - File: pipeline/stages/transformaton_stage.py                │
│    - You don't modify this                                       │
│                                                                  │
│    def execute(self, df):                                        │
│        # Framework loads your transformation                     │
│        strategy = self._load_transformation_strategy()           │
│        result_df = strategy.transform(input_df=df)               │
│        return result_df                                          │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. FRAMEWORK: TransformationLoader                               │
│    - File: transformation/loader.py                              │
│    - You don't modify this                                       │
│                                                                  │
│    def load_from_contract(contract):                             │
│        # Framework reads your module and class name              │
│        module_path = contract['transformationModule']            │
│        class_name = contract['transformationClass']              │
│        # Framework creates strategy                              │
│        return TransformationStrategy(...)                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. FRAMEWORK: TransformationStrategy                             │
│    - File: transformation/strategy.py                            │
│    - You don't modify this                                       │
│                                                                  │
│    def _load_python_module(self):                                │
│        # Framework dynamically loads your Python file            │
│        module = load("transformations/python/my_transformation") │
│        transform_class = getattr(module, "MyTransformation")     │
│        # Framework creates wrapper                               │
│        return self._create_class_wrapper(transform_class)        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. FRAMEWORK: Wrapper Function                                   │
│    - File: transformation/strategy.py                            │
│    - You don't modify this                                       │
│                                                                  │
│    def wrapper(spark, input_df=None, **kwargs):                  │
│        # Framework instantiates YOUR class                       │
│        instance = MyTransformation(spark)  # <-- Your class      │
│        # Framework calls YOUR run() method                       │
│        return instance.run(input_df, **kwargs)  # <-- Your code │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 6. YOUR CODE: MyTransformation.run()                             │
│    - File: transformations/python/my_transformation.py           │
│    - You write this                                              │
│                                                                  │
│    class MyTransformation(AbstractTransformation):               │
│        def run(self, input_df=None, **kwargs):                   │
│            # YOUR LOGIC HERE                                     │
│            df = self.spark.table("bronze.orders")                │
│            result = self._my_helper_method(df)                   │
│            return result  # Must return DataFrame                │
│                                                                  │
│        def _my_helper_method(self, df):                          │
│            # YOUR HELPER LOGIC                                   │
│            return df.filter("active = true")                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ 7. FRAMEWORK: Continue Pipeline                                  │
│    - Framework receives your DataFrame                           │
│    - Continues with quality checks, writes, etc.                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## File Structure: What Goes Where

```
pelagisflow/
├── transformation/                    # ← FRAMEWORK CODE (don't modify)
│   ├── strategy.py                   # Loads and executes transformations
│   ├── loader.py                     # Creates strategy from contract
│   └── registry.py                   # Manages transformation catalog
│
├── pipeline/stages/                   # ← FRAMEWORK CODE (don't modify)
│   └── transformaton_stage.py        # Calls your transformation
│
├── transformations/                   # ← YOUR CODE (you write this)
│   ├── python/
│   │   ├── base.py                   # ← FRAMEWORK: Base class to inherit
│   │   │
│   │   ├── my_transformation.py      # ← YOU WRITE: Your transformation
│   │   ├── customer_metrics.py       # ← YOU WRITE: Another transformation
│   │   ├── sales_summary.py          # ← YOU WRITE: Another transformation
│   │   │
│   │   └── common/                   # ← YOU WRITE: Shared utilities
│   │       ├── aggregation_base.py   # ← YOU WRITE: Reusable base classes
│   │       └── utils.py              # ← YOU WRITE: Helper functions
│   │
│   └── registry/                      # ← YOU CONFIGURE: Register transformations
│       └── python_transformations.yaml
│
└── samples/                           # ← YOU WRITE: Data contracts
    └── transformation_examples/
        └── my_contract.yaml          # ← YOU WRITE: Your contract
```

---

## Developer Workflow: Step-by-Step

### Step 1: Create Your Transformation File

**File: `transformations/python/my_transformation.py`** ← YOU CREATE THIS

```python
# ============================================================================
# YOUR CODE: This entire file is written by you
# ============================================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional

# Import framework base class (provided by framework)
from transformations.python.base import AbstractTransformation


class MyTransformation(AbstractTransformation):
    """
    YOUR TRANSFORMATION CLASS

    You write this class. The framework will:
    1. Load this file
    2. Instantiate this class
    3. Call your run() method
    4. Pass the result DataFrame to the next stage
    """

    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        YOUR MAIN ENTRY POINT - You must implement this method.

        The framework calls this method and expects a DataFrame back.

        Args:
            input_df: Optional input DataFrame (usually None for transformations)
            **kwargs: Configuration from your data contract's transformationConfig
                     - You define what config keys you need
                     - Access them with kwargs.get('key_name', default_value)

        Returns:
            DataFrame: Your final transformed data
        """
        # ═══════════════════════════════════════════════════════════════
        # YOUR LOGIC STARTS HERE
        # ═══════════════════════════════════════════════════════════════

        # Get configuration from data contract (you define these keys)
        source_catalog = kwargs.get('source_catalog', 'bronze')
        min_amount = kwargs.get('min_amount', 0)

        # Stage 1: Load data using self.spark (provided by framework)
        orders_df = self._load_orders(source_catalog)

        # Stage 2: Apply your business logic
        filtered_df = self._filter_orders(orders_df, min_amount)

        # Stage 3: Aggregate
        result_df = self._aggregate_by_customer(filtered_df)

        # Must return a DataFrame
        return result_df

        # ═══════════════════════════════════════════════════════════════
        # YOUR LOGIC ENDS HERE - Framework takes over
        # ═══════════════════════════════════════════════════════════════

    # ═══════════════════════════════════════════════════════════════════
    # YOUR HELPER METHODS - Organize your logic however you want
    # ═══════════════════════════════════════════════════════════════════

    def _load_orders(self, catalog: str) -> DataFrame:
        """YOUR HELPER: Load orders from catalog."""
        # You have access to self.spark (SparkSession provided by framework)
        return self.spark.table(f"{catalog}.orders")

    def _filter_orders(self, df: DataFrame, min_amount: float) -> DataFrame:
        """YOUR HELPER: Filter orders by minimum amount."""
        return df.filter(F.col("order_total") >= min_amount)

    def _aggregate_by_customer(self, df: DataFrame) -> DataFrame:
        """YOUR HELPER: Aggregate orders by customer."""
        return df.groupBy("customer_id").agg(
            F.count("order_id").alias("order_count"),
            F.sum("order_total").alias("total_revenue"),
            F.avg("order_total").alias("avg_order_value")
        )
```

### Step 2: Create Your Data Contract

**File: `samples/transformation_examples/my_contract.yaml`** ← YOU CREATE THIS

```yaml
# ============================================================================
# YOUR CONFIGURATION: You write this data contract
# ============================================================================
apiVersion: v3.0.2
kind: DataContract
name: customer_order_summary
version: "1.0.0"

schema:
  name: gold_sales.customer_order_summary
  table: customer_order_summary
  format: delta

  properties:
    - name: customer_id
      type: bigint
      isPrimaryKey: true

    - name: order_count
      type: bigint

    - name: total_revenue
      type: decimal(18,2)

customProperties:
  pipelineType: transformation
  writeStrategy: overwrite

  # Tell framework to use Python transformation
  transformationType: python

  # YOUR PYTHON FILE (without .py extension)
  transformationModule: my_transformation

  # YOUR CLASS NAME (inside the Python file)
  transformationClass: MyTransformation

  # YOUR CONFIGURATION (passed to your run() method as **kwargs)
  transformationConfig:
    source_catalog: bronze_sales  # You define these keys
    min_amount: 100.0             # You define these keys
```

### Step 3: Framework Executes Your Code

**You don't write this - the framework handles it automatically**

```python
# ============================================================================
# FRAMEWORK CODE: transformation/strategy.py (you don't modify)
# ============================================================================

def _load_python_module(self):
    """
    FRAMEWORK: Loads your Python file and class.
    You don't call this - the framework does.
    """
    # Framework finds your file
    module_file = Path("transformations/python") / f"{self.module_path}.py"

    # Framework loads your Python module
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Framework gets your class
    transform_class = getattr(module, self.function_name)  # Your class

    # Framework creates wrapper for your class
    return self._create_class_wrapper(transform_class)

def _create_class_wrapper(self, transform_class):
    """
    FRAMEWORK: Creates wrapper to call your class.
    You don't call this - the framework does.
    """
    def wrapper(spark, input_df=None, **kwargs):
        # Framework instantiates YOUR class
        instance = transform_class(spark)  # ← Your MyTransformation(spark)

        # Framework calls YOUR run() method
        return instance.run(input_df, **kwargs)  # ← Your run() method

    return wrapper
```

---

## Pattern 1: Simple Class-Based Transformation

**What you write:**

```python
# File: transformations/python/simple_example.py
# ════════════════════════════════════════════════════════════════════════════
# YOU WRITE THIS ENTIRE FILE
# ════════════════════════════════════════════════════════════════════════════

from transformations.python.base import AbstractTransformation  # Framework provides
from pyspark.sql import DataFrame, functions as F
from typing import Optional


class SimpleExample(AbstractTransformation):
    """Simple transformation - just implement run()"""

    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """Your code here - return a DataFrame."""

        # Get config from contract
        table_name = kwargs.get('source_table', 'bronze.sales')

        # Load and transform
        df = self.spark.table(table_name)  # self.spark provided by framework

        # Apply logic
        result = df.filter("amount > 100") \
                   .groupBy("customer_id") \
                   .agg(F.sum("amount").alias("total"))

        return result  # Framework takes this DataFrame
```

**Your data contract:**

```yaml
# File: samples/transformation_examples/simple_example.yaml
# ════════════════════════════════════════════════════════════════════════════
# YOU WRITE THIS FILE
# ════════════════════════════════════════════════════════════════════════════
customProperties:
  transformationType: python
  transformationModule: simple_example      # ← Your file name
  transformationClass: SimpleExample        # ← Your class name
  transformationConfig:
    source_table: bronze_sales.orders       # ← Your config
```

---

## Pattern 2: Multi-Stage Transformation with Helpers

**What you write:**

```python
# File: transformations/python/customer_analytics.py
# ════════════════════════════════════════════════════════════════════════════
# YOU WRITE THIS ENTIRE FILE
# ════════════════════════════════════════════════════════════════════════════

from transformations.python.base import AbstractTransformation
from pyspark.sql import DataFrame, functions as F
from typing import Optional


class CustomerAnalytics(AbstractTransformation):
    """
    YOUR COMPLEX TRANSFORMATION

    Organize with helper methods for each stage.
    """

    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        YOUR MAIN ORCHESTRATION

        Call your helper methods in sequence.
        """
        # Get your configuration
        catalog = kwargs.get('source_catalog', 'bronze')
        days = kwargs.get('lookback_days', 90)

        # YOUR STAGE 1: Load data
        orders = self._load_orders(catalog, days)
        customers = self._load_customers(catalog)

        # YOUR STAGE 2: Join and enrich
        enriched = self._enrich_with_customers(orders, customers)

        # YOUR STAGE 3: Calculate metrics
        with_metrics = self._calculate_metrics(enriched)

        # YOUR STAGE 4: Classify segments
        final = self._classify_segments(with_metrics)

        return final  # Framework takes over

    # ════════════════════════════════════════════════════════════════════════
    # YOUR HELPER METHODS - You organize your logic
    # ════════════════════════════════════════════════════════════════════════

    def _load_orders(self, catalog: str, days: int) -> DataFrame:
        """YOUR HELPER: Load and filter orders."""
        return self.spark.sql(f"""
            SELECT * FROM {catalog}.orders
            WHERE order_date >= current_date() - INTERVAL {days} DAYS
        """)

    def _load_customers(self, catalog: str) -> DataFrame:
        """YOUR HELPER: Load customer dimension."""
        return self.spark.table(f"{catalog}.customers")

    def _enrich_with_customers(self, orders: DataFrame, customers: DataFrame) -> DataFrame:
        """YOUR HELPER: Join orders with customer data."""
        return orders.join(customers, "customer_id", "left")

    def _calculate_metrics(self, df: DataFrame) -> DataFrame:
        """YOUR HELPER: Calculate customer metrics."""
        return df.groupBy("customer_id").agg(
            F.count("order_id").alias("order_count"),
            F.sum("amount").alias("total_revenue")
        )

    def _classify_segments(self, df: DataFrame) -> DataFrame:
        """YOUR HELPER: Add customer segment classification."""
        return df.withColumn(
            "segment",
            F.when(F.col("total_revenue") > 10000, "VIP")
             .when(F.col("total_revenue") > 1000, "Premium")
             .otherwise("Standard")
        )
```

---

## Pattern 3: Reusable Base Classes

**Create reusable base classes for common patterns:**

```python
# File: transformations/python/common/my_base_classes.py
# ════════════════════════════════════════════════════════════════════════════
# YOU WRITE REUSABLE BASE CLASSES (shared across transformations)
# ════════════════════════════════════════════════════════════════════════════

from transformations.python.base import AbstractTransformation
from pyspark.sql import DataFrame
from typing import List


class MyAggregationBase(AbstractTransformation):
    """
    YOUR REUSABLE BASE CLASS

    Create template for common aggregation pattern.
    Subclasses only need to implement get_metrics().
    """

    def run(self, input_df=None, **kwargs):
        """YOUR TEMPLATE: Standard aggregation flow."""

        # YOUR TEMPLATE: Load source data
        source_table = kwargs.get('source_table')
        df = self.spark.table(source_table)

        # YOUR TEMPLATE: Filter
        df = self._filter_data(df, **kwargs)

        # YOUR TEMPLATE: Aggregate with metrics from subclass
        group_by = kwargs.get('group_by_columns', [])
        metrics = self.get_metrics()  # Subclass implements this
        result = df.groupBy(*group_by).agg(*metrics)

        return result

    def _filter_data(self, df: DataFrame, **kwargs) -> DataFrame:
        """YOUR HELPER: Default filtering (can be overridden)."""
        return df

    def get_metrics(self) -> List:
        """YOUR TEMPLATE: Subclasses must implement this."""
        raise NotImplementedError("Subclass must implement get_metrics()")
```

**Use your reusable base class:**

```python
# File: transformations/python/daily_sales.py
# ════════════════════════════════════════════════════════════════════════════
# YOU WRITE SPECIFIC TRANSFORMATIONS (inherit from your base)
# ════════════════════════════════════════════════════════════════════════════

from pyspark.sql import functions as F
from transformations.python.common.my_base_classes import MyAggregationBase


class DailySales(MyAggregationBase):
    """
    YOUR SPECIFIC TRANSFORMATION

    Inherits template from MyAggregationBase.
    Only needs to implement get_metrics().
    """

    def get_metrics(self):
        """YOUR IMPLEMENTATION: Define your specific metrics."""
        return [
            F.count("order_id").alias("order_count"),
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_order_value")
        ]

    def _filter_data(self, df, **kwargs):
        """YOUR OVERRIDE: Add custom filtering."""
        days = kwargs.get('lookback_days', 90)
        return df.filter(
            F.col("order_date") >= F.date_sub(F.current_date(), days)
        )
```

---

## What Framework Provides vs What You Write

### Framework Provides (You Don't Write)

| Component | File | What It Does |
|-----------|------|--------------|
| `AbstractTransformation` | `transformations/python/base.py` | Base class you inherit from |
| `TransformationStrategy` | `transformation/strategy.py` | Loads and executes your code |
| `TransformationLoader` | `transformation/loader.py` | Creates strategy from contract |
| `TransformationStage` | `pipeline/stages/transformaton_stage.py` | Calls your transformation |
| SparkSession | Provided to your class | Access via `self.spark` |

### You Write (Developer Code)

| Component | File | What You Write |
|-----------|------|----------------|
| Transformation class | `transformations/python/your_file.py` | Your logic inheriting from `AbstractTransformation` |
| `run()` method | Inside your class | Your transformation logic |
| Helper methods | Inside your class | Your helper functions (optional) |
| Base classes | `transformations/python/common/*.py` | Your reusable base classes (optional) |
| Utilities | `transformations/python/common/utils.py` | Your shared functions (optional) |
| Data contract | `samples/*.yaml` | Configuration for your transformation |

---

## Key Concepts

### 1. SparkSession Access

```python
# Framework provides self.spark in your class
class MyTransform(AbstractTransformation):
    def run(self, input_df=None, **kwargs):
        # You have access to self.spark everywhere
        df1 = self.spark.table("bronze.orders")  # ✓ Works
        df2 = self.spark.sql("SELECT * FROM ...")  # ✓ Works
        return df1
```

### 2. Configuration from Contract

```python
# In your data contract:
# transformationConfig:
#   source_catalog: bronze_sales
#   min_amount: 100
#   lookback_days: 90

# In your run() method:
def run(self, input_df=None, **kwargs):
    # Access config with kwargs.get()
    catalog = kwargs.get('source_catalog', 'bronze')  # Your key
    min_amt = kwargs.get('min_amount', 0)             # Your key
    days = kwargs.get('lookback_days', 90)            # Your key

    # Use your config
    df = self.spark.table(f"{catalog}.orders")
```

### 3. Return Value

```python
def run(self, input_df=None, **kwargs):
    # Do your transformations
    result = ...

    # MUST return a DataFrame
    return result  # ← Framework expects DataFrame
```

### 4. Error Handling

```python
def run(self, input_df=None, **kwargs):
    try:
        # Your transformation logic
        result = self._process_data(**kwargs)
        return result
    except Exception as e:
        # Framework will catch and log the error
        raise ValueError(f"Transformation failed: {str(e)}")
```

---

## Common Questions

### Q: Do I need to create the SparkSession?
**A:** No. Framework provides it via `self.spark`

### Q: How do I pass configuration to my transformation?
**A:** Add `transformationConfig` in your data contract. Access via `kwargs.get()`

### Q: Can I have multiple classes in one file?
**A:** Yes, but only specify one in `transformationClass`. Others can be helpers.

### Q: Can I import from other files?
**A:** Yes! Use normal Python imports from `transformations.python.*`

### Q: Do I need to register my transformation?
**A:** No (optional). Direct class usage doesn't need registry.

### Q: How do I test my transformation?
**A:** Create unit tests that instantiate your class and call `run()`

---

## Next Steps

1. **Copy an example** - Start with `customer_360_class.py`
2. **Modify the logic** - Change to your business logic
3. **Create your contract** - Point to your class
4. **Run pipeline** - Framework handles the rest

The framework handles all the plumbing - you just write `run()` and return a DataFrame!
