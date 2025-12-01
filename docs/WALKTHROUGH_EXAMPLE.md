# Complete Walkthrough: Product Sales Summary Example

## Overview

This guide walks you through a **complete, working example** of creating and running a Python transformation in PelagisFlow.

**What we'll build:** A product sales summary that aggregates order data by product, calculates metrics, and classifies products by revenue tier.

**Files involved:**
- `transformations/python/product_sales_summary_example.py` - Your transformation (✓ Already created)
- `samples/transformation_examples/EXAMPLE_product_sales_summary.yaml` - Data contract (✓ Already created)

---

## Step-by-Step Walkthrough

### Step 1: Understand What We Have

#### The Transformation File

**Location:** `transformations/python/product_sales_summary_example.py`

This file contains:
```python
class ProductSalesSummaryExample(AbstractTransformation):
    def run(self, input_df=None, **kwargs):
        # 1. Get configuration
        source_catalog = kwargs.get('source_catalog', 'bronze')
        lookback_days = kwargs.get('lookback_days', 30)

        # 2. Load and filter orders
        orders = self._load_orders(source_catalog, lookback_days, ...)

        # 3. Aggregate by product
        aggregated = self._aggregate_by_product(orders)

        # 4. Add calculated columns
        final = self._add_calculated_columns(aggregated)

        return final  # Framework takes this
```

**Key points:**
- ✅ Inherits from `AbstractTransformation`
- ✅ Implements `run()` method
- ✅ Uses `self.spark` to query tables
- ✅ Gets config from `**kwargs`
- ✅ Returns a DataFrame

#### The Data Contract

**Location:** `samples/transformation_examples/EXAMPLE_product_sales_summary.yaml`

```yaml
customProperties:
  transformationType: python
  transformationModule: product_sales_summary_example  # ← Your file
  transformationClass: ProductSalesSummaryExample      # ← Your class

  transformationConfig:
    source_catalog: bronze     # ← Passed to your run()
    lookback_days: 30          # ← Passed to your run()
    min_order_amount: 10.0     # ← Passed to your run()
```

---

### Step 2: Set Up Test Data

Before running the transformation, you need source data in the `bronze.orders` table.

#### Option A: If You Have Databricks/Spark Environment

```python
# Create test database
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Create orders table with test data
spark.sql("""
CREATE OR REPLACE TABLE bronze.orders (
    order_id BIGINT,
    product_id BIGINT,
    customer_id BIGINT,
    order_date DATE,
    order_amount DECIMAL(10,2),
    status STRING
)
""")

# Insert test data
spark.sql("""
INSERT INTO bronze.orders VALUES
    -- Product 1: High revenue
    (1, 101, 1001, '2024-11-15', 500.00, 'completed'),
    (2, 101, 1002, '2024-11-16', 750.00, 'completed'),
    (3, 101, 1003, '2024-11-17', 600.00, 'shipped'),
    (4, 101, 1001, '2024-11-18', 800.00, 'completed'),

    -- Product 2: Medium revenue
    (5, 102, 1004, '2024-11-15', 150.00, 'completed'),
    (6, 102, 1005, '2024-11-16', 200.00, 'completed'),
    (7, 102, 1006, '2024-11-17', 180.00, 'shipped'),

    -- Product 3: Low revenue
    (8, 103, 1007, '2024-11-18', 25.00, 'completed'),
    (9, 103, 1008, '2024-11-19', 30.00, 'completed'),

    -- Product 4: Below minimum (will be filtered out)
    (10, 104, 1009, '2024-11-19', 5.00, 'completed'),

    -- Old order (will be filtered out if > 30 days)
    (11, 105, 1010, '2024-01-15', 100.00, 'completed'),

    -- Cancelled order (will be filtered out)
    (12, 106, 1011, '2024-11-20', 250.00, 'cancelled')
""")

print("✓ Test data created in bronze.orders")
```

#### Option B: Quick Setup Script

Create a file `setup_test_data.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DecimalType, StringType, DateType
from datetime import date, timedelta

spark = SparkSession.builder.appName("SetupTestData").getOrCreate()

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Define schema
schema = StructType([
    StructField("order_id", LongType(), False),
    StructField("product_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("order_date", DateType(), False),
    StructField("order_amount", DecimalType(10,2), False),
    StructField("status", StringType(), False)
])

# Create test data
today = date.today()
data = [
    # Product 101: High revenue (should be classified as "High")
    (1, 101, 1001, today - timedelta(days=5), 500.00, "completed"),
    (2, 101, 1002, today - timedelta(days=4), 750.00, "completed"),
    (3, 101, 1003, today - timedelta(days=3), 600.00, "shipped"),
    (4, 101, 1001, today - timedelta(days=2), 800.00, "completed"),
    (5, 101, 1004, today - timedelta(days=1), 550.00, "completed"),

    # Product 102: Medium revenue (should be classified as "Medium")
    (6, 102, 1005, today - timedelta(days=10), 150.00, "completed"),
    (7, 102, 1006, today - timedelta(days=9), 200.00, "completed"),
    (8, 102, 1007, today - timedelta(days=8), 180.00, "shipped"),
    (9, 102, 1008, today - timedelta(days=7), 220.00, "completed"),

    # Product 103: Low revenue (should be classified as "Low")
    (10, 103, 1009, today - timedelta(days=6), 25.00, "completed"),
    (11, 103, 1010, today - timedelta(days=5), 30.00, "completed"),
    (12, 103, 1011, today - timedelta(days=4), 35.00, "completed"),
]

# Create DataFrame and save
df = spark.createDataFrame(data, schema)
df.write.mode("overwrite").saveAsTable("bronze.orders")

print(f"✓ Created bronze.orders with {df.count()} test orders")
print("\nSample data:")
df.show()
```

Run it:
```bash
spark-submit setup_test_data.py
```

---

### Step 3: Verify Test Data

Check that your test data is ready:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Check table exists and has data
df = spark.table("bronze.orders")
print(f"Total orders: {df.count()}")

# Show sample
df.show(10)

# Expected output:
# +--------+----------+-----------+----------+------------+----------+
# |order_id|product_id|customer_id|order_date|order_amount|status    |
# +--------+----------+-----------+----------+------------+----------+
# |1       |101       |1001       |2024-11-26|500.00      |completed |
# |2       |101       |1002       |2024-11-27|750.00      |completed |
# ...
```

---

### Step 4: Run the Transformation Manually (Testing)

Test your transformation directly before running through the full pipeline:

```python
from pyspark.sql import SparkSession
from transformations.python.product_sales_summary_example import ProductSalesSummaryExample

# Get SparkSession
spark = SparkSession.builder.getOrCreate()

# Instantiate your transformation (like framework does)
transformation = ProductSalesSummaryExample(spark)

# Call run() with config (like framework does)
result_df = transformation.run(
    input_df=None,  # Not used in this transformation
    source_catalog='bronze',
    lookback_days=30,
    min_order_amount=10.0
)

# Check results
print(f"\nTotal products: {result_df.count()}")
result_df.show(truncate=False)

# Expected output:
# ===================================================================================
# STARTING: Product Sales Summary Transformation
# ===================================================================================
#
# Configuration:
#   - source_catalog: bronze
#   - lookback_days: 30
#   - min_order_amount: 10.0
#
# STAGE 1: Loading orders from bronze.orders...
#   ✓ Loaded 9 orders
#
# STAGE 2: Aggregating by product_id...
#   ✓ Aggregated to 3 products
#
# STAGE 3: Adding calculated columns...
#   ✓ Added tier classification
#
# Sample Results (first 5 rows):
# +----------+------------+-------------+----------------+----------------+----------------+-----------------+-------------+---------------------+------------+
# |product_id|total_orders|total_revenue|avg_order_value|first_order_date|last_order_date|unique_customers|revenue_tier|revenue_per_customer|days_active|
# +----------+------------+-------------+----------------+----------------+----------------+-----------------+-------------+---------------------+------------+
# |101       |5           |3200.00      |640.00         |2024-11-26      |2024-11-30     |4                |High        |800.00               |4          |
# |102       |4           |750.00       |187.50         |2024-11-21      |2024-11-24     |4                |Medium      |187.50               |3          |
# |103       |3           |90.00        |30.00          |2024-11-25      |2024-11-27     |3                |Low         |30.00                |2          |
# +----------+------------+-------------+----------------+----------------+----------------+-----------------+-------------+---------------------+------------+
```

---

### Step 5: Run Through PelagisFlow Pipeline

Now run it through the full framework pipeline:

```python
from pipeline.orchestrator import PipelineOrchestrator
from contract.contract import DataContract

# Load your data contract
contract = DataContract.from_yaml("samples/transformation_examples/EXAMPLE_product_sales_summary.yaml")

# Create pipeline orchestrator
orchestrator = PipelineOrchestrator(contract)

# Execute pipeline
# Framework will:
# 1. Read your contract
# 2. Load your transformation class
# 3. Instantiate it: ProductSalesSummaryExample(spark)
# 4. Call your run() method with config
# 5. Apply quality checks
# 6. Write results to gold_sales.product_sales_summary_example
result = orchestrator.execute()

print(f"Pipeline Status: {result.status}")
print(f"Rows Processed: {result.stats.rows_written}")
```

---

### Step 6: Verify Results

Check that the transformation wrote data to the target table:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read the output table
result_df = spark.table("gold_sales.product_sales_summary_example")

print(f"Total products in output: {result_df.count()}")
result_df.show(truncate=False)

# Check revenue tiers
print("\nRevenue Tier Distribution:")
result_df.groupBy("revenue_tier").count().show()

# Expected output:
# +-------------+-----+
# |revenue_tier |count|
# +-------------+-----+
# |High         |1    |
# |Medium       |1    |
# |Low          |1    |
# +-------------+-----+
```

---

### Step 7: Modify and Experiment

Now try modifying the transformation:

#### Example 1: Change the Revenue Thresholds

Edit `transformations/python/product_sales_summary_example.py`:

```python
def _add_calculated_columns(self, df: DataFrame) -> DataFrame:
    # Change the thresholds
    df = df.withColumn(
        "revenue_tier",
        F.when(F.col("total_revenue") >= 5000, "High")      # Changed from 10000
         .when(F.col("total_revenue") >= 500, "Medium")     # Changed from 1000
         .otherwise("Low")
    )
    return df
```

Re-run and see different results!

#### Example 2: Add a New Metric

Add a new helper method:

```python
def _add_popularity_score(self, df: DataFrame) -> DataFrame:
    """Add popularity score based on order count and customers."""
    return df.withColumn(
        "popularity_score",
        (F.col("total_orders") * 0.6) + (F.col("unique_customers") * 0.4)
    )
```

Call it in `run()`:

```python
def run(self, input_df=None, **kwargs):
    # ... existing code ...
    final_df = self._add_calculated_columns(aggregated_df)
    final_df = self._add_popularity_score(final_df)  # Add this
    return final_df
```

Don't forget to add the column to your data contract!

#### Example 3: Change Configuration

Edit the data contract to change behavior:

```yaml
transformationConfig:
  source_catalog: bronze
  lookback_days: 7        # Changed from 30 - only last week
  min_order_amount: 50.0  # Changed from 10 - higher threshold
```

---

## Understanding the Flow

### What Framework Does (You Don't Write)

1. ✅ **Reads data contract** → Gets your module and class names
2. ✅ **Loads your Python file** → Imports your module dynamically
3. ✅ **Finds your class** → Gets `ProductSalesSummaryExample` from module
4. ✅ **Instantiates your class** → Calls `ProductSalesSummaryExample(spark)`
5. ✅ **Prepares config** → Extracts `transformationConfig` from contract
6. ✅ **Calls your run()** → `instance.run(input_df=None, **your_config)`
7. ✅ **Takes your DataFrame** → Uses returned DataFrame
8. ✅ **Applies quality checks** → From contract's `quality` section
9. ✅ **Writes results** → To `gold_sales.product_sales_summary_example`

### What You Write

1. ✅ **Transformation class** → `class ProductSalesSummaryExample(AbstractTransformation)`
2. ✅ **run() method** → Your entry point with transformation logic
3. ✅ **Helper methods** → `_load_orders()`, `_aggregate_by_product()`, etc.
4. ✅ **Data contract** → YAML file pointing to your class
5. ✅ **Configuration** → Define keys in `transformationConfig`

---

## Troubleshooting

### Issue: "Table bronze.orders not found"

**Solution:** Set up test data (see Step 2)

```python
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
# ... create table and insert data
```

### Issue: "Module 'product_sales_summary_example' not found"

**Solution:** Check file name matches contract

- File: `transformations/python/product_sales_summary_example.py` ✓
- Contract: `transformationModule: product_sales_summary_example` ✓

### Issue: "Class 'ProductSalesSummaryExample' not found"

**Solution:** Check class name matches contract

- Code: `class ProductSalesSummaryExample(AbstractTransformation):` ✓
- Contract: `transformationClass: ProductSalesSummaryExample` ✓

### Issue: "run() missing required positional argument"

**Solution:** Make sure run() signature is correct

```python
def run(self, input_df=None, **kwargs):  # ✓ Correct
    # not def run(self):  ✗ Wrong
```

### Issue: Configuration not passed to run()

**Solution:** Check contract has `transformationConfig` section

```yaml
customProperties:
  transformationConfig:    # ← Must be here
    source_catalog: bronze
```

---

## Next Steps

### Create Your Own Transformation

1. **Copy the example:**
   ```bash
   cp transformations/python/product_sales_summary_example.py \
      transformations/python/my_transformation.py
   ```

2. **Modify the class name:**
   ```python
   class MyTransformation(AbstractTransformation):
   ```

3. **Change the logic in run():**
   ```python
   def run(self, input_df=None, **kwargs):
       # Your custom logic here
       pass
   ```

4. **Create your data contract:**
   ```yaml
   customProperties:
     transformationModule: my_transformation
     transformationClass: MyTransformation
   ```

5. **Run it!**

### Learn More

- See `docs/DEVELOPER_GUIDE.md` for comprehensive patterns
- See `transformations/python/README.md` for more examples
- See other examples in `transformations/python/` directory

---

## Summary

You've learned:
- ✅ How to create a transformation class
- ✅ How to configure it via data contract
- ✅ How to get configuration in your code
- ✅ How to use self.spark
- ✅ How to organize complex logic with helpers
- ✅ How the framework calls your code
- ✅ How to test and verify results

**Remember:** You just write `run()` and return a DataFrame - the framework handles everything else!
