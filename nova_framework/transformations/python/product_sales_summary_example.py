"""
CONCRETE EXAMPLE: Product Sales Summary Transformation

FILE: transformations/python/product_sales_summary_example.py
PURPOSE: Complete working example showing how to build a transformation

This transformation demonstrates:
- Reading from bronze tables
- Filtering by date range
- Aggregating data
- Adding calculated columns
- Using configuration from data contract

BUSINESS LOGIC:
1. Read orders from bronze.orders table
2. Filter to last N days (configurable)
3. Aggregate by product_id
4. Calculate: total_orders, total_revenue, avg_order_value
5. Add tier classification based on revenue
"""

# ════════════════════════════════════════════════════════════════════════════
# STEP 1: Import required modules
# ════════════════════════════════════════════════════════════════════════════

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional

# Import framework base class
from nova_framework.transformations.python.base import AbstractTransformation


# ════════════════════════════════════════════════════════════════════════════
# STEP 2: Define your transformation class
# ════════════════════════════════════════════════════════════════════════════

class ProductSalesSummaryExample(AbstractTransformation):
    """
    Product Sales Summary Transformation - Complete Working Example

    This class demonstrates a typical transformation pattern:
    - Main run() method orchestrates the flow
    - Helper methods handle specific stages
    - Configuration comes from data contract
    - Returns final DataFrame to framework

    Usage in data contract:
        customProperties:
          transformationType: python
          transformationModule: product_sales_summary_example
          transformationClass: ProductSalesSummaryExample
          transformationConfig:
            source_catalog: bronze
            lookback_days: 30
            min_order_amount: 10.0
    """

    # ════════════════════════════════════════════════════════════════════════
    # STEP 3: Implement the required run() method
    # ════════════════════════════════════════════════════════════════════════

    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Main entry point - Framework calls this method.

        Args:
            input_df: Not used in this example (we read from tables)
            **kwargs: Configuration from data contract's transformationConfig

        Returns:
            DataFrame: Product sales summary with metrics
        """
        # ═══════════════════════════════════════════════════════════════════
        # YOUR TRANSFORMATION LOGIC STARTS HERE
        # ═══════════════════════════════════════════════════════════════════

        print("=" * 80)
        print("STARTING: Product Sales Summary Transformation")
        print("=" * 80)

        # STAGE 1: Get configuration from data contract
        # These come from your data contract's transformationConfig section
        source_catalog = kwargs.get('source_catalog', 'bronze')
        lookback_days = int(kwargs.get('lookback_days', 30))
        min_order_amount = float(kwargs.get('min_order_amount', 0.0))

        print(f"\nConfiguration:")
        print(f"  - source_catalog: {source_catalog}")
        print(f"  - lookback_days: {lookback_days}")
        print(f"  - min_order_amount: {min_order_amount}")

        # STAGE 2: Load and filter orders
        print(f"\nSTAGE 1: Loading orders from {source_catalog}.orders...")
        orders_df = self._load_orders(source_catalog, lookback_days, min_order_amount)
        print(f"  ✓ Loaded {orders_df.count()} orders")

        # STAGE 3: Aggregate by product
        print(f"\nSTAGE 2: Aggregating by product_id...")
        aggregated_df = self._aggregate_by_product(orders_df)
        print(f"  ✓ Aggregated to {aggregated_df.count()} products")

        # STAGE 4: Add calculated columns
        print(f"\nSTAGE 3: Adding calculated columns...")
        final_df = self._add_calculated_columns(aggregated_df)
        print(f"  ✓ Added tier classification")

        # STAGE 5: Show sample results
        print(f"\nSample Results (first 5 rows):")
        final_df.show(5, truncate=False)

        print("\n" + "=" * 80)
        print("COMPLETED: Product Sales Summary Transformation")
        print("=" * 80 + "\n")

        # Return final DataFrame to framework
        return final_df

        # ═══════════════════════════════════════════════════════════════════
        # YOUR TRANSFORMATION LOGIC ENDS HERE - Framework takes over
        # ═══════════════════════════════════════════════════════════════════

    # ════════════════════════════════════════════════════════════════════════
    # STEP 4: Add helper methods to organize your logic
    # ════════════════════════════════════════════════════════════════════════

    def _load_orders(
        self,
        catalog: str,
        lookback_days: int,
        min_amount: float
    ) -> DataFrame:
        """
        Helper: Load and filter orders from bronze table.

        This method demonstrates:
        - Using self.spark to query tables
        - Applying date filtering
        - Applying business rules (min amount)

        Args:
            catalog: Catalog name (e.g., 'bronze')
            lookback_days: Number of days to look back
            min_amount: Minimum order amount to include

        Returns:
            Filtered orders DataFrame
        """
        # Use self.spark (provided by framework) to query table
        orders_df = self.spark.table(f"{catalog}.orders")

        # Apply date filter - last N days
        filtered_df = orders_df.filter(
            F.col("order_date") >= F.date_sub(F.current_date(), lookback_days)
        )

        # Apply amount filter - minimum order amount
        filtered_df = filtered_df.filter(
            F.col("order_amount") >= min_amount
        )

        # Only include completed orders
        filtered_df = filtered_df.filter(
            F.col("status").isin("completed", "shipped")
        )

        return filtered_df

    def _aggregate_by_product(self, orders_df: DataFrame) -> DataFrame:
        """
        Helper: Aggregate orders by product.

        This method demonstrates:
        - Grouping data
        - Multiple aggregation functions
        - Column aliasing

        Args:
            orders_df: Filtered orders DataFrame

        Returns:
            Aggregated DataFrame with product metrics
        """
        return orders_df.groupBy("product_id").agg(
            # Count total orders
            F.count("order_id").alias("total_orders"),

            # Sum total revenue
            F.sum("order_amount").alias("total_revenue"),

            # Calculate average order value
            F.avg("order_amount").alias("avg_order_value"),

            # Get date range
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),

            # Count unique customers
            F.countDistinct("customer_id").alias("unique_customers")
        )

    def _add_calculated_columns(self, df: DataFrame) -> DataFrame:
        """
        Helper: Add derived/calculated columns.

        This method demonstrates:
        - Adding new columns based on existing data
        - Using conditional logic (CASE WHEN)
        - Calculating additional metrics

        Args:
            df: Aggregated DataFrame

        Returns:
            DataFrame with additional calculated columns
        """
        # Add revenue tier classification
        df = df.withColumn(
            "revenue_tier",
            F.when(F.col("total_revenue") >= 10000, "High")
             .when(F.col("total_revenue") >= 1000, "Medium")
             .otherwise("Low")
        )

        # Add revenue per customer
        df = df.withColumn(
            "revenue_per_customer",
            F.round(F.col("total_revenue") / F.col("unique_customers"), 2)
        )

        # Add days active (difference between first and last order)
        df = df.withColumn(
            "days_active",
            F.datediff(F.col("last_order_date"), F.col("first_order_date"))
        )

        # Sort by total revenue descending
        df = df.orderBy(F.col("total_revenue").desc())

        return df

    # ════════════════════════════════════════════════════════════════════════
    # STEP 5: (Optional) Override validate() for custom validation
    # ════════════════════════════════════════════════════════════════════════

    def validate(self) -> bool:
        """
        Optional: Validate that required tables exist before running.

        This method demonstrates:
        - Pre-execution validation
        - Checking table existence
        - Returning True/False

        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check if orders table exists
            self.spark.table("bronze.orders")
            print("✓ Validation passed: bronze.orders table exists")
            return True
        except Exception as e:
            print(f"✗ Validation failed: {str(e)}")
            return False


# ════════════════════════════════════════════════════════════════════════════
# NOTES FOR DEVELOPERS:
# ════════════════════════════════════════════════════════════════════════════
#
# 1. This class inherits from AbstractTransformation (framework provides)
# 2. You must implement run() - it's your main entry point
# 3. Use self.spark to access SparkSession (framework provides)
# 4. Get config from **kwargs (comes from data contract)
# 5. Organize complex logic into helper methods (_load_orders, etc.)
# 6. Return a DataFrame from run() (framework expects this)
# 7. Framework handles instantiation and calling your code
#
# ════════════════════════════════════════════════════════════════════════════
