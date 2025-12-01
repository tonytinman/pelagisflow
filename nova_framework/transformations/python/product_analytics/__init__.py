"""
Product analytics transformation package.

This package demonstrates a complex transformation with multiple helper modules.

Example usage in data contract:
    customProperties:
      transformationType: python
      transformationModule: product_analytics
      transformationFunction: transform
      transformationConfig:
        source_catalog: bronze_sales
        include_profitability: true
        velocity_window_days: 30
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Import helper modules
from .helpers import (
    calculate_product_velocity,
    identify_trending_products,
    calculate_product_seasonality,
    enrich_with_product_attributes
)
from .metrics import ProductMetricsCalculator


def transform(
    spark: SparkSession,
    input_df: Optional[DataFrame] = None,
    **kwargs
) -> DataFrame:
    """
    Main transformation function for product analytics.

    This transformation:
    1. Calculates comprehensive product metrics
    2. Identifies trending products
    3. Calculates seasonality patterns
    4. Enriches with product attributes
    5. Optionally includes profitability analysis

    Args:
        spark: Active SparkSession
        input_df: Optional input DataFrame (not used)
        **kwargs: Configuration parameters:
            - source_catalog: Source catalog name (default: bronze_sales)
            - include_profitability: Include profit metrics (default: False)
            - velocity_window_days: Days for velocity calculation (default: 30)
            - velocity_threshold: Threshold for trending (default: 0.2)

    Returns:
        DataFrame with comprehensive product analytics
    """
    # Get configuration
    source_catalog = kwargs.get('source_catalog', 'bronze_sales')
    include_profitability = kwargs.get('include_profitability', False)
    velocity_window_days = int(kwargs.get('velocity_window_days', 30))
    velocity_threshold = float(kwargs.get('velocity_threshold', 0.2))

    # Initialize calculator
    calculator = ProductMetricsCalculator()

    # Read source tables
    orders_df = spark.table(f"{source_catalog}.orders")
    products_df = spark.table(f"{source_catalog}.products")

    # Calculate basic metrics
    product_metrics = calculator.calculate_basic_metrics(orders_df)

    # Calculate customer metrics
    customer_metrics = calculator.calculate_customer_metrics(orders_df)

    # Merge metrics
    result = product_metrics.join(customer_metrics, on="product_id", how="left")

    # Calculate profitability if requested
    if include_profitability:
        # Assume product_costs table exists
        try:
            product_costs = spark.table(f"{source_catalog}.product_costs")
            result = calculator.calculate_profitability_metrics(result, product_costs)
        except Exception:
            # If costs table doesn't exist, skip profitability
            pass

    # Enrich with product attributes
    result = enrich_with_product_attributes(
        result,
        products_df,
        attributes=["product_name", "category", "brand", "supplier"]
    )

    # For velocity calculation, we need daily aggregates
    daily_sales = orders_df.groupBy("product_id", "order_date").agg(
        F.sum("quantity").alias("daily_quantity"),
        F.sum("item_total").alias("daily_revenue")
    )

    # Calculate velocity
    daily_sales_with_velocity = calculate_product_velocity(
        daily_sales,
        window_days=velocity_window_days
    )

    # Identify trending products
    daily_sales_with_velocity = identify_trending_products(
        daily_sales_with_velocity,
        velocity_threshold=velocity_threshold
    )

    # Get latest trending status per product
    from pyspark.sql.window import Window
    latest_window = Window.partitionBy("product_id").orderBy(F.col("order_date").desc())

    trending_status = daily_sales_with_velocity.withColumn(
        "row_num",
        F.row_number().over(latest_window)
    ).filter(F.col("row_num") == 1).select(
        "product_id",
        "is_trending",
        "sales_velocity"
    )

    # Join trending status
    result = result.join(trending_status, on="product_id", how="left")

    # Fill nulls
    result = result.fillna({
        "is_trending": False,
        "sales_velocity": 0.0
    })

    return result
