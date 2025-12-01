"""
Helper functions for product analytics transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List


def calculate_product_velocity(df: DataFrame, window_days: int = 30) -> DataFrame:
    """
    Calculate product sales velocity (trend).

    Args:
        df: DataFrame with product sales data
        window_days: Number of days for velocity calculation

    Returns:
        DataFrame with velocity metrics
    """
    from pyspark.sql.window import Window

    # Create window for moving average
    days_window = Window.partitionBy("product_id").orderBy("order_date").rowsBetween(
        -window_days, 0
    )

    df = df.withColumn(
        "moving_avg_daily_sales",
        F.avg("daily_quantity").over(days_window)
    )

    df = df.withColumn(
        "moving_avg_daily_revenue",
        F.avg("daily_revenue").over(days_window)
    )

    # Calculate velocity (rate of change)
    df = df.withColumn(
        "sales_velocity",
        (F.col("daily_quantity") - F.col("moving_avg_daily_sales")) /
        F.when(F.col("moving_avg_daily_sales") > 0, F.col("moving_avg_daily_sales")).otherwise(1)
    )

    return df


def identify_trending_products(
    df: DataFrame,
    velocity_threshold: float = 0.2,
    min_sales_threshold: int = 10
) -> DataFrame:
    """
    Identify trending products based on velocity and sales volume.

    Args:
        df: DataFrame with product velocity data
        velocity_threshold: Minimum velocity to be considered trending
        min_sales_threshold: Minimum sales volume required

    Returns:
        DataFrame with trending flag
    """
    return df.withColumn(
        "is_trending",
        (F.col("sales_velocity") > velocity_threshold) &
        (F.col("moving_avg_daily_sales") >= min_sales_threshold)
    )


def calculate_product_seasonality(df: DataFrame) -> DataFrame:
    """
    Calculate seasonality patterns for products.

    Args:
        df: DataFrame with time-series product data

    Returns:
        DataFrame with seasonality metrics
    """
    # Extract time features
    df = df.withColumn("month", F.month("order_date"))
    df = df.withColumn("quarter", F.quarter("order_date"))
    df = df.withColumn("day_of_week", F.dayofweek("order_date"))

    # Calculate average sales by month
    from pyspark.sql.window import Window
    month_window = Window.partitionBy("product_id", "month")

    df = df.withColumn(
        "avg_monthly_sales",
        F.avg("daily_quantity").over(month_window)
    )

    # Calculate seasonal index (sales vs overall average)
    product_window = Window.partitionBy("product_id")

    df = df.withColumn(
        "overall_avg_sales",
        F.avg("daily_quantity").over(product_window)
    )

    df = df.withColumn(
        "seasonal_index",
        F.col("avg_monthly_sales") / F.when(
            F.col("overall_avg_sales") > 0,
            F.col("overall_avg_sales")
        ).otherwise(1)
    )

    return df


def enrich_with_product_attributes(
    df: DataFrame,
    product_dim: DataFrame,
    attributes: List[str]
) -> DataFrame:
    """
    Enrich product data with dimension attributes.

    Args:
        df: Product metrics DataFrame
        product_dim: Product dimension table
        attributes: List of attributes to join

    Returns:
        Enriched DataFrame
    """
    # Select only needed columns from dimension
    dim_cols = ["product_id"] + attributes
    product_dim_filtered = product_dim.select(*dim_cols)

    # Join with product metrics
    result = df.join(
        product_dim_filtered,
        on="product_id",
        how="left"
    )

    return result
