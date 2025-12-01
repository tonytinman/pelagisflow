"""
Customer aggregation transformation.

This transformation demonstrates:
- Reading from multiple source tables
- Using helper functions
- Complex aggregations
- Business logic encapsulation

Example usage in data contract:
    customProperties:
      transformationType: python
      transformationModule: customer_aggregation
      transformationFunction: transform
      transformationConfig:
        source_catalog: bronze_sales
        min_order_threshold: 100
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_customer_metrics(
    customers_df: DataFrame,
    orders_df: DataFrame,
    min_threshold: float = 0.0
) -> DataFrame:
    """
    Calculate aggregated customer metrics.

    Args:
        customers_df: Customer dimension DataFrame
        orders_df: Orders fact DataFrame
        min_threshold: Minimum order amount to include

    Returns:
        DataFrame with customer metrics
    """
    # Filter orders by threshold
    filtered_orders = orders_df.filter(F.col("order_total") >= min_threshold)

    # Calculate order metrics
    order_metrics = filtered_orders.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("order_total").alias("total_revenue"),
        F.avg("order_total").alias("avg_order_value"),
        F.max("order_date").alias("last_order_date"),
        F.min("order_date").alias("first_order_date")
    )

    # Calculate customer lifetime (days between first and last order)
    order_metrics = order_metrics.withColumn(
        "customer_lifetime_days",
        F.datediff(F.col("last_order_date"), F.col("first_order_date"))
    )

    # Join with customer dimension
    result = customers_df.join(
        order_metrics,
        on="customer_id",
        how="left"
    )

    # Fill nulls for customers with no orders
    result = result.fillna({
        "total_orders": 0,
        "total_revenue": 0.0,
        "avg_order_value": 0.0,
        "customer_lifetime_days": 0
    })

    return result


def classify_customer_segment(df: DataFrame) -> DataFrame:
    """
    Classify customers into segments based on revenue and order count.

    Segments:
    - VIP: total_revenue > 10000 OR total_orders > 50
    - High Value: total_revenue > 5000 OR total_orders > 20
    - Medium Value: total_revenue > 1000 OR total_orders > 5
    - Low Value: everything else

    Args:
        df: DataFrame with customer metrics

    Returns:
        DataFrame with customer_segment column added
    """
    return df.withColumn(
        "customer_segment",
        F.when(
            (F.col("total_revenue") > 10000) | (F.col("total_orders") > 50),
            F.lit("VIP")
        ).when(
            (F.col("total_revenue") > 5000) | (F.col("total_orders") > 20),
            F.lit("High Value")
        ).when(
            (F.col("total_revenue") > 1000) | (F.col("total_orders") > 5),
            F.lit("Medium Value")
        ).otherwise(F.lit("Low Value"))
    )


def calculate_rfm_scores(df: DataFrame, current_date: str = None) -> DataFrame:
    """
    Calculate RFM (Recency, Frequency, Monetary) scores.

    Args:
        df: DataFrame with customer metrics
        current_date: Reference date for recency calculation (default: today)

    Returns:
        DataFrame with RFM scores
    """
    if current_date is None:
        current_date = F.current_date()
    else:
        current_date = F.lit(current_date).cast("date")

    # Calculate recency (days since last order)
    df = df.withColumn(
        "recency_days",
        F.when(
            F.col("last_order_date").isNotNull(),
            F.datediff(current_date, F.col("last_order_date"))
        ).otherwise(999999)
    )

    # Calculate RFM score quartiles using window functions
    window_spec = Window.orderBy(F.col("recency_days"))
    df = df.withColumn("recency_quartile", F.ntile(4).over(window_spec))

    window_spec = Window.orderBy(F.col("total_orders").desc())
    df = df.withColumn("frequency_quartile", F.ntile(4).over(window_spec))

    window_spec = Window.orderBy(F.col("total_revenue").desc())
    df = df.withColumn("monetary_quartile", F.ntile(4).over(window_spec))

    # Calculate composite RFM score (higher is better)
    # Invert recency quartile so higher is better
    df = df.withColumn(
        "rfm_score",
        ((5 - F.col("recency_quartile")) * 100) +
        (F.col("frequency_quartile") * 10) +
        F.col("monetary_quartile")
    )

    return df


def transform(
    spark: SparkSession,
    input_df: Optional[DataFrame] = None,
    **kwargs
) -> DataFrame:
    """
    Main transformation function for customer aggregation.

    This transformation:
    1. Reads customers and orders from source tables
    2. Calculates customer metrics (order count, revenue, etc.)
    3. Classifies customers into segments
    4. Calculates RFM scores

    Args:
        spark: Active SparkSession
        input_df: Optional input DataFrame (not used - reads from tables)
        **kwargs: Configuration parameters:
            - source_catalog: Catalog name for source tables (default: bronze_sales)
            - min_order_threshold: Minimum order amount (default: 0)
            - current_date: Reference date for RFM calculation (default: today)

    Returns:
        DataFrame with aggregated customer data
    """
    # Get configuration
    source_catalog = kwargs.get('source_catalog', 'bronze_sales')
    min_threshold = float(kwargs.get('min_order_threshold', 0.0))
    current_date = kwargs.get('current_date', None)

    # Read source tables
    customers_df = spark.table(f"{source_catalog}.customers")
    orders_df = spark.table(f"{source_catalog}.orders")

    # Calculate customer metrics
    result = calculate_customer_metrics(
        customers_df,
        orders_df,
        min_threshold=min_threshold
    )

    # Classify customer segments
    result = classify_customer_segment(result)

    # Calculate RFM scores
    result = calculate_rfm_scores(result, current_date=current_date)

    # Select and order final columns
    result = result.select(
        "customer_id",
        "customer_name",
        "customer_email",
        "customer_segment",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "first_order_date",
        "last_order_date",
        "customer_lifetime_days",
        "recency_days",
        "recency_quartile",
        "frequency_quartile",
        "monetary_quartile",
        "rfm_score"
    )

    return result
