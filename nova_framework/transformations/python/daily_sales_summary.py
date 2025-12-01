"""
Daily sales summary transformation.

Example of using reusable base classes for common patterns.
"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nova_framework.transformations.python.common.aggregation_base import TimeSeriesAggregationBase


class DailySalesSummary(TimeSeriesAggregationBase):
    """
    Daily sales summary aggregation.

    Inherits from TimeSeriesAggregationBase to get:
    - Time-based filtering
    - Time granularity grouping
    - Standard aggregation pattern

    Only needs to implement:
    - get_aggregation_expressions() for specific metrics
    - Optionally override other methods for customization

    Usage in data contract:
        customProperties:
          transformationType: python
          transformationModule: daily_sales_summary
          transformationClass: DailySalesSummary
          transformationConfig:
            source_table: bronze.orders
            date_column: order_date
            lookback_days: 90
            time_granularity: daily
            group_by_columns:
              - time_key
              - product_category
    """

    def get_aggregation_expressions(self, **kwargs) -> List:
        """
        Define aggregation metrics for daily sales.

        Returns:
            List of aggregation expressions
        """
        return [
            F.count("order_id").alias("order_count"),
            F.sum("order_total").alias("total_revenue"),
            F.avg("order_total").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("quantity").alias("total_quantity"),
            F.min("order_total").alias("min_order_value"),
            F.max("order_total").alias("max_order_value")
        ]

    def post_aggregation_transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Add calculated metrics after aggregation.

        Args:
            df: Aggregated DataFrame
            **kwargs: Configuration

        Returns:
            DataFrame with additional calculated columns
        """
        return df.withColumn(
            "revenue_per_customer",
            F.col("total_revenue") / F.col("unique_customers")
        ).withColumn(
            "avg_quantity_per_order",
            F.col("total_quantity") / F.col("order_count")
        )


class WeeklySalesSummary(TimeSeriesAggregationBase):
    """
    Weekly sales summary with additional trend calculations.

    Example of extending the base class with custom logic.
    """

    def get_aggregation_expressions(self, **kwargs) -> List:
        """Define weekly aggregation metrics."""
        return [
            F.count("order_id").alias("order_count"),
            F.sum("order_total").alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("order_date").alias("active_days")
        ]

    def post_aggregation_transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Calculate week-over-week growth.

        Args:
            df: Aggregated DataFrame
            **kwargs: Configuration

        Returns:
            DataFrame with growth metrics
        """
        from pyspark.sql.window import Window

        # Create window for previous week
        window_spec = Window.partitionBy("product_category").orderBy("time_key")

        # Calculate week-over-week growth
        df = df.withColumn(
            "prev_week_revenue",
            F.lag("total_revenue", 1).over(window_spec)
        )

        df = df.withColumn(
            "wow_growth_pct",
            F.when(
                F.col("prev_week_revenue").isNotNull() & (F.col("prev_week_revenue") > 0),
                ((F.col("total_revenue") - F.col("prev_week_revenue")) / F.col("prev_week_revenue")) * 100
            )
        )

        return df
