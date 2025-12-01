"""
Product metrics calculation functions.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ProductMetricsCalculator:
    """
    Calculator for various product performance metrics.
    """

    @staticmethod
    def calculate_basic_metrics(orders_df: DataFrame) -> DataFrame:
        """
        Calculate basic product metrics from orders.

        Args:
            orders_df: Orders DataFrame

        Returns:
            DataFrame with basic product metrics
        """
        return orders_df.groupBy("product_id").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.sum("item_total").alias("total_revenue"),
            F.avg("unit_price").alias("avg_unit_price"),
            F.min("order_date").alias("first_sale_date"),
            F.max("order_date").alias("last_sale_date")
        )

    @staticmethod
    def calculate_customer_metrics(orders_df: DataFrame) -> DataFrame:
        """
        Calculate customer-related product metrics.

        Args:
            orders_df: Orders DataFrame

        Returns:
            DataFrame with customer metrics per product
        """
        return orders_df.groupBy("product_id").agg(
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("quantity").alias("avg_quantity_per_order")
        )

    @staticmethod
    def calculate_inventory_metrics(
        orders_df: DataFrame,
        inventory_df: DataFrame
    ) -> DataFrame:
        """
        Calculate inventory-related metrics.

        Args:
            orders_df: Orders DataFrame
            inventory_df: Inventory DataFrame

        Returns:
            DataFrame with inventory metrics
        """
        # Calculate sales rate
        sales_by_product = orders_df.groupBy("product_id").agg(
            F.sum("quantity").alias("total_sold")
        )

        # Join with current inventory
        result = inventory_df.join(
            sales_by_product,
            on="product_id",
            how="left"
        ).fillna({"total_sold": 0})

        # Calculate metrics
        result = result.withColumn(
            "inventory_turnover",
            F.when(
                F.col("current_stock") > 0,
                F.col("total_sold") / F.col("current_stock")
            ).otherwise(0)
        )

        result = result.withColumn(
            "stock_coverage_days",
            F.when(
                F.col("total_sold") > 0,
                F.col("current_stock") / (F.col("total_sold") / 365.0)
            ).otherwise(999999)
        )

        return result

    @staticmethod
    def calculate_profitability_metrics(
        product_metrics: DataFrame,
        product_costs: DataFrame
    ) -> DataFrame:
        """
        Calculate profitability metrics.

        Args:
            product_metrics: Product sales metrics
            product_costs: Product cost data

        Returns:
            DataFrame with profitability metrics
        """
        # Join with cost data
        result = product_metrics.join(
            product_costs.select("product_id", "unit_cost"),
            on="product_id",
            how="left"
        )

        # Calculate profit metrics
        result = result.withColumn(
            "total_cost",
            F.col("total_quantity_sold") * F.col("unit_cost")
        )

        result = result.withColumn(
            "total_profit",
            F.col("total_revenue") - F.col("total_cost")
        )

        result = result.withColumn(
            "profit_margin",
            F.when(
                F.col("total_revenue") > 0,
                F.col("total_profit") / F.col("total_revenue")
            ).otherwise(0)
        )

        result = result.withColumn(
            "roi",
            F.when(
                F.col("total_cost") > 0,
                F.col("total_profit") / F.col("total_cost")
            ).otherwise(0)
        )

        return result
