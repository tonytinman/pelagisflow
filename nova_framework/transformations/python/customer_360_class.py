"""
Customer 360 transformation - Class-based implementation.

This example demonstrates a class-based transformation with:
- Multiple helper methods
- Clear separation of concerns
- Multi-stage processing
- Reusable logic
"""

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from nova_framework.transformations.python.base import AbstractTransformation


class Customer360Transformation(AbstractTransformation):
    """
    Customer 360 transformation with RFM scoring and segmentation.

    This transformation processes customer and order data to create
    a comprehensive customer view with metrics and segments.

    Usage in data contract:
        customProperties:
          transformationType: python
          transformationModule: customer_360_class
          transformationClass: Customer360Transformation
          transformationConfig:
            source_catalog: bronze_sales
            lookback_days: 90
    """

    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute the multi-stage Customer 360 transformation.

        Args:
            input_df: Not used (transformation reads from tables)
            **kwargs: Configuration parameters
                - source_catalog: Source catalog name (default: bronze)
                - lookback_days: Days to look back for orders (default: 90)
                - min_order_amount: Minimum order amount to include (default: 0)

        Returns:
            DataFrame with customer 360 view
        """
        # Get configuration
        source_catalog = kwargs.get('source_catalog', 'bronze')
        lookback_days = int(kwargs.get('lookback_days', 90))
        min_order_amount = float(kwargs.get('min_order_amount', 0))

        # Stage 1: Load and filter orders
        orders_df = self._load_filtered_orders(source_catalog, lookback_days, min_order_amount)

        # Stage 2: Load customer data
        customers_df = self._load_customers(source_catalog)

        # Stage 3: Calculate customer metrics
        metrics_df = self._calculate_customer_metrics(orders_df)

        # Stage 4: Enrich with customer data
        enriched_df = self._enrich_with_customer_data(customers_df, metrics_df)

        # Stage 5: Calculate RFM scores
        rfm_df = self._calculate_rfm_scores(enriched_df)

        # Stage 6: Classify customer segments
        final_df = self._classify_segments(rfm_df)

        return final_df

    def _load_filtered_orders(
        self,
        catalog: str,
        lookback_days: int,
        min_amount: float
    ) -> DataFrame:
        """
        Load and filter orders based on criteria.

        Args:
            catalog: Source catalog name
            lookback_days: Days to look back
            min_amount: Minimum order amount

        Returns:
            Filtered orders DataFrame
        """
        return self.spark.sql(f"""
            SELECT *
            FROM {catalog}.orders
            WHERE status IN ('completed', 'shipped')
              AND order_date >= current_date() - INTERVAL {lookback_days} DAYS
              AND order_total >= {min_amount}
        """)

    def _load_customers(self, catalog: str) -> DataFrame:
        """
        Load customer dimension data.

        Args:
            catalog: Source catalog name

        Returns:
            Customers DataFrame
        """
        return self.spark.table(f"{catalog}.customers")

    def _calculate_customer_metrics(self, orders_df: DataFrame) -> DataFrame:
        """
        Calculate aggregated customer metrics from orders.

        Args:
            orders_df: Orders DataFrame

        Returns:
            DataFrame with customer metrics
        """
        return orders_df.groupBy("customer_id").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_total").alias("total_revenue"),
            F.avg("order_total").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("product_category").alias("categories_purchased")
        )

    def _enrich_with_customer_data(
        self,
        customers_df: DataFrame,
        metrics_df: DataFrame
    ) -> DataFrame:
        """
        Join customer data with calculated metrics.

        Args:
            customers_df: Customer dimension
            metrics_df: Customer metrics

        Returns:
            Enriched DataFrame
        """
        enriched = customers_df.join(metrics_df, "customer_id", "left")

        # Fill nulls for customers with no orders
        return enriched.fillna({
            "total_orders": 0,
            "total_revenue": 0.0,
            "avg_order_value": 0.0,
            "categories_purchased": 0
        })

    def _calculate_rfm_scores(self, df: DataFrame) -> DataFrame:
        """
        Calculate RFM (Recency, Frequency, Monetary) scores.

        Args:
            df: DataFrame with customer metrics

        Returns:
            DataFrame with RFM scores
        """
        # Calculate recency (days since last order)
        df = df.withColumn(
            "recency_days",
            F.when(
                F.col("last_order_date").isNotNull(),
                F.datediff(F.current_date(), F.col("last_order_date"))
            ).otherwise(999999)
        )

        # Calculate RFM quartiles using window functions
        # Recency: Lower is better (recent orders)
        window_spec = Window.orderBy(F.col("recency_days"))
        df = df.withColumn("recency_score", 5 - F.ntile(4).over(window_spec))

        # Frequency: Higher is better (more orders)
        window_spec = Window.orderBy(F.col("total_orders").desc())
        df = df.withColumn("frequency_score", F.ntile(4).over(window_spec))

        # Monetary: Higher is better (more revenue)
        window_spec = Window.orderBy(F.col("total_revenue").desc())
        df = df.withColumn("monetary_score", F.ntile(4).over(window_spec))

        # Calculate composite RFM score
        df = df.withColumn(
            "rfm_score",
            (F.col("recency_score") * 100) +
            (F.col("frequency_score") * 10) +
            F.col("monetary_score")
        )

        return df

    def _classify_segments(self, df: DataFrame) -> DataFrame:
        """
        Classify customers into segments based on RFM scores.

        Args:
            df: DataFrame with RFM scores

        Returns:
            DataFrame with customer segments
        """
        return df.withColumn(
            "customer_segment",
            F.when(F.col("rfm_score") >= 444, F.lit("Champions"))
             .when(F.col("rfm_score") >= 400, F.lit("Loyal Customers"))
             .when(F.col("rfm_score") >= 300, F.lit("Potential Loyalists"))
             .when(F.col("rfm_score") >= 200, F.lit("At Risk"))
             .when(F.col("rfm_score") >= 100, F.lit("Hibernating"))
             .otherwise(F.lit("Lost"))
        ).withColumn(
            "customer_value",
            F.when(F.col("total_revenue") > 10000, F.lit("High"))
             .when(F.col("total_revenue") > 1000, F.lit("Medium"))
             .otherwise(F.lit("Low"))
        )
