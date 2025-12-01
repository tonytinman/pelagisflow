"""
Base classes for aggregation transformations.

Provides reusable patterns for common aggregation scenarios.
"""

from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformations.python.base import AbstractTransformation


class AggregationTransformationBase(AbstractTransformation):
    """
    Base class for aggregation transformations.

    Provides common patterns for reading source data, grouping,
    and aggregating with configurable group-by keys and metrics.

    Subclasses can override specific methods to customize behavior
    while inheriting the overall structure.
    """

    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute aggregation transformation.

        Args:
            input_df: Optional input DataFrame (uses get_source_data if None)
            **kwargs: Configuration parameters

        Returns:
            Aggregated DataFrame
        """
        # Stage 1: Get source data
        if input_df is None:
            source_df = self.get_source_data(**kwargs)
        else:
            source_df = input_df

        # Stage 2: Filter data
        filtered_df = self.filter_data(source_df, **kwargs)

        # Stage 3: Apply pre-aggregation transformations
        transformed_df = self.pre_aggregation_transform(filtered_df, **kwargs)

        # Stage 4: Perform aggregation
        aggregated_df = self.aggregate(transformed_df, **kwargs)

        # Stage 5: Apply post-aggregation transformations
        final_df = self.post_aggregation_transform(aggregated_df, **kwargs)

        return final_df

    def get_source_data(self, **kwargs) -> DataFrame:
        """
        Get source data for aggregation.

        Default implementation reads from a table. Override to customize.

        Args:
            **kwargs: Configuration (should include 'source_table')

        Returns:
            Source DataFrame
        """
        source_table = kwargs.get('source_table')
        if not source_table:
            raise ValueError("source_table configuration required")

        return self.spark.table(source_table)

    def filter_data(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Filter source data before aggregation.

        Default implementation applies no filtering. Override to customize.

        Args:
            df: Source DataFrame
            **kwargs: Configuration

        Returns:
            Filtered DataFrame
        """
        return df

    def pre_aggregation_transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Apply transformations before aggregation.

        Default implementation applies no transformations. Override to customize.

        Args:
            df: Filtered DataFrame
            **kwargs: Configuration

        Returns:
            Transformed DataFrame
        """
        return df

    def aggregate(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Perform aggregation.

        Must be implemented by subclasses.

        Args:
            df: DataFrame to aggregate
            **kwargs: Configuration (should include 'group_by_columns')

        Returns:
            Aggregated DataFrame
        """
        group_by_columns = kwargs.get('group_by_columns', [])
        if not group_by_columns:
            raise ValueError("group_by_columns configuration required")

        # Get aggregation expressions
        agg_exprs = self.get_aggregation_expressions(**kwargs)

        return df.groupBy(*group_by_columns).agg(*agg_exprs)

    def get_aggregation_expressions(self, **kwargs) -> List:
        """
        Get list of aggregation expressions.

        Must be implemented by subclasses.

        Args:
            **kwargs: Configuration

        Returns:
            List of aggregation expressions
        """
        raise NotImplementedError("Subclasses must implement get_aggregation_expressions()")

    def post_aggregation_transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Apply transformations after aggregation.

        Default implementation applies no transformations. Override to customize.

        Args:
            df: Aggregated DataFrame
            **kwargs: Configuration

        Returns:
            Transformed DataFrame
        """
        return df


class TimeSeriesAggregationBase(AggregationTransformationBase):
    """
    Base class for time-series aggregations.

    Extends AggregationTransformationBase with time-based filtering
    and grouping capabilities.
    """

    def filter_data(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Filter data based on time window.

        Args:
            df: Source DataFrame
            **kwargs: Configuration
                - date_column: Column name for date filtering
                - lookback_days: Number of days to look back (default: 90)

        Returns:
            Filtered DataFrame
        """
        date_column = kwargs.get('date_column', 'created_date')
        lookback_days = int(kwargs.get('lookback_days', 90))

        return df.filter(
            F.col(date_column) >= F.date_sub(F.current_date(), lookback_days)
        )

    def pre_aggregation_transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """
        Add time-based grouping columns.

        Args:
            df: Filtered DataFrame
            **kwargs: Configuration
                - date_column: Column name for date
                - time_granularity: 'daily', 'weekly', 'monthly' (default: 'daily')

        Returns:
            DataFrame with time grouping columns
        """
        date_column = kwargs.get('date_column', 'created_date')
        granularity = kwargs.get('time_granularity', 'daily')

        if granularity == 'daily':
            return df.withColumn("time_key", F.to_date(F.col(date_column)))

        elif granularity == 'weekly':
            return df.withColumn(
                "time_key",
                F.date_trunc('week', F.col(date_column))
            )

        elif granularity == 'monthly':
            return df.withColumn(
                "time_key",
                F.date_trunc('month', F.col(date_column))
            )

        else:
            raise ValueError(f"Unknown time_granularity: {granularity}")
