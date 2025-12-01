"""
Abstract base class for Python transformations.

Provides a structured pattern for building transformations with:
- Clear interface via run() method
- Access to SparkSession
- Support for helper methods and classes
- Reusable base classes via inheritance
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession


class AbstractTransformation(ABC):
    """
    Abstract base class for all Python transformations.

    Developers inherit from this class and implement the run() method
    to define their transformation logic.

    Example:
        class CustomerAggregation(AbstractTransformation):
            def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
                # Stage 1: Read data
                orders = self.spark.table("bronze.orders")

                # Stage 2: Apply transformations
                result = self._aggregate_by_customer(orders)

                return result

            def _aggregate_by_customer(self, df: DataFrame) -> DataFrame:
                '''Helper method for aggregation logic.'''
                return df.groupBy("customer_id").agg(...)
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the transformation with SparkSession.

        Args:
            spark: Active SparkSession instance
        """
        self.spark = spark

    @abstractmethod
    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute the transformation and return a DataFrame.

        This is the main entry point that must be implemented by all
        transformation classes.

        Args:
            input_df: Optional input DataFrame (for transformations that accept input)
            **kwargs: Configuration parameters from data contract
                - Can include catalog names, thresholds, dates, etc.
                - Passed from contract's transformationConfig

        Returns:
            Transformed DataFrame

        Raises:
            ValueError: If transformation requirements are not met
            RuntimeError: If transformation execution fails

        Example:
            def run(self, input_df=None, **kwargs):
                source_catalog = kwargs.get('source_catalog', 'bronze')
                min_amount = kwargs.get('min_amount', 0)

                df = self.spark.table(f"{source_catalog}.orders")
                df = df.filter(f"amount >= {min_amount}")

                return df
        """
        pass

    def validate(self) -> bool:
        """
        Optional validation method called before run().

        Override this to add validation logic for your transformation.

        Returns:
            True if validation passes, False otherwise

        Example:
            def validate(self):
                # Check if required tables exist
                try:
                    self.spark.table("bronze.orders")
                    return True
                except:
                    return False
        """
        return True

    def get_name(self) -> str:
        """
        Get the name of this transformation.

        Returns:
            Class name by default, can be overridden
        """
        return self.__class__.__name__
