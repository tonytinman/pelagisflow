"""
FRAMEWORK CODE: Abstract base class for Python transformations.

FILE: transformations/python/base.py
DEVELOPERS: Inherit from this class but don't modify this file.

This file provides the base class that all Python transformations inherit from.

Usage (in your code):
    from transformations.python.base import AbstractTransformation

    class MyTransformation(AbstractTransformation):
        def run(self, input_df=None, **kwargs) -> DataFrame:
            # Your code here
            return dataframe
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession


class AbstractTransformation(ABC):
    """
    FRAMEWORK: Base class for Python transformations.

    ═══════════════════════════════════════════════════════════════════════════
    DEVELOPERS: Inherit from this class and implement run()
    ═══════════════════════════════════════════════════════════════════════════

    What you get from this base class:
    - self.spark: SparkSession (provided by framework)
    - run() method: Your entry point (you must implement)
    - validate() method: Optional validation (you can override)

    What framework does with this class:
    1. Framework instantiates your class: MyTransformation(spark)
    2. Framework calls your run() method: instance.run(input_df, **kwargs)
    3. Framework expects a DataFrame back

    Example of YOUR code:
        # File: transformations/python/my_transformation.py
        from transformations.python.base import AbstractTransformation

        class MyTransformation(AbstractTransformation):
            def run(self, input_df=None, **kwargs) -> DataFrame:
                # YOUR CODE: Implement your transformation logic
                df = self.spark.table("bronze.orders")  # ← self.spark from framework
                result = df.filter("amount > 100")
                return result  # ← Framework expects DataFrame
    """

    def __init__(self, spark: SparkSession):
        """
        FRAMEWORK: Constructor called by framework.

        The framework calls this when instantiating your class.
        You don't call this directly.

        Args:
            spark: SparkSession provided by framework
        """
        # FRAMEWORK: Stores SparkSession for your use
        self.spark = spark  # ← You access this in your code as self.spark

    @abstractmethod
    def run(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        DEVELOPER: YOU MUST IMPLEMENT THIS METHOD.

        This is your main entry point. The framework calls this method
        and expects a DataFrame back.

        ═══════════════════════════════════════════════════════════════════════
        EXECUTION FLOW:
        ═══════════════════════════════════════════════════════════════════════
        1. Framework instantiates: instance = YourClass(spark)
        2. Framework calls: result = instance.run(input_df, **kwargs)
        3. Framework uses: result DataFrame in pipeline
        ═══════════════════════════════════════════════════════════════════════

        Args:
            input_df: Optional[DataFrame]
                - Usually None for transformations (you read from tables)
                - Can be used if transformation takes input from previous stage
                - Provided by framework

            **kwargs: Dict[str, Any]
                - YOUR configuration from data contract's transformationConfig
                - Example contract:
                    transformationConfig:
                      source_catalog: bronze_sales  ← kwargs['source_catalog']
                      min_amount: 100               ← kwargs['min_amount']
                - Access with: kwargs.get('key', default_value)

        Returns:
            DataFrame: Your transformed data (REQUIRED)

        Example implementation:
            def run(self, input_df=None, **kwargs):
                # ═══════════════════════════════════════════════════════════
                # YOUR CODE STARTS HERE
                # ═══════════════════════════════════════════════════════════

                # Get YOUR configuration
                catalog = kwargs.get('source_catalog', 'bronze')

                # Use self.spark (provided by framework)
                df = self.spark.table(f"{catalog}.orders")

                # YOUR transformation logic
                result = df.filter("amount > 100")

                # Must return DataFrame
                return result

                # ═══════════════════════════════════════════════════════════
                # YOUR CODE ENDS - Framework takes over
                # ═══════════════════════════════════════════════════════════
        """
        pass

    def validate(self) -> bool:
        """
        DEVELOPER: OPTIONAL - Override this to add validation logic.

        Framework may call this before run() to validate prerequisites.
        Default implementation returns True (no validation).

        Example override in YOUR class:
            def validate(self):
                # YOUR VALIDATION: Check if required tables exist
                try:
                    self.spark.table("bronze.orders")
                    self.spark.table("bronze.customers")
                    return True
                except Exception:
                    return False
        """
        return True  # FRAMEWORK: Default - no validation

    def get_name(self) -> str:
        """
        FRAMEWORK: Returns transformation name (your class name).

        You can override this if you want custom naming.
        """
        return self.__class__.__name__
