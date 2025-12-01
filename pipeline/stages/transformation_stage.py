"""
Transformation Stage - Executes transformations using pluggable strategies.

This stage supports multiple transformation types:
- SQL transformations (legacy and new)
- Custom Python transformations
- Scala transformations

Used in TransformationPipeline for silver/gold layer processing.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Optional
from nova_framework.pipeline.stages.base import AbstractStage
from transformation.loader import TransformationLoader
from transformation.strategy import TransformationStrategy


class TransformationStage(AbstractStage):
    """
    Stage for executing transformations using pluggable strategies.

    Supports three transformation types:
    1. SQL transformations (inline or registry-based)
    2. Custom Python transformations (from transformations/python/)
    3. Scala transformations (precompiled JARs)

    Configuration via contract customProperties:

    # SQL (inline) - backward compatible:
    customProperties:
      transformationSql: |
        SELECT customer_id, COUNT(*) as order_count
        FROM bronze_sales.orders
        GROUP BY customer_id

    # Python (direct):
    customProperties:
      transformationType: python
      transformationModule: aggregations/customer_rollup
      transformationFunction: transform  # optional, defaults to 'transform'

    # Scala (direct):
    customProperties:
      transformationType: scala
      transformationClass: com.pelagisflow.transformations.CustomerAggregation
      transformationJar: /path/to/transformations.jar  # optional if on classpath

    # Registry-based (any type):
    customProperties:
      transformationName: customer_aggregation_v1

    Args:
        context: Execution context
        stats: Statistics tracker
    """

    def __init__(self, context, stats):
        super().__init__(context, stats, "Transformation")
        self.spark = SparkSession.getActiveSession()

        if not self.spark:
            raise RuntimeError("No active Spark session found")

        self.loader = TransformationLoader(self.spark)
    
    def execute(self, df: Optional[DataFrame]) -> DataFrame:
        """
        Execute transformation using the appropriate strategy.

        Loads and executes transformation based on contract configuration.
        Supports SQL, Python, and Scala transformations.

        Args:
            df: Optional input DataFrame (may be used by some transformations)

        Returns:
            Transformed DataFrame

        Raises:
            ValueError: If transformation configuration is invalid or execution fails
        """
        # Load transformation strategy from contract
        try:
            strategy = self._load_transformation_strategy()
        except Exception as e:
            raise ValueError(
                f"Failed to load transformation strategy: {str(e)}\n"
                f"Contract: {self.context.data_contract_name}"
            ) from e

        # Log transformation metadata
        metadata = strategy.get_metadata()
        self.logger.info(f"Executing {metadata['type']} transformation")
        self.logger.debug(f"Transformation metadata: {metadata}")

        # Execute transformation
        try:
            df_transformed = strategy.transform(input_df=df)

            # Log statistics
            row_count = df_transformed.count()
            self.stats.log_stat("rows_transformed", row_count)
            self.stats.log_stat("transformation_type", metadata['type'])

            column_count = len(df_transformed.columns)
            self.logger.info(
                f"Transformation completed: {row_count} rows, {column_count} columns"
            )
            self.logger.debug(f"Output columns: {df_transformed.columns}")

            return df_transformed

        except Exception as e:
            self.logger.error(f"Transformation execution failed: {str(e)}")
            self.logger.error(f"Transformation metadata: {metadata}")
            raise

    def _load_transformation_strategy(self) -> TransformationStrategy:
        """
        Load transformation strategy based on contract configuration.

        Supports backward compatibility with transformationSql.

        Returns:
            Loaded transformation strategy

        Raises:
            ValueError: If transformation configuration is invalid
        """
        custom_props = self.context.contract.raw_contract.get('customProperties', {})

        # Backward compatibility: Check for legacy transformationSql
        if 'transformationSql' in custom_props and 'transformationName' not in custom_props:
            sql = custom_props['transformationSql'].strip()
            self.logger.info("Loading SQL transformation (legacy mode)")
            return self.loader.load_sql(sql)

        # New mode: Use loader to load from contract
        try:
            return self.loader.load_from_contract(
                self.context.contract.raw_contract
            )
        except Exception as e:
            # Provide helpful error message
            raise ValueError(
                f"Invalid transformation configuration in contract. "
                f"Expected one of:\n"
                f"  - transformationSql: <SQL query>\n"
                f"  - transformationName: <registry name>\n"
                f"  - transformationType + transformationModule (Python)\n"
                f"  - transformationType + transformationClass (Scala)\n"
                f"Error: {str(e)}"
            ) from e
    
    def validate(self) -> bool:
        """
        Validate that transformation configuration is valid.

        Checks that required transformation configuration is present
        and can be loaded successfully.

        Returns:
            True if valid, False otherwise
        """
        try:
            # Try to load the transformation strategy
            strategy = self._load_transformation_strategy()

            # Get metadata to confirm it loaded correctly
            metadata = strategy.get_metadata()

            self.logger.info(
                f"Validation passed: {metadata['type']} transformation configured"
            )
            return True

        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return False