"""
SQL-based transformation strategy.

Executes SQL queries to transform data using Spark SQL engine.
"""

from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession

from transformation.base import AbstractTransformationStrategy, TransformationType


class SQLTransformationStrategy(AbstractTransformationStrategy):
    """
    Transformation strategy that executes SQL queries.

    The SQL can reference:
    - Registered temp views
    - Catalog tables (catalog.schema.table)
    - The input DataFrame (if provided, registered as 'input_table')

    Example SQL:
        SELECT customer_id, upper(name) as name, sum(amount) as total_amount
        FROM input_table
        WHERE status = 'active'
        GROUP BY customer_id, name
    """

    def __init__(self, spark: SparkSession, sql: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize SQL transformation strategy.

        Args:
            spark: Active SparkSession
            sql: SQL query to execute
            config: Optional configuration dictionary
        """
        self.sql = sql
        super().__init__(spark, config)

    def _validate_config(self) -> None:
        """Validate SQL transformation configuration."""
        if not self.sql or not isinstance(self.sql, str):
            raise ValueError("SQL query must be a non-empty string")

        if self.sql.strip() == "":
            raise ValueError("SQL query cannot be empty or whitespace only")

    def transform(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute SQL transformation.

        Args:
            input_df: Optional input DataFrame (registered as 'input_table')
            **kwargs: Additional arguments:
                - temp_view_name: Custom name for input temp view (default: 'input_table')

        Returns:
            Transformed DataFrame

        Raises:
            ValueError: If SQL execution fails
        """
        temp_view_name = kwargs.get('temp_view_name', 'input_table')

        try:
            # Register input DataFrame as temp view if provided
            if input_df is not None:
                input_df.createOrReplaceTempView(temp_view_name)

            # Execute SQL query
            result_df = self.spark.sql(self.sql)

            # Validate result is not empty schema
            if len(result_df.columns) == 0:
                raise ValueError("SQL transformation returned DataFrame with no columns")

            return result_df

        except Exception as e:
            raise ValueError(f"SQL transformation failed: {str(e)}") from e

    @property
    def transformation_type(self) -> TransformationType:
        """Return SQL transformation type."""
        return TransformationType.SQL

    def get_metadata(self) -> Dict[str, Any]:
        """Return metadata including SQL query."""
        metadata = super().get_metadata()
        metadata['sql'] = self.sql
        metadata['sql_length'] = len(self.sql)
        return metadata
