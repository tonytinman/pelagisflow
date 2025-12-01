"""
Transformation Stage - Executes SQL transformations.

This stage reads SQL from the contract and executes it to transform data.
Used in TransformationPipeline for silver/gold layer processing.
"""

from pyspark.sql import DataFrame, SparkSession
from typing import Optional
from nova_framework.pipeline.stages.base import AbstractStage


class TransformationStage(AbstractStage):
    """
    Stage for executing SQL transformations.
    
    Reads SQL query from contract's customProperties.transformationSql
    and executes it to produce transformed data.
    
    Used in transformation pipelines (bronze → silver, silver → gold).
    
    Args:
        context: Execution context
        stats: Statistics tracker
        
    Example:
        stage = TransformationStage(context, stats)
        df_transformed = stage.execute(None)
        
        # Contract should have:
        # customProperties:
        #   transformationSql: |
        #     SELECT customer_id, COUNT(*) as order_count
        #     FROM bronze_sales.orders
        #     GROUP BY customer_id
    """
    
    def __init__(self, context, stats):
        super().__init__(context, stats, "Transformation")
        self.spark = SparkSession.getActiveSession()
        
        if not self.spark:
            raise RuntimeError("No active Spark session found")
    
    def execute(self, df: Optional[DataFrame]) -> DataFrame:
        """
        Execute transformation SQL.
        
        Note: Input df is ignored (transformation reads from tables via SQL)
        
        Returns:
            Transformed DataFrame
            
        Raises:
            ValueError: If transformationSql is not defined in contract
            
        Example:
            # Contract specifies:
            # customProperties:
            #   transformationSql: |
            #     SELECT 
            #       c.customer_id,
            #       c.customer_name,
            #       COUNT(o.order_id) as total_orders,
            #       SUM(o.order_total) as total_revenue
            #     FROM bronze_sales.customers c
            #     LEFT JOIN bronze_sales.orders o 
            #       ON c.customer_id = o.customer_id
            #     GROUP BY c.customer_id, c.customer_name
            
            df_transformed = stage.execute(None)
            # Returns DataFrame with aggregated customer data
        """
        # Get transformation SQL from contract
        transformation_sql = self.context.contract.get(
            "customProperties.transformationSql"
        )
        
        if not transformation_sql:
            raise ValueError(
                f"Transformation pipeline requires 'customProperties.transformationSql' "
                f"in contract '{self.context.data_contract_name}'. "
                f"Please add SQL query to contract."
            )
        
        # Clean up SQL (remove extra whitespace)
        transformation_sql = transformation_sql.strip()
        
        self.logger.info("Executing transformation SQL")
        self.logger.debug(f"SQL query:\n{transformation_sql}")
        
        try:
            # Execute SQL
            df_transformed = self.spark.sql(transformation_sql)
            
            # Log row count
            row_count = df_transformed.count()
            self.stats.log_stat("rows_transformed", row_count)
            
            self.logger.info(f"Transformation completed: {row_count} rows produced")
            
            # Log column info
            column_count = len(df_transformed.columns)
            self.logger.info(f"Output schema: {column_count} columns")
            self.logger.debug(f"Columns: {df_transformed.columns}")
            
            return df_transformed
            
        except Exception as e:
            self.logger.error(f"Transformation SQL failed: {str(e)}")
            self.logger.error(f"SQL query was:\n{transformation_sql}")
            raise
    
    def validate(self) -> bool:
        """
        Validate that transformation SQL is defined.
        
        Called before execution to check configuration.
        
        Returns:
            True if valid, False otherwise
        """
        transformation_sql = self.context.contract.get(
            "customProperties.transformationSql"
        )
        
        if not transformation_sql:
            self.logger.error(
                "Validation failed: transformationSql not defined in contract"
            )
            return False
        
        self.logger.info("Validation passed: transformationSql is defined")
        return True