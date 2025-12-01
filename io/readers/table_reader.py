from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, Tuple, Optional

from pelagisflow.io.readers.base import AbstractReader


class TableReader(AbstractReader):
    """
    Reads data from existing Delta tables.
    
    Used for transformation pipelines that read from bronze/silver tables.
    """
    
    def read(
        self,
        table_name: Optional[str] = None,
        filter_condition: Optional[str] = None,
        columns: Optional[list] = None,
        verbose: bool = True
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Read from Delta table.
        
        Args:
            table_name: Fully qualified table name (catalog.schema.table)
            filter_condition: SQL WHERE clause to filter data
            columns: List of columns to select (None = all)
            verbose: Print progress messages
            
        Returns:
            Tuple of (dataframe, read_report)
        """
        self.spark = SparkSession.getActiveSession()
        
        # Build table name
        if table_name is None:
            table_name = f"{self.catalog}.{self.contract.schema_name}.{self.contract.table_name}"
        
        # Read table
        df = self.spark.table(table_name)
        
        # Apply filter
        if filter_condition:
            df = df.filter(filter_condition)
        
        # Select columns
        if columns:
            df = df.select(*columns)
        
        # Count rows
        row_count = df.count()
        self.pipeline_stats.log_stat("rows_read", row_count)
        
        # Build report
        read_report = {
            "table_name": table_name,
            "filter_condition": filter_condition,
            "columns_selected": columns or "all",
            "rows_read": row_count,
            "success": True
        }
        
        if verbose:
            logger.info(f"Read {row_count:,} rows from {table_name}")
        
        return df, read_report