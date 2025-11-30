"""
Python-based transformations for PelagisFlow.

All transformation modules must implement a transform function with signature:
    def transform(spark: SparkSession, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame

Transformations can include:
- Helper functions
- Helper classes
- Multiple files organized as a package
"""
