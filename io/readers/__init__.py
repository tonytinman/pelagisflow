"""
Nova Framework I/O Readers

This module provides reader strategies for data ingestion.

Available Readers:
- FileReader: Read from files (parquet, csv, json, delta, avro, orc)
- TableReader: Read from existing Delta tables
- StreamReader: (Future) Read from streaming sources

Usage:
    from pelagisflow.io.readers import FileReader, TableReader
    from pelagisflow.core.context import ExecutionContext
    from pelagisflow.observability.stats import PipelineStats
    
    # File reader
    reader = FileReader(context, stats)
    df, report = reader.read(
        file_path="/path/to/data.parquet",
        file_format="parquet"
    )
    
    # Table reader
    reader = TableReader(context, stats)
    df, report = reader.read(
        table_name="catalog.schema.table",
        filter_condition="date >= '2024-01-01'"
    )
    
    # Or use factory (recommended)
    from pelagisflow.io.factory import IOFactory
    reader = IOFactory.create_reader("file", context, stats)
"""

from pelagisflow.io.readers.base import AbstractReader
from pelagisflow.io.readers.file_reader import FileReader
from pelagisflow.io.readers.table_reader import TableReader

__all__ = [
    "AbstractReader",
    "FileReader",
    "TableReader",
]