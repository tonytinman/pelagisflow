"""
Nova Framework I/O Readers

This module provides reader strategies for data ingestion.

Available Readers:
- FileReader: Read from files (parquet, csv, json, delta, avro, orc)
- TableReader: Read from existing Delta tables
- StreamReader: (Future) Read from streaming sources

Usage:
    from nova_framework.io.readers import FileReader, TableReader
    from nova_framework.observability.context import PipelineContext
    from nova_framework.observability.pipeline_stats import PipelineStats
    
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
    from nova_framework.io.factory import IOFactory
    reader = IOFactory.create_reader("file", context, stats)
"""

from nova_framework.io.readers.base import AbstractReader
from nova_framework.io.readers.file_reader import FileReader
from nova_framework.io.readers.table_reader import TableReader

__all__ = [
    "AbstractReader",
    "FileReader",
    "TableReader",
]