"""
Nova Framework I/O Module

This module provides reader and writer strategies for data ingestion and output.
Uses the Strategy pattern to support multiple read/write approaches.

Main Components:
- readers: Data reading strategies (file, table, stream)
- writers: Data writing strategies (overwrite, append, scd2, scd4, file_export)
- factory: IOFactory for creating appropriate reader/writer instances

Usage:
    from pelagisflow.io.factory import IOFactory
    
    # Create reader
    reader = IOFactory.create_reader("file", context, stats)
    df, report = reader.read()
    
    # Create writer from contract configuration
    writer = IOFactory.create_writer_from_contract(context, stats)
    write_stats = writer.write(df)
    
    # Or create specific writer
    writer = IOFactory.create_writer("scd2", context, stats)
    write_stats = writer.write(df, soft_delete=True)
"""

# Main factory
from pelagisflow.io.factory import IOFactory

# Reader base and implementations
from pelagisflow.io.readers.base import AbstractReader
from pelagisflow.io.readers.file_reader import FileReader
from pelagisflow.io.readers.table_reader import TableReader

# Writer base and implementations
from pelagisflow.io.writers.base import AbstractWriter
from pelagisflow.io.writers.overwrite import OverwriteWriter
from pelagisflow.io.writers.append import AppendWriter
from pelagisflow.io.writers.scd2 import SCD2Writer
from pelagisflow.io.writers.scd4 import SCD4Writer
from pelagisflow.io.writers.file_export import FileExportWriter


__all__ = [
    # Factory
    "IOFactory",
    
    # Reader abstractions and implementations
    "AbstractReader",
    "FileReader",
    "TableReader",
    
    # Writer abstractions and implementations
    "AbstractWriter",
    "OverwriteWriter",
    "AppendWriter",
    "SCD2Writer",
    "SCD4Writer",
    "FileExportWriter",
]

__version__ = "1.0.0"