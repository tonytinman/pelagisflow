"""
Nova Framework I/O Writers

This module provides writer strategies for data output.

Available Writers:
- OverwriteWriter: Full table refresh (overwrites all data)
- AppendWriter: Append-only writes (fact tables, event logs)
- T2CLWriter: Type 2 Change Log with effective dating and history
- SCD4Writer: SCD Type 4 (current + historical table pattern)
- FileExportWriter: Export to files (parquet, csv, json)

Strategy Selection:
Writers can be selected via contract configuration or explicitly:

    # Contract-driven (recommended)
    customProperties:
      writeStrategy: type_2_change_log
      softDelete: true

    # Then in code:
    writer = IOFactory.create_writer_from_contract(context, stats)

    # Or explicit selection:
    writer = IOFactory.create_writer("type_2_change_log", context, stats)

Usage Examples:

    # Overwrite (full refresh)
    writer = OverwriteWriter(context, stats)
    stats = writer.write(df, optimize=True)

    # Append (add new records)
    writer = AppendWriter(context, stats)
    stats = writer.write(df, deduplicate=True, dedup_cols=["id"])

    # Type 2 Change Log (track history)
    writer = T2CLWriter(context, stats)
    stats = writer.write(
        df,
        soft_delete=True,
        process_date="2024-11-28"
    )

    # SCD Type 4 (current + history)
    writer = SCD4Writer(context, stats)
    stats = writer.write(df)

    # File export
    writer = FileExportWriter(context, stats)
    stats = writer.write(
        df,
        output_path="/mnt/exports/data.parquet",
        file_format="parquet"
    )

Write Strategy Comparison:

| Strategy         | Use Case                          | History | Performance |
|------------------|-----------------------------------|---------|-------------|
| Overwrite        | Reference data, small dimensions  | No      | Fast        |
| Append           | Fact tables, event logs           | Implicit| Fast        |
| type_2_change_log| Dimensions with history tracking  | Yes     | Medium      |
| SCD4             | Large dimensions (current + hist) | Yes     | Medium      |
| FileExport       | External system integration       | N/A     | Varies      |

For more details, see:
- T2CL: Type 2 Change Log (full history with effective dates)
- SCD4: Current table + Historical table pattern (fast queries + history)
"""

from nova_framework.io.writers.base import AbstractWriter
from nova_framework.io.writers.overwrite import OverwriteWriter
from nova_framework.io.writers.append import AppendWriter
from nova_framework.io.writers.t2cl import T2CLWriter
from nova_framework.io.writers.scd4 import SCD4Writer
from nova_framework.io.writers.file_export import FileExportWriter

__all__ = [
    "AbstractWriter",
    "OverwriteWriter",
    "AppendWriter",
    "T2CLWriter",
    "SCD4Writer",
    "FileExportWriter",
]