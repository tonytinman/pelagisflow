from pyspark.sql import DataFrame
from typing import Dict, Any, Optional

from nova_framework.io.writers.base import AbstractWriter
from nova_framework.observability.logging import get_logger

logger = get_logger(__name__)


class FileExportWriter(AbstractWriter):
    """
    Exports DataFrame to file (parquet, csv, json).
    
    Use case: External system integration, data extract delivery
    """
    
    SUPPORTED_FORMATS = ["parquet", "csv", "json", "delta"]
    
    def write(
        self,
        df: DataFrame,
        output_path: str,
        file_format: str = "parquet",
        mode: str = "overwrite",
        partition_cols: Optional[list] = None,
        **format_options
    ) -> Dict[str, Any]:
        """
        Export DataFrame to file.
        
        Args:
            df: DataFrame to export
            output_path: Output file path
            file_format: Output format (parquet, csv, json, delta)
            mode: Write mode (overwrite, append)
            partition_cols: Columns to partition by
            **format_options: Format-specific options (e.g., header, delimiter for CSV)
            
        Returns:
            Dictionary with export statistics
        """
        if file_format not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format: {file_format}. "
                f"Supported: {self.SUPPORTED_FORMATS}"
            )
        
        row_count = df.count()
        logger.info(f"Exporting {row_count:,} rows to {output_path} ({file_format})")
        
        # Build writer
        writer = df.write.format(file_format).mode(mode)
        
        # Apply format-specific options
        if file_format == "csv":
            writer = writer.option("header", "true")
            for key, value in format_options.items():
                writer = writer.option(key, value)
        
        # Apply partitioning
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        # Write
        writer.save(output_path)
        
        logger.info(f"Export complete: {output_path}")
        
        self._log_write_stats(row_count, f"file_export_{file_format}")
        
        return {
            "strategy": "file_export",
            "output_path": output_path,
            "file_format": file_format,
            "rows_exported": row_count,
            "mode": mode,
            "partitioned": partition_cols is not None
        }