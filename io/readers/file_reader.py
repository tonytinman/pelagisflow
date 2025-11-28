from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Dict, Any, Tuple, Optional
from datetime import datetime
import logging

from nova_framework.io.readers.base import AbstractReader
from nova_framework.observability.context import PipelineContext
from nova_framework.observability.pipeline_stats import PipelineStats

logger = logging.getLogger(__name__)


class FileReader(AbstractReader):
    """
    Reads data from files with schema validation and bad row isolation.
    
    Supports: parquet, csv, json, delta, avro, orc
    """
    
    SUPPORTED_FORMATS = ["parquet", "csv", "json", "delta", "avro", "orc"]
    
    def __init__(self, context: PipelineContext, pipeline_stats: PipelineStats):
        super().__init__(context, pipeline_stats)
        self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise RuntimeError("No active Spark session found")
    
    def read(
        self,
        file_path: Optional[str] = None,
        file_format: Optional[str] = None,
        fail_on_schema_change: bool = True,
        fail_on_bad_rows: bool = False,
        invalid_row_threshold: float = 0.1,
        verbose: bool = True
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Read file with schema validation and bad row handling.
        
        Args:
            file_path: Path to file (default: from context)
            file_format: File format (default: from contract)
            fail_on_schema_change: Raise error on schema breaking changes
            fail_on_bad_rows: Raise error if bad row threshold exceeded
            invalid_row_threshold: Max allowed bad row rate (0.0 to 1.0)
            verbose: Print progress messages
            
        Returns:
            Tuple of (valid_dataframe, read_report)
        """
        start_time = datetime.now()
        
        # Initialize
        file_path = file_path or self.context.data_file_path
        file_format = file_format or self.contract.source_format
        expected_schema = self.contract.spark_schema
        
        if verbose:
            logger.info("="*80)
            logger.info("FILE READER - STARTING")
            logger.info(f"File: {file_path}")
            logger.info(f"Format: {file_format}")
            logger.info(f"Expected columns: {len(expected_schema.fields)}")
        
        # Validate schema
        schema_validation = self._validate_schema(file_path, expected_schema, file_format)
        
        if schema_validation["has_breaking_changes"] and fail_on_schema_change:
            raise ValueError(
                f"Schema breaking changes detected: "
                f"{schema_validation['missing_columns']}"
            )
        
        # Read file
        df = self._read_file(file_path, expected_schema, file_format)
        total_rows = df.count()
        
        # Separate valid/invalid rows
        valid_df, invalid_df, stats = self._separate_valid_invalid(df, expected_schema)
        
        # Log statistics
        self._log_read_stats(total_rows, stats['valid_rows'], stats['invalid_rows'])
        
        # Write invalid rows to quarantine
        quarantine_path = None
        if invalid_df.count() > 0:
            quarantine_path = self._write_quarantine(invalid_df, file_path)
            if verbose:
                logger.info(f"Quarantined {stats['invalid_rows']} rows to: {quarantine_path}")
        
        # Check threshold
        rejection_rate = stats['invalid_rows'] / total_rows if total_rows > 0 else 0
        
        if rejection_rate > invalid_row_threshold and fail_on_bad_rows:
            raise ValueError(
                f"Invalid row threshold exceeded: "
                f"{rejection_rate*100:.2f}% (threshold: {invalid_row_threshold*100}%)"
            )
        
        # Build report
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        read_report = {
            "timestamp": start_time.isoformat(),
            "processing_time_seconds": processing_time,
            "file_path": file_path,
            "file_format": file_format,
            "schema_validation": schema_validation,
            "total_rows": total_rows,
            "valid_rows": stats['valid_rows'],
            "invalid_rows": stats['invalid_rows'],
            "rejection_rate_pct": rejection_rate * 100,
            "quarantine_path": quarantine_path,
            "success": True
        }
        
        if verbose:
            logger.info(f"Valid rows: {stats['valid_rows']:,} ({(1-rejection_rate)*100:.1f}%)")
            logger.info(f"Processing time: {processing_time:.2f}s")
            logger.info("FILE READER - COMPLETE")
        
        return valid_df, read_report
    
    def _validate_schema(self, file_path: str, expected_schema, file_format: str) -> Dict:
        """Validate file schema matches expected schema."""
        # Implementation similar to existing DataReader._validate_schema
        # ...
        pass
    
    def _read_file(self, file_path: str, schema, file_format: str) -> DataFrame:
        """Read file with schema applied."""
        reader = self.spark.read.format(file_format).schema(schema)
        
        # Apply format-specific options
        if file_format == "csv":
            csv_options = self.contract.csv_options
            for key, value in csv_options.items():
                reader = reader.option(key, value)
            reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")
        elif file_format == "json":
            json_options = self.contract.source_file_json_options
            for key, value in json_options.items():
                reader = reader.option(key, value)
            reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")
        
        return reader.load(file_path)
    
    def _separate_valid_invalid(self, df: DataFrame, expected_schema) -> Tuple:
        """Separate valid and invalid rows."""
        # Implementation similar to existing DataReader._separate_good_bad_rows
        # ...
        pass
    
    def _write_quarantine(self, invalid_df: DataFrame, source_path: str) -> str:
        """Write invalid rows to quarantine location."""
        quarantine_path = self.context.quarantine_path
        
        invalid_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(quarantine_path)
        
        return quarantine_path