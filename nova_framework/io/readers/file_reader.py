from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import Dict, Any, Tuple, Optional
from datetime import datetime
import logging

from nova_framework.io.readers.base import AbstractReader
from nova_framework.core.context import ExecutionContext
from nova_framework.observability.stats import PipelineStats

logger = logging.getLogger(__name__)


class FileReader(AbstractReader):
    """
    Reads data from files with schema validation and bad row isolation.
    
    Supports: parquet, csv, json, delta, avro, orc
    """
    
    SUPPORTED_FORMATS = ["parquet", "csv", "json", "delta", "avro", "orc"]
    
    def __init__(self, context: ExecutionContext, pipeline_stats: PipelineStats):
        super().__init__(context, pipeline_stats)
        self.spark = SparkSession.getActiveSession()
        
        if self.spark is None:
            raise RuntimeError("No active Spark session found")
    
    def read(
        self,
        file_path: Optional[str] = None,
        file_format: Optional[str] = None,
        fail_on_schema_change: bool = False,  # Changed to False for flexibility
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
        
        # Read file with Parquet fallback handling
        if file_format == "parquet":
            try:
                df = self._read_file(file_path, expected_schema, file_format)
                # Trigger read to catch type mismatch errors (Spark is lazy)
                total_rows = df.count()
            except Exception as e:
                error_msg = str(e)
                # Check for known Parquet type mismatch errors
                parquet_errors = ["PARQUET_COLUMN_DATA_TYPE_MISMATCH", "INT96", "FIXED_LEN_BYTE_ARRAY"]
                if any(err in error_msg for err in parquet_errors):
                    logger.warning(
                        f"Parquet type mismatch detected during count(). "
                        f"Retrying with schema casting. Error: {error_msg[:200]}"
                    )
                    # Retry with casting fallback
                    df = self._read_parquet_with_casting(file_path, expected_schema)
                    total_rows = df.count()
                else:
                    raise
        else:
            df = self._read_file(file_path, expected_schema, file_format)
            total_rows = df.count()
        
        # Separate valid/invalid rows
        valid_df, invalid_df, row_stats = self._separate_valid_invalid(df, expected_schema)
        
        # Log statistics
        self._log_read_stats(total_rows, row_stats['valid_rows'], row_stats['invalid_rows'])
        
        # Write invalid rows to quarantine
        quarantine_path = None
        if row_stats['invalid_rows'] > 0:
            try:
                quarantine_path = self._write_quarantine(invalid_df, file_path)
                if verbose:
                    logger.info(f"Quarantined {row_stats['invalid_rows']} rows to: {quarantine_path}")
            except Exception as e:
                logger.warning(f"Failed to write quarantine: {e}")
        
        # Check threshold
        rejection_rate = row_stats['invalid_rows'] / total_rows if total_rows > 0 else 0
        
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
            "valid_rows": row_stats['valid_rows'],
            "invalid_rows": row_stats['invalid_rows'],
            "rejection_rate_pct": rejection_rate * 100,
            "quarantine_path": quarantine_path,
            "success": True
        }
        
        if verbose:
            logger.info(f"Valid rows: {row_stats['valid_rows']:,} ({(1-rejection_rate)*100:.1f}%)")
            logger.info(f"Invalid rows: {row_stats['invalid_rows']:,}")
            logger.info(f"Processing time: {processing_time:.2f}s")
            logger.info("FILE READER - COMPLETE")
            logger.info("="*80)
        
        return valid_df, read_report
    
    def _validate_schema(self, file_path: str, expected_schema: StructType, file_format: str) -> Dict:
        """
        Validate file schema matches expected schema.
        
        Returns:
            Dict with validation results
        """
        try:
            # Read a sample to get actual schema
            sample_df = self.spark.read.format(file_format).load(file_path).limit(1)
            actual_schema = sample_df.schema
            
            # Get field names
            expected_fields = {f.name for f in expected_schema.fields}
            actual_fields = {f.name for f in actual_schema.fields}
            
            # Find differences
            missing_columns = expected_fields - actual_fields
            extra_columns = actual_fields - expected_fields
            
            # Check for breaking changes (missing required columns)
            has_breaking_changes = len(missing_columns) > 0
            
            return {
                "has_breaking_changes": has_breaking_changes,
                "missing_columns": list(missing_columns),
                "extra_columns": list(extra_columns),
                "expected_column_count": len(expected_fields),
                "actual_column_count": len(actual_fields),
                "schema_match": missing_columns == set() and extra_columns == set()
            }
        
        except Exception as e:
            logger.warning(f"Schema validation failed: {e}")
            # Return safe defaults on error
            return {
                "has_breaking_changes": False,
                "missing_columns": [],
                "extra_columns": [],
                "expected_column_count": len(expected_schema.fields),
                "actual_column_count": 0,
                "schema_match": False,
                "error": str(e)
            }
    
    def _read_file(self, file_path: str, schema: StructType, file_format: str) -> DataFrame:
        """Read file with schema applied."""

        # Parquet needs special handling for type mismatches
        if file_format == "parquet":
            try:
                # Try reading with schema first (fastest path)
                reader = self.spark.read.format(file_format).schema(schema)
                reader = reader.option("mergeSchema", "true")
                reader = reader.option("datetimeRebaseMode", "CORRECTED")
                return reader.load(file_path)
            except Exception as e:
                # If schema enforcement fails (e.g., FIXED_LEN_BYTE_ARRAY, INT96 mismatch),
                # read without schema and cast columns
                error_msg = str(e)
                parquet_errors = ["PARQUET_COLUMN_DATA_TYPE_MISMATCH", "INT96", "FIXED_LEN_BYTE_ARRAY"]
                if any(err in error_msg for err in parquet_errors):
                    logger.warning(
                        f"Parquet type mismatch detected during load(). "
                        f"Reading without schema and casting columns. Error: {error_msg[:200]}"
                    )
                    return self._read_parquet_with_casting(file_path, schema)
                else:
                    # Re-raise if it's a different error
                    raise

        # Standard path for other formats
        reader = self.spark.read.format(file_format).schema(schema)

        # Apply format-specific options
        if file_format == "csv":
            csv_options = self.contract.csv_options or {}
            for key, value in csv_options.items():
                reader = reader.option(key, value)
            reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")
        elif file_format == "json":
            json_options = getattr(self.contract, 'source_file_json_options', None) or {}
            for key, value in json_options.items():
                reader = reader.option(key, value)
            reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")

        return reader.load(file_path)

    def _read_parquet_with_casting(self, file_path: str, expected_schema: StructType) -> DataFrame:
        """
        Read Parquet without schema enforcement, then cast columns to expected types.

        This handles cases where Parquet physical types (like FIXED_LEN_BYTE_ARRAY)
        don't match the expected logical types (like string).
        """
        logger.info("Reading Parquet without schema enforcement and casting columns...")

        # Read without schema
        df = self.spark.read.format("parquet").load(file_path)

        # Cast each column to expected type
        for field in expected_schema.fields:
            col_name = field.name
            expected_type = field.dataType

            if col_name in df.columns:
                # Check if type already matches
                actual_type = dict(df.dtypes)[col_name]
                if actual_type != str(expected_type):
                    logger.info(f"Casting column '{col_name}' from {actual_type} to {expected_type}")
                    df = df.withColumn(col_name, F.col(col_name).cast(expected_type))
            else:
                # Column missing - add as null with correct type
                logger.warning(f"Column '{col_name}' missing in file, adding as null")
                df = df.withColumn(col_name, F.lit(None).cast(expected_type))

        # Select only expected columns in expected order
        df = df.select([field.name for field in expected_schema.fields])

        logger.info("Successfully read and cast Parquet file")
        return df
    
    def _separate_valid_invalid(
        self, 
        df: DataFrame, 
        expected_schema: StructType
    ) -> Tuple[DataFrame, DataFrame, Dict[str, int]]:
        """
        Separate valid and invalid rows.
        
        Invalid rows are those with:
        - Corrupt records (if CSV/JSON)
        - All null values
        
        Returns:
            (valid_df, invalid_df, stats_dict)
        """
        total_count = df.count()
        
        # Check if corrupt record column exists
        has_corrupt_column = "_corrupt_record" in df.columns
        
        if has_corrupt_column:
            # Separate based on corrupt record column
            valid_df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
            invalid_df = df.filter(F.col("_corrupt_record").isNotNull())
        else:
            # No corrupt records - all rows are valid
            valid_df = df
            invalid_df = self.spark.createDataFrame([], df.schema)
        
        # Additional check: filter out rows where ALL data columns are null
        data_columns = [f.name for f in expected_schema.fields]
        
        # Create condition for at least one non-null column
        non_null_condition = None
        for col_name in data_columns:
            if col_name in valid_df.columns:
                if non_null_condition is None:
                    non_null_condition = F.col(col_name).isNotNull()
                else:
                    non_null_condition = non_null_condition | F.col(col_name).isNotNull()
        
        if non_null_condition is not None:
            all_null_df = valid_df.filter(~non_null_condition)
            valid_df = valid_df.filter(non_null_condition)
            
            # Combine with corrupt records for quarantine
            if all_null_df.count() > 0:
                invalid_df = invalid_df.union(all_null_df.select(invalid_df.columns))
        
        # Count results
        valid_count = valid_df.count()
        invalid_count = total_count - valid_count
        
        stats = {
            "valid_rows": valid_count,
            "invalid_rows": invalid_count
        }
        
        return valid_df, invalid_df, stats
    
    def _write_quarantine(self, invalid_df: DataFrame, source_path: str) -> str:
        """Write invalid rows to quarantine location."""
        try:
            quarantine_path = self.context.quarantine_path
            
            # Add quarantine metadata
            invalid_df_with_meta = invalid_df \
                .withColumn("quarantine_timestamp", F.current_timestamp()) \
                .withColumn("source_file", F.lit(source_path)) \
                .withColumn("process_queue_id", F.lit(self.context.process_queue_id))
            
            invalid_df_with_meta.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(quarantine_path)
            
            return quarantine_path
        
        except Exception as e:
            logger.error(f"Failed to write quarantine: {e}")
            raise