from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import functions as F
from typing import Tuple, Dict, List, Optional
from datetime import datetime
import json
from nova_framework.utils.telemetry import Telemetry
from nova_framework.utils.pipeline_metrics import PipelineMetrics
from nova_framework.observability.context import PipelineContext
from nova_framework.observability.pipeline_stats import PipelineStats
from nova_framework.contract.contract import DataContract

"""
DataReader - Core reader function for the framework
Supports all file types with schema validation and data quality checks
"""


class DataReader:
    """
    Core data reader for the framework.
    Reads data with schema validation, quality checks, and bad row isolation.
    """
   
    # Supported file formats
    SUPPORTED_FORMATS = ["parquet", "csv", "json", "delta", "avro", "orc", "text"]
   
    def __init__(self, context: PipelineContext, pipeline_stats: PipelineStats):
        """
        Initialize DataReader with pipeline context.
       
        Args:
            context: Pipeline context containing contract, paths, etc.
        """
        self.context = context
        self.pipeline_stats = pipeline_stats
        self.spark = SparkSession.getActiveSession()
       
        # Derived properties
        self.catalog = self.context.catalog
        self.schema = self.context.contract.schema
        self.volume = self.context.contract.volume
    # ========================================================================
    # MAIN READ METHOD
    # ========================================================================
   
    def read(
        self,
        file_path: Optional[str] = None,
        apply_validation_rules: bool = False,
        fail_on_schema_change: bool = True,
        fail_on_bad_rows: bool = False,
        invalid_row_threshold: float = 0.1,
        verbose: bool = True,
        pipeline_stats: PipelineStats = None
    ) -> Tuple[DataFrame, Dict]:
        """
        Core read method with comprehensive validation.
       
        Args:
            file_path: Path to data file (default: from context)
            apply_validation_rules: Apply contract validation rules
            fail_on_schema_change: Raise error on schema breaking changes
            fail_on_bad_rows: Raise error if bad row threshold exceeded
            invalid_row_threshold: Max allowed bad row rate (0.1 = 10%)
            verbose: Print progress and results
           
        Returns:
            Tuple of (good_df_with_dq_status, read_report_dict)
        """
        origin="DataReader.read()"
        start_time = datetime.now()
        print(f"{origin} - start_time: {start_time}")
       
        # Step 1: Initialize
        file_path = file_path or self.context.data_file_path
        print(f"{origin} - file_path:{file_path}")
        file_format = self._get_file_format()
        print(f"{origin} - file_format:{file_format}")
        expected_schema = self.context.contract.spark_schema
        print(f"{origin} - spark_schema:{expected_schema}")
        pipeline_stats = self.pipeline_stats
        print(f"{origin} - Initialise complete")
       
        if verbose:
            print("\n" + "="*80)
            print("DATA READER - STARTING")
            print("="*80)
            print(f"File: {file_path}")
            print(f"Format: {file_format}")
            print(f"Expected columns: {len(expected_schema.fields)}")
            print("="*80)

        # Step 2: Schema validation
        if verbose:
            print("\n[1/5] Schema Validation...")
       
        # check column and data type alignment
        # validate only at schema level - expected schema vs. actual schema
        schema_validation = self._validate_schema(file_path, expected_schema, file_format)
       
        if schema_validation["has_breaking_changes"]:
            if fail_on_schema_change:
                raise ValueError(
                    f"Schema breaking changes detected: "
                    f"Missing columns: {schema_validation['missing_columns']}, "
                    f"Type mismatches: {len(schema_validation['type_mismatches'])}"
                )
            elif verbose:
                print(f"⚠️  Schema warnings: {schema_validation['missing_columns']}")
        elif verbose:
            print("✅ Schema validation passed")

        # Step 3: Read data
        if verbose:
            print("\n[2/5] Reading data...")
       
        # get a dataframe based on a data file read with schema applied
        df = self._read_file(file_path, expected_schema, file_format)
        total_rows = df.count()
        pipeline_stats.log_stat("rows_read",total_rows)

       
        if verbose:
            print(f"✅ Read {total_rows:,} rows")

        #Separate valid and invalid rows (schema-based)
        if verbose:
            print("\n[3/5] Schema-based quality checks...")
       
        valid_df, invalid_df, schema_stats = self._separate_good_bad_rows(
            df,
            expected_schema
        )
       

        if verbose:
            print(f"✅ Valid rows: {schema_stats['valid_rows']:,} "
                  f"({schema_stats['total_rows']/total_rows*100:.1f}%)")
            print(f"❌ Invalid rows: {schema_stats['invalid_rows']:,} "
                  f"({schema_stats['rejection_rate']:.2f}%)")
            pipeline_stats.log_stat("valid_rows",schema_stats['valid_rows'])
            pipeline_stats.log_stat("invalid_rows",schema_stats['invalid_rows'])


        # quarantine schema invalid rows
        if verbose:
            print("\n[5/5] Writing invalid rows to quarantine...")
       
       
        quarantine_path = self._write_invalid_rows(invalid_df, file_path)
       
        if invalid_df.count() > 0:
            if verbose:
                print(f"📦 Invalid rows written to: {quarantine_path}")
        else:
            if verbose:
                print("✅ No invalid rows to quarantine")

        # Step 7: Build comprehensive report
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
       
        final_valid_rows = valid_df.count()
       
        read_report = {
            "timestamp": start_time.isoformat(),
            "processing_time_seconds": processing_time,
            "file_path": file_path,
            "file_format": file_format,
            "schema_validation": schema_validation,
            "total_rows_read": total_rows,
            "valid_rows": final_valid_rows,
            "invalid_rows": schema_stats['invalid_rows'],
            "rejection_rate": schema_stats['rejection_rate'],
            "quarantine_path": quarantine_path if schema_stats['invalid_rows'] > 0 else None,
            "success": True
        }

        # Step 8: Check thresholds
        if schema_stats['rejection_rate'] > (invalid_row_threshold * 100):
            error_msg = (
                f"Invalid row threshold exceeded! "
                f"{schema_stats['rejection_rate']:.2f}% invalid rows "
                f"(threshold: {invalid_row_threshold*100}%)"
            )
           
            if fail_on_bad_rows:
                read_report["success"] = False
                raise ValueError(error_msg)
            elif verbose:
                print(f"\n⚠️  WARNING: {error_msg}")

        # Final summary
        if verbose:
            print("\n" + "="*80)
            print("DATA READER - COMPLETE")
            print("="*80)
            print(f"✅ Valid rows: {final_valid_rows:,} "
                  f"({final_valid_rows/total_rows*100:.1f}%)")
            print(f"❌ Invalid rows: {schema_stats['invalid_rows']:,} "
                  f"({schema_stats['rejection_rate']:.2f}%)")
            print(f"⏱  Processing time: {processing_time:.2f}s")
            print("="*80 + "\n")

       
        return valid_df, read_report
   
    # ========================================================================
    # FILE FORMAT DETECTION AND READING
    # ========================================================================
   
    def _get_file_format(self) -> str:
        """Get file format from contract."""
        format_attr = self.context.contract.source_format.lower()
       
        if not format_attr:
            # Try to infer from file extension
            file_path = self.context.data_file_path
            if file_path.endswith(".parquet"):
                format_attr = "parquet"
            elif file_path.endswith(".csv"):
                format_attr = "csv"
            elif file_path.endswith(".json"):
                format_attr = "json"
            else:
                format_attr = "parquet"  # Default
       
        format_lower = format_attr.lower()
       
        if format_lower not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported file format: {format_attr}. "
                f"Supported formats: {self.SUPPORTED_FORMATS}"
            )
       
        return format_lower
   
    def _read_file(
        self,
        file_path: str,
        schema: StructType,
        file_format: str
    ) -> DataFrame:
        """Read file with appropriate format and options."""
       
        # apply schema to reader
        reader = self.spark.read.schema(schema)
       
        # return dataframe based on appropriate reader type
        if file_format == "parquet":
            return reader.parquet(file_path)
       
        elif file_format == "csv":
            # Get CSV options from contract
            csv_options = self.context.contract.source_file_csv_options
           
            reader = reader \
                .option("header", csv_options.get("header", True)) \
                .option("delimiter", csv_options.get("delimiter", ",")) \
                .option("quote", csv_options.get("quote", '"')) \
                .option("escape", csv_options.get("escape", "\\")) \
                .option("multiLine", csv_options.get("multiLine", False)) \
                .option("mode", "PERMISSIVE")
           
            return reader.csv(file_path)
       
        elif file_format == "json":
            # Get JSON options from contract
            json_options = self.context.contract.source_file_json_options
           
            reader = reader \
                .option("multiLine", json_options.get("multiLine", False)) \
                .option("mode", "PERMISSIVE")
           
            return reader.json(file_path)
       
        elif file_format == "delta":
            return self.spark.read.format("delta").load(file_path)
       
        elif file_format == "avro":
            return reader.format("avro").load(file_path)
       
        elif file_format == "orc":
            return reader.orc(file_path)
       
        elif file_format == "text":
            return self.spark.read.text(file_path)
       
        else:
            raise ValueError(f"Unsupported format: {file_format}")
   
    # ========================================================================
    # SCHEMA VALIDATION
    # ========================================================================
   
    def _validate_schema(
        self,
        file_path: str,
        expected_schema: StructType,
        file_format: str
    ) -> Dict:
        """Validate file schema against expected schema."""
       
        try:
            # Read schema without loading data (format-specific)
            if file_format == "parquet":
                actual_schema = self.spark.read.parquet(file_path).schema
            elif file_format == "csv":
                csv_options = self.context.contract.source_file_csv_options
                actual_schema = self.spark.read \
                    .option("header", csv_options.get("header", True)) \
                    .option("inferSchema", True) \
                    .csv(file_path).schema
            elif file_format == "json":
                actual_schema = self.spark.read.json(file_path).schema
            elif file_format == "delta":
                actual_schema = self.spark.read.format("delta").load(file_path).schema
            else:
                # For other formats, skip detailed schema validation
                return {
                    "has_breaking_changes": False,
                    "missing_columns": [],
                    "extra_columns": [],
                    "type_mismatches": []
                }
           
            # Compare schemas
            expected_cols = set(expected_schema.fieldNames())
            actual_cols = set(actual_schema.fieldNames())
           
            missing_cols = expected_cols - actual_cols
            extra_cols = actual_cols - expected_cols
           
            # Check type mismatches
            expected_fields = {f.name: f.dataType for f in expected_schema.fields}
            actual_fields = {f.name: f.dataType for f in actual_schema.fields}
           
            type_mismatches = []
            common_cols = expected_cols & actual_cols
            for col in common_cols:
                if expected_fields[col] != actual_fields[col]:
                    type_mismatches.append({
                        "column": col,
                        "expected_type": str(expected_fields[col]),
                        "actual_type": str(actual_fields[col])
                    })
           
            has_breaking_changes = len(missing_cols) > 0 or len(type_mismatches) > 0
           
            return {
                "has_breaking_changes": has_breaking_changes,
                "missing_columns": list(missing_cols),
                "extra_columns": list(extra_cols),
                "type_mismatches": type_mismatches
            }
           
        except Exception as e:
            return {
                "has_breaking_changes": False,
                "error": str(e),
                "missing_columns": [],
                "extra_columns": [],
                "type_mismatches": []
            }
   
    # ========================================================================
    # GOOD/BAD ROW SEPARATION
    # ========================================================================
   
    def _separate_good_bad_rows(
        self,
        df: DataFrame,
        schema: StructType
    ) -> Tuple[DataFrame, DataFrame, Dict]:
        """
        Separate good and bad rows based on schema constraints.
        Bad rows = rows with NULL in non-nullable columns.
        """

        print('Separating good and bad rows...')
        total_rows = df.count()
        print(f'Total rows: {total_rows}')
       
        # Build validation conditions for non-nullable columns
        validation_conditions = []
        rejection_reasons = []
       
        for field in schema.fields:
            col_name = field.name
            print(f'Validating column: {col_name}')
            if col_name not in df.columns:
                continue
           
            # Check nullable constraint
            if not field.nullable:
                null_condition = F.col(col_name).isNull()
                validation_conditions.append(~null_condition)
                rejection_reasons.append(
                    F.when(null_condition, F.lit(f"NULL in non-nullable column: {col_name}"))
                )
        print("cols validated")
        # All rows valid if no validation rules
        if validation_conditions:
            is_valid = validation_conditions[0]
            for condition in validation_conditions[1:]:
                is_valid = is_valid & condition
        else:
            is_valid = F.lit(True)
        print(f"is_valid:{is_valid}")

        # Add validation columns
        df_with_validation = df.withColumn("_is_valid", is_valid)
       
        # Add rejection reason
        print("adding rejection reasons...")
        if rejection_reasons:
            rejection_reason = rejection_reasons[0]
            for reason in rejection_reasons[1:]:
                rejection_reason = F.coalesce(rejection_reason, reason)
           
            df_with_validation = df_with_validation.withColumn(
                "_rejection_reason",
                F.when(~F.col("_is_valid"), rejection_reason).otherwise(F.lit(None))
            )
        else:
            df_with_validation = df_with_validation.withColumn(
                "_rejection_reason",
                F.lit(None).cast(StringType())
            )
        print("adding rejection reasons... complete")

        print("splitting good and bad rows...")
        # Split good and bad
        good_df = df_with_validation \
            .filter(F.col("_is_valid")) \
            .drop("_is_valid", "_rejection_reason")
       
        bad_df = df_with_validation \
            .filter(~F.col("_is_valid"))
        print("splitting good and bad rows... complete")
       
        print("collecting stats...")
        # Statistics
        good_count = good_df.count()
        bad_count = bad_df.count()
       
        stats = {
            "total_rows": total_rows,
            "valid_rows": good_count,
            "invalid_rows": bad_count,
            "rejection_rate": (bad_count / total_rows * 100) if total_rows > 0 else 0
        }
        print(f"collecting stats... {stats}")
       
        return good_df, bad_df, stats
   
    # ========================================================================
    # VALIDATION RULES
    # ========================================================================
   
   
    def _write_invalid_rows(
        self,
        invalid_df: DataFrame,
        source_file: str
    ) -> str:
        """
        Write bad rows to quarantine in bad_data/yyyymmdd folder.
       
        Args:
            bad_df: DataFrame of bad rows
            source_file: Source file path
           
        Returns:
            Quarantine path
        """

        run_date = datetime.now().strftime("%Y-%m-%d")  
        run_date_path =  run_date.replace("-", "") # yyyymmdd format

        # Build quarantine path: /Volumes/catalog/schema/volume/bad_data/yyyymmdd/
        quarantine_path = (
            f"/Volumes/{self.catalog}/{self.schema}/"
            f"/invalid_data/{self.context.data_contract_name}/{run_date_path}"
        )
       
        if invalid_df.count() == 0:
            return quarantine_path
       
        # Add metadata to bad rows
        invalid_df_with_metadata = invalid_df \
            .withColumn("_quarantine_timestamp", F.lit(datetime.now())) \
            .withColumn("_source_file", F.lit(source_file)) \
            .withColumn("_run_date", F.lit(self.run_date))
       
        # Write to quarantine
        invalid_df_with_metadata.write \
            .format("parquet") \
            .mode("append") \
            .save(quarantine_path)
       
        return quarantine_path
   
    # ========================================================================
    # REPORTING
    # ========================================================================
   
    def generate_report(self, read_report: Dict, format: str = "text") -> str:
        """
        Generate formatted read report.
       
        Args:
            read_report: Read report dictionary
            format: 'text' or 'json'
           
        Returns:
            Formatted report string
        """
        if format == "json":
            return json.dumps(read_report, indent=2)
       
        # Text format
        report = []
        report.append("=" * 80)
        report.append("DATA READ REPORT")
        report.append("=" * 80)
        report.append(f"Timestamp: {read_report['timestamp']}")
        report.append(f"File: {read_report['file_path']}")
        report.append(f"Format: {read_report['file_format']}")
        report.append(f"Processing Time: {read_report['processing_time_seconds']:.2f}s")
        report.append("")
       
        report.append("SCHEMA VALIDATION:")
        schema_val = read_report['schema_validation']
        if schema_val['has_breaking_changes']:
            report.append("  ❌ FAILED")
            if schema_val['missing_columns']:
                report.append(f"     Missing columns: {schema_val['missing_columns']}")
            if schema_val['type_mismatches']:
                report.append(f"     Type mismatches: {len(schema_val['type_mismatches'])}")
        else:
            report.append("  ✅ PASSED")
        report.append("")
       
        report.append("DATA QUALITY:")
        report.append(f"  Total rows read: {read_report['total_rows_read']:,}")
        report.append(f"  Good rows: {read_report['good_rows']:,} "
                     f"({read_report['good_rows']/read_report['total_rows_read']*100:.1f}%)")
        report.append(f"  Bad rows: {read_report['bad_rows']:,} "
                     f"({read_report['rejection_rate']:.2f}%)")
        report.append("")
       
        if read_report.get('validation_stats'):
            val_stats = read_report['validation_stats']
            report.append("VALIDATION RESULTS:")
            report.append(f"  ✅ Passed: {val_stats['rows_passed']:,}")
            report.append(f"  ⚠️  Warning: {val_stats['rows_warning']:,}")
            report.append(f"  ❌ Failed: {val_stats['rows_failed']:,}")
            report.append("")
       
        if read_report.get('quarantine_path'):
            report.append(f"BAD ROWS LOCATION:")
            report.append(f"  {read_report['quarantine_path']}")
            report.append("")
       
        report.append("=" * 80)
       
        return "\n".join(report)


class FileReader:
    """
    Reads a data file into a Spark DataFrame, applying schema validation and routing violations.

    Args:
        context: PipelineContext object encapsulates the necessary information for processing the data.
    Returns:
        DataFrame: DataFrame containing valid rows that match the schema.
    """

    def __init__(
        self,
        context: PipelineContext
    ):
        self.context = context
        self.file_format = self.context.contract.source_format
        self.data_file_path = self.context.data_file_path
        self.spark_schema = self.context.contract.spark_schema
        self.violations_schema = self.context.violations_schema
        self.violations_table = self.context.contract.table_name
        self.catalog = self.context.catalog

    def read_strict(self) -> DataFrame:
        """
        Reads the file, validates schema, writes violations, and returns valid data.
        """
        file_name = self.data_file_path.split("/")[-1].split(".")[0]

        #self.spark_schema = self.spark_schema.add("ingest_ts", "timestamp")}

        Telemetry.log("FileReader", f"Reading file: {self.data_file_path}")

        try:
            reader = spark.read.format(self.file_format).schema(self.spark_schema)

            # Read the data with schema enforcement and bad row capture
            if self.file_format in ["csv", "json", "text"]:
                reader = (
                    reader
                    .option("header", "true")
                    .option("columnNameOfCorruptRecord", "_corrupt_record")
                )
                df_raw = reader.load(self.data_file_path)

                # Split valid vs invalid rows
                has_corrupt_col = "_corrupt_record" in df_raw.columns
                if has_corrupt_col:
                    df_good = df_raw.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
                    df_bad = df_raw.filter(col("_corrupt_record").isNotNull())
                else:
                    df_good = df_raw
                    df_bad = spark.createDataFrame([], df_raw.schema)
            else:
                # For parquet, delta, orc — no _corrupt_record support
                df_good = reader.load(self.data_file_path)
                df_bad = spark.createDataFrame([], df_good.schema)

            rows_read = df_good.count() + df_bad.count()
            good_rows = df_good.count()
            bad_rows = df_bad.count()

            # Log throughput stats
            self.context.stats.log_stat("rows_read", rows_read)
            self.context.stats.log_stat("clean_rows", good_rows)
            self.context.stats.log_stat("violation_rows", bad_rows)

            Telemetry.log(
                "FileReader",
                f"Rows read={rows_read}, valid={good_rows}, violations={bad_rows}"
            )

            # Write violations only if there are any
            if bad_rows > 0:
                (
                    df_bad.write
                    .mode("append")
                    .saveAsTable(violations_table)
                )
                Telemetry.log("FileReader", f"Bad rows written to {violations_table}")

            return df_good

        except Exception as e:
            Telemetry.log("FileReader", f"Unexpected error reading file {self.data_file_path}: {e}")
            raise e

class Writer:

    def __init__(self):
        self.catalog = 'cluk_dev_nova'
        pass

    def write_data(self, df: DataFrame, contract: DataContract) -> None:

        origin = f"io.writer.write_data"
        Telemetry.log(origin,"Getting ready to write data to table")

        # target = f"{self.catalog}.{contract.schema}.{contract.table}"
        target = "sampletable"

        row_count = df.count()

        self._overwrite_write(df, target)

        # if contract.write_strategy.lower() == "merge":
        #     self._merge_write(df, target)
        # elif contract.write_strategy.lower() == "overwrite":
        #     self._overwrite_write(df, target)
        # elif contract.write_strategy.lower() == "append":
        #     self._append_write(df, target)
        # else:
        #     raise ValueError(f"Unsupported write strategy: {contract.write_strategy}")


    # -------------------------------------------------------------------------
    # Write strategy implementations
    # -------------------------------------------------------------------------
    def _merge_write(self, df: DataFrame, target: str) -> None:
        """
        Performs a merge (upsert) operation into the target table.
        Requires that the DataFrame and target table share a key column named 'id'.
        """
        df.createOrReplaceTempView("staging")

        merge_sql = f"""
        MERGE INTO {target} AS tgt
        USING staging AS src
        ON src.id = tgt.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        self.spark.sql(merge_sql)

    def _overwrite_write(self, df: DataFrame, target: str) -> None:
        """
        Overwrites the target table with new data.
        """
        # df.write.mode("overwrite").saveAsTable(target)
        origin = f"io.writer.write_data._overwrite_write"
        Telemetry.log(origin,"Overwrites the target table with new data")

        df.write.format("noop").mode("overwrite").save()
        Telemetry.log(origin,"Table Overwrite Completed")

    def _append_write(self, df: DataFrame, target: str) -> None:
        """
        Appends new rows to the target table.
        """
        df.write.mode("append").saveAsTable(target)