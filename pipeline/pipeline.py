from nova_framework.utils.telemetry import Telemetry
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Union
from nova_framework.observability.context import PipelineContext
from nova_framework.observability.pipeline_stats import PipelineStats
from nova_framework.io.io import DataReader
from nova_framework.quality.dq import DQEngine
from pyspark.sql import DataFrame, functions as F
from databricks.sdk.runtime import *
from delta.tables import DeltaTable

class PipelineFunctions:
    """
    Class containing functions for data processing.
    """
    @staticmethod
    def add_standard_lineage(df: DataFrame, process_queue_id) -> DataFrame:
        return df \
        .withColumn("source_file_path", F.col("_metadata.file_path")) \
        .withColumn("source_file_name", F.col("_metadata.file_name")) \
        .withColumn("process_queue_id", F.lit(process_queue_id)) \
        .withColumn("processed_at", F.current_timestamp()) \
        .withColumn("processed_by",F.lit("nova_framework"))
   
    @staticmethod
    def add_hash_column(df:DataFrame,column_name: str,key_columns: list) ->DataFrame:
        return df.withColumn( f"{column_name}", F.hash(*[F.coalesce(F.col(c),F.lit("NULL")) for c in key_columns]) )
       
    @staticmethod
    def add_partition_hash(df:DataFrame, num_partitions: int = 100) -> DataFrame:
        return df.withColumn( "partition_key", F.abs(F.col("natural_key_hash")) % num_partitions )
   
    @staticmethod
    def deduplicate(df: DataFrame,natural_key_hash_col: str = "natural_key_hash",change_key_hash_col: str = "change_key_hash", pipeline_stats: PipelineStats = None) -> DataFrame:
        """Simple deduplication - keeps arbitrary record per unique hash combination"""
        org_count = df.count()
        df = df.dropDuplicates([natural_key_hash_col, change_key_hash_col])
        dedeup_count = org_count - df.count()
        pipeline_stats.log_stat("deduped",dedeup_count)
        return df
   
    @staticmethod
    def build_current_table(
    historical_table: str,
    current_table: str
    ) -> dict:
        """Build current table from historical SCD2 table"""
   
        # Get current records
        current_df = spark.table(historical_table).filter("is_current = true")
   
        # Drop SCD2 columns
        scd2_cols = ["effective_from", "effective_to", "is_current", "inserted_at", "updated_at"]
        current_df = current_df.drop(*[c for c in scd2_cols if c in current_df.columns])
   
        # Overwrite current table
        current_df.write.format("delta").mode("overwrite").saveAsTable(current_table)
   
        return {"current_records": current_df.count()}

   
    @staticmethod
    def merge_scd2(
        incoming_df: DataFrame,
        target_table: str,
        natural_key_hash_col: str = "natural_key_hash",
        change_key_hash_col: str = "change_key_hash",
        partition_col: str = "partition_key",
        process_date: str = None,
        pipeline_stats: PipelineStats = None,
        soft_delete: bool = False   # << ENABLE SOFT DELETES (default off)
    ) -> dict:
        """SCD Type 2 merge with schema evolution + optional soft deletes."""
        from datetime import datetime

        if process_date is None:
            process_date = datetime.now().strftime("%Y-%m-%d")

        # -------------------------------------------------------------
        # 1. Prepare incoming records (add SCD2 metadata)
        # -------------------------------------------------------------
        incoming_prepared = (
            incoming_df
            .withColumn("effective_from", F.lit(process_date).cast("date"))
            .withColumn("effective_to", F.lit("9999-12-31").cast("date"))
            .withColumn("is_current", F.lit(True))
            .withColumn("deletion_flag", F.lit(False))
        )

        # -------------------------------------------------------------
        # 2. FIRST LOAD — create table
        # -------------------------------------------------------------
        if not spark.catalog.tableExists(target_table):
            incoming_prepared.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy(partition_col) \
                .saveAsTable(target_table)

            return {
                "first_load": True,
                "records_inserted": incoming_prepared.count()
            }

        # -------------------------------------------------------------
        # 3. Load current active records
        # -------------------------------------------------------------
        current = spark.table(target_table).filter("is_current = true")

        # -------------------------------------------------------------
        # 4. Join by natural key to identify new / changed / unchanged
        # -------------------------------------------------------------
        joined = incoming_prepared.alias("i").join(
            current.alias("c"),
            F.col(f"i.{natural_key_hash_col}") == F.col(f"c.{natural_key_hash_col}"),
            "left"
        )

        # New rows
        new = joined.filter(F.col(f"c.{natural_key_hash_col}").isNull()).select("i.*")

        # Changed rows (same natural key, different change hash)
        changed = (
            joined.filter(F.col(f"c.{natural_key_hash_col}").isNotNull())
                .filter(F.col(f"i.{change_key_hash_col}") != F.col(f"c.{change_key_hash_col}"))
                .select("i.*")
        )

        # -------------------------------------------------------------
        # 5. SOFT DELETE DETECTION (optional)
        # -------------------------------------------------------------
        deleted_df = spark.createDataFrame([], current.schema)

        if soft_delete:
            # Keys in current but missing in incoming
            incoming_keys = incoming_prepared.select(natural_key_hash_col).distinct()
            missing = (
                current.select(natural_key_hash_col)
                    .join(incoming_keys, natural_key_hash_col, "left_anti")
            )

            if missing.count() > 0:
                missing.createOrReplaceTempView("soft_delete_keys")

                # Mark existing rows as not current + set effective_to
                DeltaTable.forName(spark, target_table).update(
                    condition=(
                        f"{natural_key_hash_col} IN (SELECT {natural_key_hash_col} FROM soft_delete_keys) "
                        "AND is_current = true"
                    ),
                    set={
                        "is_current": "false",
                        "effective_to": f"date('{process_date}')",
                        "deletion_flag": "true"
                    }
                )

                # Insert a tombstone SCD2 row
                deleted_df = (
                    current.join(missing, natural_key_hash_col)
                        .select(
                            natural_key_hash_col,
                            change_key_hash_col,
                            partition_col,
                            # SCD2 fields
                            F.lit(process_date).cast("date").alias("effective_from"),
                            F.lit("9999-12-31").cast("date").alias("effective_to"),
                            F.lit(True).alias("is_current"),
                            F.lit(True).alias("deletion_flag")
                        )
                )

        # -------------------------------------------------------------
        # 6. Close existing rows for keys in "changed"
        # -------------------------------------------------------------
        if changed.count() > 0:
            changed.select(natural_key_hash_col).distinct().createOrReplaceTempView("changed_keys")
            DeltaTable.forName(spark, target_table).update(
                condition=(
                    f"{natural_key_hash_col} IN (SELECT {natural_key_hash_col} FROM changed_keys) "
                    "AND is_current = true"
                ),
                set={
                    "is_current": "false",
                    "effective_to": f"date('{process_date}')"
                }
            )

        # -------------------------------------------------------------
        # 7. Insert new, changed, and soft-deleted rows
        # -------------------------------------------------------------
        to_insert = new.union(changed)

        if soft_delete and deleted_df.count() > 0:
            to_insert = to_insert.unionByName(deleted_df)

        if to_insert.count() > 0:
            to_insert.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .partitionBy(partition_col) \
                .saveAsTable(target_table)

        if pipeline_stats:
            pipeline_stats.log_stat("merged_rows", to_insert.count())

        return {
            "new_records": new.count(),
            "changed_records": changed.count(),
            "soft_deleted": deleted_df.count() if soft_delete else 0,
            "records_inserted": to_insert.count()
        }

class PipelineStrategy(ABC):
    """
    Abstract base class for pipeline execution strategies.
    """

    @abstractmethod
    def execute(self, context: PipelineContext) -> str :
        """
        Execute the strategy using the provided pipeline context.
        """
        result = None
        return result

class IngestionPipeline(PipelineStrategy):
    """
    Concrete strategy for data ingestion.
    """

    def __init__(self, context: PipelineContext, pipeline_stats: PipelineStats):
        self.context = context
        self.pipeline_stats = pipeline_stats

    def execute(self) -> str :
        origin = "IngestionPipeline.execute()"
        Telemetry.log(origin, "Ingestion started")

        ################################################################################
        # read using DataReader
        ################################################################################
        reader = DataReader(self.context, self.pipeline_stats)
        df,report=reader.read(verbose=True, pipeline_stats=self.pipeline_stats)


        ################################################################################
        # apply ingestion augmentation
        ################################################################################
        #add lineage columns
        df = PipelineFunctions.add_standard_lineage(df,self.context.process_queue_id)
       
        #add hash columns
        natural_key_cols = self.context.contract.natural_key_columns
        change_key_cols = self.context.contract.change_tracking_columns
        df = PipelineFunctions.add_hash_column(df, 'natural_key_hash',natural_key_cols)
        df = PipelineFunctions.add_hash_column(df, 'change_key_hash',change_key_cols)
        df = PipelineFunctions.add_partition_hash(df,self.context.contract.recommended_partitions)

        #dedupe columns (using hash values)
        df = PipelineFunctions.deduplicate(df, pipeline_stats = self.pipeline_stats)
        ################################################################################

        ################################################################################
        # apply data quality rules
        ################################################################################
        engine = DQEngine()
        df_clean = engine.apply_cleansing(df, self.context.contract.cleansing_rules)
        summary, df_with_quality, df_dq_errors = engine.apply_dq(df_clean, self.context.contract.quality_rules)
        df_dq_errors = df_dq_errors.withColumn("data_contract_name",F.lit(self.context.data_contract_name))

        #write quality errors to violations schema

        fq_target_quality_errors_table = f"{self.context.catalog}.nova_framework.pipeline_dq_errors"
        df_dq_errors.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(fq_target_quality_errors_table)


        ################################################################################
        # write dataframe to target delta
        ################################################################################
        fq_target_table = f"{self.context.catalog}.{self.context.contract.schema_name}.{self.context.contract.table_name}"
        Telemetry.log(origin, f"Writing data file to: {fq_target_table}")
 
        merge_stats= PipelineFunctions.merge_scd2(df_with_quality,fq_target_table,pipeline_stats=self.pipeline_stats)
        Telemetry.log(origin, f"merge_stats={merge_stats}")
        ################################################################################

        Telemetry.log(origin, "Ingestion completed")
        result = 'SUCCESS'
        return result

class TransformationPipeline(PipelineStrategy):
    """
    Concrete strategy for data transformation.
    """
    def __init__(self, context: PipelineContext, pipeline_stats: PipelineStats):
        self.context = context
        self.pipeline_stats = pipeline_stats

    def execute(self) -> str :
        Telemetry.log("TransformationClass.execute", "Transformation started")    
        result = "FAILED"
        return result

class PipelineFactory:
    """Factory for creating pipeline instances based on process type"""
   
    @staticmethod
    def create(context: PipelineContext, pipeline_stats: PipelineStats) -> Union[IngestionPipeline, TransformationPipeline]:
        """Create appropriate pipeline based on context.contract.process_type
       
        Args:
            context: Pipeline context with loaded contract
           
        Returns:
            IngestionPipeline or TransformationPipeline instance
           
        Raises:
            ValueError: If process_type is unknown
        """
        Telemetry.log("PipelineFactory.create()", "START")
        pipeline_type = context.contract.pipeline_type
        Telemetry.log("PipelineFactory.create()", f"pipeline_type={pipeline_type}")
       
        if pipeline_type == 'ingestion':
            return IngestionPipeline(context,pipeline_stats)
        elif pipeline_type == 'transformation':
            return TransformationPipeline(context,pipeline_stats)
        else:
            raise ValueError(
                f"Unknown pipeline_type '{pipeline_type}' from contract: '{context.data_contract_name}'"
            )    

class Pipeline:
    """
    Orchestrates execution by loading a contract, selecting a strategy,
    initializing Spark, and executing the process.
    """

    def run(self, process_queue_id: int, data_contract_name: str, source_ref: str, env: str) -> str:
        """
        Entrypoint for all pipeline executions.
        """
        origin = f"Pipeline.run({process_queue_id},{data_contract_name},{source_ref},{env})"
        Telemetry.log(origin,"Pipeline [Start]")
        result = None

        try:
            ######################################################################
            # - setup pipeline context
            # - pipeline context is a data class which encapsulates all data required
            # to execute and audit a pipeline (including a data contract)
            # - context is injected into each different pipeline type and
            # available during excution
            ######################################################################
            context = PipelineContext(process_queue_id=process_queue_id,
                                    data_contract_name=data_contract_name,
                                    source_ref=source_ref,
                                    env=env)          
           
            pipeline_stats = PipelineStats()

            ######################################################################
            # create and execute a pipeline
            ######################################################################
            # use PipelineFactory to create a pipeline of the correct type
            # all pipeline types inherit from PipelineStrategy and implement an execute() method
            pipeline = PipelineFactory.create(context, pipeline_stats)
            result = pipeline.execute()

            pipeline_stats.summary()
            pipeline_stats.finalize(process_queue_id=process_queue_id)
            ######################################################################
        except Exception as e:
            Telemetry.log(origin,str(e))
            ######################################################################


        Telemetry.log(origin,f"Pipeline [End][{result}]")

        return result