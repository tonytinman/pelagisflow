from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from typing import Dict, Any, Optional
from datetime import datetime
import logging

from nova_framework.io.writers.base import AbstractWriter

logger = logging.getLogger(__name__)


class SCD2Writer(AbstractWriter):
    """
    SCD Type 2 writer with surrogate key generation.

    Extends T2CL functionality with automatic surrogate key generation.
    Maintains full history of changes with effective dates and generates
    unique integer surrogate keys for each record version.

    Surrogate key (sk column):
    - BIGINT type supporting 100M+ records
    - Automatically generated for new record versions
    - Sequential, starting from max existing + 1
    - Unique across all record versions

    Use case: Dimensional tables requiring surrogate keys for star schema joins
    """

    SURROGATE_KEY_COL = "sk"

    def _get_current_view_name(self, target_table: str) -> str:
        """
        Generate the materialized view name for current position.

        Format: mv_{schema}.{table}_curr
        Example: catalog.schema.customers -> catalog.mv_schema.customers_curr
        """
        parts = target_table.split(".")
        if len(parts) == 3:
            catalog, schema, table = parts
            return f"{catalog}.mv_{schema}.{table}_curr"
        elif len(parts) == 2:
            schema, table = parts
            return f"mv_{schema}.{table}_curr"
        else:
            return f"mv_{target_table}_curr"

    def _refresh_current_view(self, target_table: str) -> str:
        """
        Create or refresh the materialized view for current position.

        Returns the view name.
        """
        view_name = self._get_current_view_name(target_table)

        # Drop SCD2 metadata columns for the current view (keep surrogate key)
        scd2_metadata_cols = ["effective_from", "effective_to", "is_current", "deletion_flag"]

        # Get table columns excluding SCD2 metadata
        table_df = self.spark.table(target_table)
        select_cols = [col for col in table_df.columns if col not in scd2_metadata_cols]
        select_clause = ", ".join(select_cols)

        # Create or replace materialized view
        create_mv_sql = f"""
            CREATE OR REPLACE MATERIALIZED VIEW {view_name} AS
            SELECT {select_clause}
            FROM {target_table}
            WHERE is_current = true AND deletion_flag = false
        """

        logger.info(f"Creating/refreshing materialized view: {view_name}")
        self.spark.sql(create_mv_sql)

        # Refresh the materialized view to ensure it's up to date
        refresh_sql = f"REFRESH MATERIALIZED VIEW {view_name}"
        self.spark.sql(refresh_sql)

        logger.info(f"Materialized view refreshed: {view_name}")
        return view_name

    def _get_max_surrogate_key(self, target_table: str) -> int:
        """
        Get the current maximum surrogate key from the target table.

        Returns 0 if table doesn't exist or is empty.
        """
        if not self.spark.catalog.tableExists(target_table):
            return 0

        max_sk = self.spark.table(target_table).agg(
            F.coalesce(F.max(self.SURROGATE_KEY_COL), F.lit(0)).alias("max_sk")
        ).collect()[0]["max_sk"]

        return int(max_sk) if max_sk else 0

    def _add_surrogate_keys(self, df: DataFrame, start_key: int) -> DataFrame:
        """
        Add surrogate keys to a DataFrame starting from start_key + 1.

        Uses row_number() window function for sequential key generation.
        """
        if df.isEmpty():
            return df.withColumn(self.SURROGATE_KEY_COL, F.lit(None).cast("bigint"))

        # Use monotonically_increasing_id for ordering, then row_number for sequential keys
        window = Window.orderBy(F.monotonically_increasing_id())

        return df.withColumn(
            self.SURROGATE_KEY_COL,
            (F.row_number().over(window) + F.lit(start_key)).cast("bigint")
        )

    def write(
        self,
        df: DataFrame,
        target_table: Optional[str] = None,
        natural_key_col: str = "natural_key_hash",
        change_key_col: str = "change_key_hash",
        partition_col: str = "partition_key",
        process_date: Optional[str] = None,
        soft_delete: bool = False
    ) -> Dict[str, Any]:
        """
        Perform SCD Type 2 merge with surrogate key generation.

        Args:
            df: Incoming DataFrame with natural_key_hash and change_key_hash
            target_table: Target table name (default: from contract)
            natural_key_col: Column with natural key hash
            change_key_col: Column with change tracking hash
            partition_col: Partition column name
            process_date: Effective date (default: today)
            soft_delete: Enable soft delete detection

        Returns:
            Dictionary with merge statistics including surrogate key info
        """
        target = self._get_target_table(target_table)

        if process_date is None:
            process_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"SCD2 merge to {target} (process_date={process_date})")

        # Get current max surrogate key
        max_sk = self._get_max_surrogate_key(target)
        logger.info(f"Current max surrogate key: {max_sk}")

        # Prepare incoming with SCD2 metadata (without surrogate key yet)
        incoming_prepared = (
            df
            .withColumn("effective_from", F.lit(process_date).cast("date"))
            .withColumn("effective_to", F.lit("9999-12-31").cast("date"))
            .withColumn("is_current", F.lit(True))
            .withColumn("deletion_flag", F.lit(False))
        )

        # First load - create table with surrogate keys
        if not self.spark.catalog.tableExists(target):
            # Add surrogate keys starting from 1
            incoming_with_sk = self._add_surrogate_keys(incoming_prepared, 0)

            incoming_with_sk.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy(partition_col) \
                .saveAsTable(target)

            row_count = incoming_with_sk.count()
            self._log_write_stats(row_count, "scd2_first_load")

            # Create/refresh current position materialized view
            current_view = self._refresh_current_view(target)

            return {
                "strategy": "scd2",
                "first_load": True,
                "target_table": target,
                "current_view": current_view,
                "records_inserted": row_count,
                "max_surrogate_key": row_count
            }

        # Load current active records
        current = self.spark.table(target).filter("is_current = true")

        # Identify new, changed, unchanged rows
        joined = incoming_prepared.alias("i").join(
            current.alias("c"),
            F.col(f"i.{natural_key_col}") == F.col(f"c.{natural_key_col}"),
            "left"
        )

        new = joined.filter(F.col(f"c.{natural_key_col}").isNull()).select("i.*")

        changed = (
            joined
            .filter(F.col(f"c.{natural_key_col}").isNotNull())
            .filter(F.col(f"i.{change_key_col}") != F.col(f"c.{change_key_col}"))
            .select("i.*")
        )

        # Soft delete detection
        deleted_df = self.spark.createDataFrame([], incoming_prepared.schema)

        if soft_delete:
            incoming_keys = incoming_prepared.select(natural_key_col).distinct()
            missing = current.select(natural_key_col).join(
                incoming_keys, natural_key_col, "left_anti"
            )

            if missing.count() > 0:
                missing.createOrReplaceTempView("soft_delete_keys")

                # Mark as not current
                DeltaTable.forName(self.spark, target).update(
                    condition=(
                        f"{natural_key_col} IN (SELECT {natural_key_col} FROM soft_delete_keys) "
                        "AND is_current = true"
                    ),
                    set={
                        "is_current": "false",
                        "effective_to": f"date('{process_date}')",
                        "deletion_flag": "true"
                    }
                )

                # Create tombstone rows (will get new surrogate keys)
                deleted_df = (
                    current.join(missing, natural_key_col)
                    .select(
                        *[F.col(c) for c in incoming_prepared.columns]
                    )
                    .withColumn("effective_from", F.lit(process_date).cast("date"))
                    .withColumn("effective_to", F.lit("9999-12-31").cast("date"))
                    .withColumn("is_current", F.lit(True))
                    .withColumn("deletion_flag", F.lit(True))
                )

                logger.info(f"Soft deletes detected: {deleted_df.count()}")

        # Close existing rows for changed keys
        if changed.count() > 0:
            changed.select(natural_key_col).distinct().createOrReplaceTempView("changed_keys")

            DeltaTable.forName(self.spark, target).update(
                condition=(
                    f"{natural_key_col} IN (SELECT {natural_key_col} FROM changed_keys) "
                    "AND is_current = true"
                ),
                set={
                    "is_current": "false",
                    "effective_to": f"date('{process_date}')"
                }
            )

        # Collect statistics BEFORE writing
        new_count = new.count()
        changed_count = changed.count()
        deleted_count = deleted_df.count() if soft_delete else 0

        # Combine all records to insert
        to_insert = new.union(changed)

        if soft_delete and deleted_count > 0:
            to_insert = to_insert.unionByName(deleted_df)

        total_inserted = to_insert.count()

        logger.info(f"About to insert: new={new_count}, changed={changed_count}, deleted={deleted_count}, total={total_inserted}")

        # Add surrogate keys to new records
        new_max_sk = max_sk
        if total_inserted > 0:
            to_insert_with_sk = self._add_surrogate_keys(to_insert, max_sk)
            new_max_sk = max_sk + total_inserted

            to_insert_with_sk.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .partitionBy(partition_col) \
                .saveAsTable(target)

        self._log_write_stats(total_inserted, "scd2")
        self.pipeline_stats.log_stat("scd2_new", new_count)
        self.pipeline_stats.log_stat("scd2_changed", changed_count)
        self.pipeline_stats.log_stat("scd2_deleted", deleted_count)
        self.pipeline_stats.log_stat("scd2_max_sk", new_max_sk)

        logger.info(f"SCD2 merge complete: new={new_count}, changed={changed_count}, deleted={deleted_count}, max_sk={new_max_sk}")

        # Create/refresh current position materialized view
        current_view = self._refresh_current_view(target)

        return {
            "strategy": "scd2",
            "target_table": target,
            "current_view": current_view,
            "new_records": new_count,
            "changed_records": changed_count,
            "soft_deleted": deleted_count,
            "records_inserted": total_inserted,
            "max_surrogate_key": new_max_sk,
            "process_date": process_date
        }
