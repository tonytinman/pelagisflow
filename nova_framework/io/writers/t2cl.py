from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from typing import Dict, Any, Optional
from datetime import datetime
import logging

from nova_framework.io.writers.base import AbstractWriter

logger = logging.getLogger(__name__)


class T2CLWriter(AbstractWriter):
    """
    Type 2 Change Log (T2CL) writer.

    Maintains full history of changes with effective dates.
    Creates new row for changes, marks old row as not current.
    Automatically creates/refreshes a materialized view for current position.

    Use case: Customer data, product data, any dimension that needs history
    """

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

        # Drop T2CL metadata columns for the current view
        t2cl_cols = ["effective_from", "effective_to", "is_current", "deletion_flag"]

        # Get table columns excluding T2CL metadata
        table_df = self.spark.table(target_table)
        select_cols = [col for col in table_df.columns if col not in t2cl_cols]
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
        Perform Type 2 Change Log merge.

        Args:
            df: Incoming DataFrame with natural_key_hash and change_key_hash
            target_table: Target table name (default: from contract)
            natural_key_col: Column with natural key hash
            change_key_col: Column with change tracking hash
            partition_col: Partition column name
            process_date: Effective date (default: today)
            soft_delete: Enable soft delete detection

        Returns:
            Dictionary with merge statistics
        """
        target = self._get_target_table(target_table)

        if process_date is None:
            process_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"T2CL merge to {target} (process_date={process_date})")

        # Prepare incoming with T2CL metadata
        incoming_prepared = (
            df
            .withColumn("effective_from", F.lit(process_date).cast("date"))
            .withColumn("effective_to", F.lit("9999-12-31").cast("date"))
            .withColumn("is_current", F.lit(True))
            .withColumn("deletion_flag", F.lit(False))
        )

        # First load - create table
        if not self.spark.catalog.tableExists(target):
            incoming_prepared.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .partitionBy(partition_col) \
                .saveAsTable(target)

            row_count = incoming_prepared.count()
            self._log_write_stats(row_count, "t2cl_first_load")

            # Create/refresh current position materialized view
            current_view = self._refresh_current_view(target)

            return {
                "strategy": "type_2_change_log",
                "first_load": True,
                "target_table": target,
                "current_view": current_view,
                "records_inserted": row_count
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
        deleted_df = self.spark.createDataFrame([], current.schema)

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

                # Create tombstone rows
                deleted_df = (
                    current.join(missing, natural_key_col)
                    .select(
                        natural_key_col,
                        change_key_col,
                        partition_col,
                        F.lit(process_date).cast("date").alias("effective_from"),
                        F.lit("9999-12-31").cast("date").alias("effective_to"),
                        F.lit(True).alias("is_current"),
                        F.lit(True).alias("deletion_flag")
                    )
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

        # Insert new, changed, and deleted rows
        to_insert = new.union(changed)

        if soft_delete and deleted_df.count() > 0:
            to_insert = to_insert.unionByName(deleted_df)

        # Collect statistics BEFORE writing (after writing, counts may return 0)
        new_count = new.count()
        changed_count = changed.count()
        deleted_count = deleted_df.count() if soft_delete else 0
        total_inserted = to_insert.count()

        logger.info(f"About to insert: new={new_count}, changed={changed_count}, deleted={deleted_count}, total={total_inserted}")

        if total_inserted > 0:
            to_insert.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .partitionBy(partition_col) \
                .saveAsTable(target)

        self._log_write_stats(total_inserted, "type_2_change_log")
        self.pipeline_stats.log_stat("t2cl_new", new_count)
        self.pipeline_stats.log_stat("t2cl_changed", changed_count)
        self.pipeline_stats.log_stat("t2cl_deleted", deleted_count)

        logger.info(f"T2CL merge complete: new={new_count}, changed={changed_count}, deleted={deleted_count}")

        # Create/refresh current position materialized view
        current_view = self._refresh_current_view(target)

        return {
            "strategy": "type_2_change_log",
            "target_table": target,
            "current_view": current_view,
            "new_records": new_count,
            "changed_records": changed_count,
            "soft_deleted": deleted_count,
            "records_inserted": total_inserted,
            "process_date": process_date
        }
