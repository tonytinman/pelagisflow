"""
Write Stage - Writes data to target table.

This stage uses IOFactory to create the appropriate writer based on
contract configuration (writeStrategy).
"""

from pyspark.sql import DataFrame
from nova_framework.pipeline.stages.base import AbstractStage
from nova_framework.io.factory import IOFactory


class WriteStage(AbstractStage):
    """
    Stage for writing data to target.
    
    Uses IOFactory to create appropriate writer based on contract's
    customProperties.writeStrategy setting:
    - overwrite: Full table refresh
    - append: Append-only writes
    - type_2_change_log: Type 2 Change Log
    - scd4: Current + Historical tables
    - file_export: Export to file
    
    Args:
        context: Execution context
        stats: Statistics tracker
        
    Example:
        stage = WriteStage(context, stats)
        df_written = stage.execute(df)
    """
    
    def __init__(self, context, stats):
        super().__init__(context, stats, "Write")
    
    def execute(self, df: DataFrame) -> DataFrame:
        """
        Write data to target.
        
        The write strategy is determined by contract configuration:
        customProperties.writeStrategy (defaults to "type_2_change_log")
        
        Args:
            df: Input DataFrame to write
            
        Returns:
            Same DataFrame (for potential chaining)
            
        Example:
            # Contract specifies:
            # customProperties:
            #   writeStrategy: type_2_change_log
            #   softDelete: true

            stage.execute(df)
            # Writes using T2CLWriter with soft delete enabled
        """
        self.logger.info("Starting write operation")
        
        # Create writer from contract configuration
        writer = IOFactory.create_writer_from_contract(
            self.context,
            self.stats
        )
        
        self.logger.info(f"Using writer: {writer.__class__.__name__}")
        
        # Write data
        write_stats = writer.write(df)
        
        # Store stats in context for later access
        self.context.set_state("write_stats", write_stats)
        
        # Log write metrics
        strategy = write_stats.get("strategy", "unknown")
        rows_written = write_stats.get("rows_written", 0)
        target = write_stats.get("target_table", "unknown")
        
        self.stats.log_rows_written(rows_written)
        
        # Log strategy-specific metrics
        if strategy == "type_2_change_log":
            new_records = write_stats.get("new_records", 0)
            changed_records = write_stats.get("changed_records", 0)
            soft_deleted = write_stats.get("soft_deleted", 0)

            self.logger.info(
                f"T2CL write complete: {rows_written} records written "
                f"({new_records} new, {changed_records} changed, {soft_deleted} deleted) "
                f"to {target}"
            )

            self.stats.log_stat("t2cl_new_records", new_records)
            self.stats.log_stat("t2cl_changed_records", changed_records)
            self.stats.log_stat("t2cl_soft_deleted", soft_deleted)
            
        elif strategy == "scd4":
            current_rows = write_stats.get("current_rows", 0)
            current_table = write_stats.get("current_table", "unknown")
            historical_table = write_stats.get("historical_table", "unknown")
            
            self.logger.info(
                f"SCD4 write complete: {current_rows} rows to {current_table}, "
                f"history maintained in {historical_table}"
            )
            
        elif strategy == "append":
            deduplicated = write_stats.get("deduplicated", False)
            dedup_removed = write_stats.get("dedup_removed", 0)
            
            if deduplicated:
                self.logger.info(
                    f"Append write complete: {rows_written} rows written to {target} "
                    f"({dedup_removed} duplicates removed before append)"
                )
            else:
                self.logger.info(
                    f"Append write complete: {rows_written} rows written to {target}"
                )
                
        elif strategy == "overwrite":
            optimized = write_stats.get("optimized", False)
            
            self.logger.info(
                f"Overwrite complete: {rows_written} rows written to {target}"
                + (" (optimized)" if optimized else "")
            )
            
        elif strategy == "file_export":
            output_path = write_stats.get("output_path", "unknown")
            file_format = write_stats.get("file_format", "unknown")
            
            self.logger.info(
                f"File export complete: {rows_written} rows exported to {output_path} "
                f"as {file_format}"
            )
            
        else:
            self.logger.info(
                f"Write complete: {rows_written} rows written using {strategy} strategy"
            )
        
        self.logger.info("Write operation completed successfully")
        
        return df