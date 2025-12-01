"""
Quality Stage - Applies data quality rules.

This stage applies both cleansing rules (modify data) and validation
rules (check data quality) based on contract configuration.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from nova_framework.pipeline.stages.base import AbstractStage
from nova_framework.quality.dq import DQEngine


class QualityStage(AbstractStage):
    """
    Stage for applying data quality rules.
    
    Performs two operations:
    1. Cleansing: Modify data (trim, uppercase, replace nulls, etc.)
    2. Validation: Check data quality and add dq_score column
    
    Validation errors are written to DQ errors table for monitoring.
    
    Args:
        context: Execution context
        stats: Statistics tracker
        
    Example:
        stage = QualityStage(context, stats)
        df_quality = stage.execute(df)
        # df_quality has dq_score column added
    """
    
    def __init__(self, context, stats):
        super().__init__(context, stats, "DataQuality")
        self.engine = DQEngine()
    
    def execute(self, df: DataFrame) -> DataFrame:
        """
        Apply data quality rules.
        
        Process:
        1. Apply cleansing rules (if defined)
        2. Apply validation rules (if defined)
        3. Write DQ errors to violations table
        4. Return DataFrame with dq_score column
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with quality rules applied and dq_score column
            
        Example:
            # Contract defines:
            # cleansing_rules:
            #   - rule: trim
            #     columns: [name, email]
            # quality_rules:
            #   - rule: not_null
            #     columns: [customer_id]
            
            df_out = stage.execute(df)
            # df_out has:
            # - Trimmed name and email columns
            # - dq_score column (100 if valid, <100 if issues)
        """
        # Step 1: Apply cleansing rules
        if self.context.contract.cleansing_rules:
            self.logger.info(
                f"Applying {len(self.context.contract.cleansing_rules)} cleansing rules"
            )
            df_clean = self.engine.apply_cleansing(
                df, 
                self.context.contract.cleansing_rules
            )
        else:
            self.logger.info("No cleansing rules defined, skipping cleansing")
            df_clean = df
        
        # Step 2: Apply validation rules
        if self.context.contract.quality_rules:
            self.logger.info(
                f"Applying {len(self.context.contract.quality_rules)} validation rules"
            )
            
            summary, df_with_quality, df_dq_errors = self.engine.apply_dq(
                df_clean,
                self.context.contract.quality_rules
            )
            
            # Log DQ statistics
            total_rows = summary.get('total_rows', 0)
            failed_rows = summary.get('failed_rows', 0)
            failed_pct = summary.get('failed_pct', 0)
            
            self.stats.log_stat("dq_total_rows", total_rows)
            self.stats.log_stat("dq_failed_rows", failed_rows)
            self.stats.log_stat("dq_failed_pct", failed_pct)
            
            self.logger.info(
                f"DQ validation complete: {total_rows} rows, "
                f"{failed_rows} failed ({failed_pct:.2f}%)"
            )
            
            # Write DQ errors if any
            error_count = df_dq_errors.count()
            if error_count > 0:
                self.logger.warning(f"Writing {error_count} DQ errors to violations table")
                self._write_dq_errors(df_dq_errors)
            else:
                self.logger.info("No DQ errors detected")
            
            return df_with_quality
        else:
            self.logger.info("No validation rules defined, skipping validation")
            return df_clean
    
    def _write_dq_errors(self, df_dq_errors: DataFrame):
        """
        Write DQ errors to violations table.
        
        Args:
            df_dq_errors: DataFrame containing DQ errors
        """
        from nova_framework.core.config import get_config
        
        try:
            # Add contract name to errors
            df_dq_errors = df_dq_errors.withColumn(
                "data_contract_name",
                F.lit(self.context.data_contract_name)
            )
            
            # Get DQ errors table from config
            config = get_config()
            catalog = self.context.catalog
            table = f"{catalog}.{config.observability.dq_errors_table}"
            
            self.logger.info(f"Writing DQ errors to {table}")
            
            # Write errors
            df_dq_errors.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(table)
            
            self.logger.info(f"Successfully wrote {df_dq_errors.count()} DQ errors")
            
        except Exception as e:
            self.logger.error(f"Failed to write DQ errors: {str(e)}")
            # Don't fail pipeline if DQ error writing fails
    
    def skip_condition(self) -> bool:
        """
        Skip if no quality rules are defined.
        
        Returns:
            True if stage should be skipped, False otherwise
        """
        has_cleansing = bool(self.context.contract.cleansing_rules)
        has_validation = bool(self.context.contract.quality_rules)
        
        should_skip = not (has_cleansing or has_validation)
        
        if should_skip:
            self.logger.info(
                "Skipping QualityStage - no cleansing or validation rules defined"
            )
        
        return should_skip