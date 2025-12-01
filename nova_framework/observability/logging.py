"""
Logging framework for Nova Framework.

Provides structured logging with automatic persistence to Delta tables.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, Row
from nova_framework.core.config import get_config


class DeltaLogHandler(logging.Handler):
    """
    Custom log handler that writes to Delta table.
    """
    
    def __init__(self, catalog: str, table: str):
        super().__init__()
        self.catalog = catalog
        self.table = table
        self.full_table = f"{catalog}.{table}"
        self.spark = None
    
    def emit(self, record: logging.LogRecord):
        """Write log record to Delta table."""
        try:
            if self.spark is None:
                self.spark = SparkSession.getActiveSession()
            
            if self.spark:
                log_row = Row(
                    timestamp=datetime.fromtimestamp(record.created),
                    level=record.levelname,
                    logger=record.name,
                    message=self.format(record),
                    module=record.module,
                    function=record.funcName,
                    line=record.lineno
                )
                
                df = self.spark.createDataFrame([log_row])
                df.write.format("delta").mode("append").saveAsTable(self.full_table)
        except Exception as e:
            # Don't fail if logging fails
            print(f"[DeltaLogHandler] Failed to write log: {e}")


class FrameworkLogger:
    """
    Framework-wide logger with Delta persistence.
    """
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(f"nova_framework.{name}")
        self.config = get_config()
        self._setup_handlers()
    
    def _setup_handlers(self):
        """Setup log handlers based on configuration."""
        # Console handler (always enabled)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter(
                '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
            )
        )
        self.logger.addHandler(console_handler)
        
        # Delta handler (if enabled)
        if self.config.observability.log_to_delta:
            catalog = self.config.get_catalog_name()
            table = self.config.observability.log_table
            delta_handler = DeltaLogHandler(catalog, table)
            delta_handler.setFormatter(
                logging.Formatter('%(message)s')
            )
            self.logger.addHandler(delta_handler)
        
        # Set level
        level = getattr(logging, self.config.observability.log_level)
        self.logger.setLevel(level)
    
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self.logger.debug(message, extra=kwargs)
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        self.logger.info(message, extra=kwargs)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self.logger.warning(message, extra=kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message."""
        self.logger.error(message, extra=kwargs)
    
    def exception(self, message: str, **kwargs):
        """Log exception with traceback."""
        self.logger.exception(message, extra=kwargs)


def get_logger(name: str) -> FrameworkLogger:
    """Get a framework logger instance."""
    return FrameworkLogger(name)