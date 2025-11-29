from typing import Optional
from nova_framework.core.context import ExecutionContext
from nova_framework.observability.stats import PipelineStats
from nova_framework.io.readers.base import AbstractReader
from nova_framework.io.readers.file_reader import FileReader
from nova_framework.io.readers.table_reader import TableReader
from nova_framework.io.writers.base import AbstractWriter
from nova_framework.io.writers.overwrite import OverwriteWriter
from nova_framework.io.writers.append import AppendWriter
from nova_framework.io.writers.scd2 import SCD2Writer
from nova_framework.io.writers.scd4 import SCD4Writer
from nova_framework.io.writers.file_export import FileExportWriter


class IOFactory:
    """
    Factory for creating appropriate reader/writer strategies.
    """
    
    @staticmethod
    def create_reader(
        reader_type: str,
        context: ExecutionContext,
        pipeline_stats: PipelineStats
    ) -> AbstractReader:
        """
        Create reader based on type.
        
        Args:
            reader_type: Type of reader ('file', 'table')
            context: Pipeline context
            pipeline_stats: Statistics tracker
            
        Returns:
            Concrete reader instance
            
        Raises:
            ValueError: If reader_type is unknown
        """
        readers = {
            "file": FileReader,
            "table": TableReader
        }
        
        reader_class = readers.get(reader_type.lower())
        
        if reader_class is None:
            raise ValueError(
                f"Unknown reader type: {reader_type}. "
                f"Available: {list(readers.keys())}"
            )
        
        return reader_class(context, pipeline_stats)
    
    @staticmethod
    def create_writer(
        writer_type: str,
        context: ExecutionContext,
        pipeline_stats: PipelineStats
    ) -> AbstractWriter:
        """
        Create writer based on type.
        
        Args:
            writer_type: Type of writer ('overwrite', 'append', 'scd2', 'scd4', 'file_export')
            context: Pipeline context
            pipeline_stats: Statistics tracker
            
        Returns:
            Concrete writer instance
            
        Raises:
            ValueError: If writer_type is unknown
        """
        writers = {
            "overwrite": OverwriteWriter,
            "append": AppendWriter,
            "scd2": SCD2Writer,
            "scd4": SCD4Writer,
            "file_export": FileExportWriter
        }
        
        writer_class = writers.get(writer_type.lower())
        
        if writer_class is None:
            raise ValueError(
                f"Unknown writer type: {writer_type}. "
                f"Available: {list(writers.keys())}"
            )
        
        return writer_class(context, pipeline_stats)
    
    @staticmethod
    def create_writer_from_contract(
        context: PipelineContext,
        pipeline_stats: PipelineStats
    ) -> AbstractWriter:
        """
        Create writer based on contract configuration.
        
        Reads 'customProperties.writeStrategy' from contract.
        
        Args:
            context: Pipeline context with loaded contract
            pipeline_stats: Statistics tracker
            
        Returns:
            Appropriate writer for contract
        """
        # Get write strategy from contract
        write_strategy = context.contract.get("customProperties.writeStrategy", "scd2")
        
        return IOFactory.create_writer(write_strategy, context, pipeline_stats)