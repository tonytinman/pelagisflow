"""
Nova Framework Core Module

Provides configuration management, execution context, and core domain models.
"""

from pelagisflow.core.config import (
    FrameworkConfig,
    CatalogConfig,
    StorageConfig,
    ObservabilityConfig,
    get_config,
    set_config,
    reset_config
)

from pelagisflow.core.context import ExecutionContext, PipelineContext

from pelagisflow.core.models import ExecutionStatus, ExecutionResult

__all__ = [
    # Configuration
    "FrameworkConfig",
    "CatalogConfig",
    "StorageConfig",
    "ObservabilityConfig",
    "get_config",
    "set_config",
    "reset_config",
    
    # Context
    "ExecutionContext",
    "PipelineContext",  # Backward compatibility alias
    
    # Models
    "ExecutionStatus",
    "ExecutionResult",
]