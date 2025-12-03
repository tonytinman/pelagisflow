"""
Nova Framework Core Module

Provides configuration management, execution context, and core domain models.
"""

from nova_framework.core.config import (
    FrameworkConfig,
    CatalogConfig,
    StorageConfig,
    ObservabilityConfig,
    get_config,
    set_config
)

from nova_framework.core.context import ExecutionContext, PipelineContext

from nova_framework.core.models import ExecutionStatus, ExecutionResult

__all__ = [
    # Configuration
    "FrameworkConfig",
    "CatalogConfig",
    "StorageConfig",
    "ObservabilityConfig",
    "get_config",
    "set_config",

    # Context
    "ExecutionContext",
    "PipelineContext",  # Backward compatibility alias

    # Models
    "ExecutionStatus",
    "ExecutionResult",
]