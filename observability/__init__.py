"""
Nova Framework Observability Module

Provides logging, metrics, statistics, and telemetry capabilities.

REFACTORED VERSION - Clean architecture without legacy dependencies.
"""

# Primary statistics interface (use this for pipelines)
from nova_framework.observability.stats import PipelineStats

# Logging
from nova_framework.observability.logging import get_logger, DeltaLogHandler, FrameworkLogger

# Telemetry
from nova_framework.observability.telemetry import TelemetryEmitter

# Optional: Metrics collector (backward compatibility)
try:
    from nova_framework.observability.metrics import MetricsCollector
except ImportError:
    # MetricsCollector is optional
    MetricsCollector = None

# Backward compatibility aliases
Telemetry = TelemetryEmitter  # Old name


__all__ = [
    # Primary interface
    "PipelineStats",
    
    # Logging
    "get_logger",
    "DeltaLogHandler",
    "FrameworkLogger",
    
    # Telemetry
    "TelemetryEmitter",
    "Telemetry",  # Backward compatibility
    
    # Optional
    "MetricsCollector",
]