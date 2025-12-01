"""
Nova Framework Utils Module

Legacy utilities module - being deprecated.

Most functionality has been moved to:
- observability.telemetry (replaces utils.telemetry)
- observability.metrics (replaces utils.pipeline_metrics)

This module is kept for backward compatibility only.
"""

# Backward compatibility imports
try:
    from pelagisflow.utils.telemetry import Telemetry
except ImportError:
    Telemetry = None

try:
    from pelagisflow.utils.pipeline_metrics import PipelineMetrics
except ImportError:
    PipelineMetrics = None

__all__ = []

# Only export if available
if Telemetry is not None:
    __all__.append("Telemetry")
    
if PipelineMetrics is not None:
    __all__.append("PipelineMetrics")

# Deprecation warning
import warnings
warnings.warn(
    "nova_framework.utils is deprecated. "
    "Use nova_framework.observability instead.",
    DeprecationWarning,
    stacklevel=2
)