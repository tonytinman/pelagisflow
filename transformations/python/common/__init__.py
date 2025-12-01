"""
Common reusable transformation base classes and utilities.

This package provides base classes that encapsulate common patterns
and logic that can be reused across multiple transformations.
"""

from transformations.python.common.aggregation_base import (
    AggregationTransformationBase,
    TimeSeriesAggregationBase
)

__all__ = [
    "AggregationTransformationBase",
    "TimeSeriesAggregationBase",
]
