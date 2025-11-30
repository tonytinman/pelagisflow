"""
Base transformation strategy interface.

Defines the contract for all transformation strategies in PelagisFlow.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession


class TransformationType(Enum):
    """Enumeration of supported transformation types."""
    SQL = "sql"
    PYTHON = "python"
    SCALA = "scala"


class AbstractTransformationStrategy(ABC):
    """
    Abstract base class for all transformation strategies.

    All transformations must:
    1. Accept a SparkSession and optional input DataFrame(s)
    2. Return a transformed DataFrame
    3. Support parameterization via configuration
    4. Be idempotent and deterministic where possible
    """

    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the transformation strategy.

        Args:
            spark: Active SparkSession
            config: Optional configuration dictionary for the transformation
        """
        self.spark = spark
        self.config = config or {}
        self._validate_config()

    @abstractmethod
    def transform(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute the transformation and return a DataFrame.

        Args:
            input_df: Optional input DataFrame (for transformations that need input)
            **kwargs: Additional arguments specific to the transformation

        Returns:
            Transformed DataFrame

        Raises:
            ValueError: If transformation requirements are not met
            RuntimeError: If transformation execution fails
        """
        pass

    def _validate_config(self) -> None:
        """
        Validate the transformation configuration.

        Override in subclasses to implement specific validation logic.

        Raises:
            ValueError: If configuration is invalid
        """
        pass

    @property
    @abstractmethod
    def transformation_type(self) -> TransformationType:
        """Return the type of this transformation strategy."""
        pass

    def get_metadata(self) -> Dict[str, Any]:
        """
        Return metadata about this transformation.

        Returns:
            Dictionary containing transformation metadata
        """
        return {
            'type': self.transformation_type.value,
            'config': self.config,
        }
