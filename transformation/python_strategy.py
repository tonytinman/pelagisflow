"""
Python-based transformation strategy.

Dynamically loads and executes custom Python transformation code.
"""

import importlib.util
import inspect
import sys
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from pyspark.sql import DataFrame, SparkSession

from transformation.base import AbstractTransformationStrategy, TransformationType


class PythonTransformationStrategy(AbstractTransformationStrategy):
    """
    Transformation strategy that executes custom Python code.

    The Python module must contain a function with signature:
        def transform(spark: SparkSession, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame

    The module can include:
    - Helper functions
    - Helper classes
    - Constants and configuration
    - Multiple .py files (organized as a package)

    The transformation is loaded dynamically at runtime from the transformations registry.
    """

    def __init__(
        self,
        spark: SparkSession,
        module_path: str,
        function_name: str = "transform",
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Python transformation strategy.

        Args:
            spark: Active SparkSession
            module_path: Path to Python module or package (relative to transformations/python/)
            function_name: Name of the transformation function (default: 'transform')
            config: Optional configuration dictionary passed to transformation
        """
        self.module_path = module_path
        self.function_name = function_name
        self._transform_func: Optional[Callable] = None
        super().__init__(spark, config)

    def _validate_config(self) -> None:
        """Validate Python transformation configuration."""
        if not self.module_path:
            raise ValueError("module_path must be specified")

        if not self.function_name:
            raise ValueError("function_name must be specified")

    def _load_transformation(self) -> Callable:
        """
        Dynamically load the transformation function from the module.

        Returns:
            The transformation function

        Raises:
            ValueError: If module or function cannot be loaded
        """
        if self._transform_func is not None:
            return self._transform_func

        try:
            # Resolve module path
            base_path = Path(__file__).parent.parent / "transformations" / "python"
            module_file = base_path / f"{self.module_path}.py"

            if not module_file.exists():
                # Try as package
                module_file = base_path / self.module_path / "__init__.py"
                if not module_file.exists():
                    raise ValueError(f"Module not found: {self.module_path}")

            # Load module dynamically
            spec = importlib.util.spec_from_file_location(
                f"transformations.python.{self.module_path}",
                module_file
            )
            if spec is None or spec.loader is None:
                raise ValueError(f"Failed to load module spec: {self.module_path}")

            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)

            # Get transformation function
            if not hasattr(module, self.function_name):
                raise ValueError(
                    f"Module {self.module_path} does not have function '{self.function_name}'"
                )

            self._transform_func = getattr(module, self.function_name)

            # Validate function signature
            sig = inspect.signature(self._transform_func)
            params = list(sig.parameters.keys())

            if len(params) < 1 or params[0] != 'spark':
                raise ValueError(
                    f"Transformation function must have 'spark' as first parameter. "
                    f"Found: {params}"
                )

            return self._transform_func

        except Exception as e:
            raise ValueError(f"Failed to load Python transformation: {str(e)}") from e

    def transform(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute Python transformation.

        Args:
            input_df: Optional input DataFrame
            **kwargs: Additional arguments passed to transformation function

        Returns:
            Transformed DataFrame

        Raises:
            ValueError: If transformation fails
        """
        try:
            # Load transformation function
            transform_func = self._load_transformation()

            # Merge config into kwargs
            transform_kwargs = {**self.config, **kwargs}

            # Execute transformation
            result_df = transform_func(self.spark, input_df, **transform_kwargs)

            # Validate result
            if not isinstance(result_df, DataFrame):
                raise ValueError(
                    f"Transformation must return a DataFrame, got {type(result_df)}"
                )

            if len(result_df.columns) == 0:
                raise ValueError("Transformation returned DataFrame with no columns")

            return result_df

        except Exception as e:
            raise ValueError(f"Python transformation failed: {str(e)}") from e

    @property
    def transformation_type(self) -> TransformationType:
        """Return Python transformation type."""
        return TransformationType.PYTHON

    def get_metadata(self) -> Dict[str, Any]:
        """Return metadata including module and function info."""
        metadata = super().get_metadata()
        metadata['module_path'] = self.module_path
        metadata['function_name'] = self.function_name
        return metadata
