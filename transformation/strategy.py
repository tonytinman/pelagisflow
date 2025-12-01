"""
Unified transformation strategy that handles SQL, Python, and Scala transformations.
"""

import importlib.util
import inspect
import sys
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from pyspark.sql import DataFrame, SparkSession


class TransformationType(Enum):
    """Enumeration of supported transformation types."""
    SQL = "sql"
    PYTHON = "python"
    SCALA = "scala"


class TransformationStrategy:
    """
    Unified transformation strategy that handles all transformation types.

    This class can execute:
    - SQL transformations (via Spark SQL)
    - Python transformations (dynamically loaded from modules)
    - Scala transformations (via Py4J bridge to precompiled JARs)

    The transformation type is determined by the configuration provided.
    All transformations return a DataFrame.

    Example usage:
        # SQL transformation
        strategy = TransformationStrategy(
            spark,
            transformation_type=TransformationType.SQL,
            sql="SELECT * FROM table WHERE active = true"
        )
        df = strategy.transform()

        # Python transformation
        strategy = TransformationStrategy(
            spark,
            transformation_type=TransformationType.PYTHON,
            module_path="customer_aggregation",
            function_name="transform"
        )
        df = strategy.transform()

        # Scala transformation
        strategy = TransformationStrategy(
            spark,
            transformation_type=TransformationType.SCALA,
            class_name="com.example.MyTransformation",
            jar_path="/path/to/jar.jar"
        )
        df = strategy.transform()
    """

    def __init__(
        self,
        spark: SparkSession,
        transformation_type: TransformationType,
        config: Optional[Dict[str, Any]] = None,
        # SQL-specific
        sql: Optional[str] = None,
        # Python-specific
        module_path: Optional[str] = None,
        function_name: str = "transform",
        # Scala-specific
        class_name: Optional[str] = None,
        jar_path: Optional[str] = None,
    ):
        """
        Initialize transformation strategy.

        Args:
            spark: Active SparkSession
            transformation_type: Type of transformation (SQL, PYTHON, or SCALA)
            config: Optional configuration dictionary passed to transformation
            sql: SQL query (required for SQL transformations)
            module_path: Python module path (required for Python transformations)
            function_name: Python function name (default: 'transform')
            class_name: Fully qualified Scala class name (required for Scala transformations)
            jar_path: Path to Scala JAR file (optional if on classpath)
        """
        self.spark = spark
        self.transformation_type = transformation_type
        self.config = config or {}

        # Type-specific attributes
        self.sql = sql
        self.module_path = module_path
        self.function_name = function_name
        self.class_name = class_name
        self.jar_path = jar_path

        # Internal state
        self._transform_func: Optional[Callable] = None

        # Validate configuration
        self._validate()

    def _validate(self) -> None:
        """
        Validate transformation configuration based on type.

        Raises:
            ValueError: If required parameters for transformation type are missing
        """
        if self.transformation_type == TransformationType.SQL:
            if not self.sql or not isinstance(self.sql, str):
                raise ValueError("SQL transformation requires 'sql' parameter with valid SQL query")
            if self.sql.strip() == "":
                raise ValueError("SQL query cannot be empty or whitespace only")

        elif self.transformation_type == TransformationType.PYTHON:
            if not self.module_path:
                raise ValueError("Python transformation requires 'module_path' parameter")
            if not self.function_name:
                raise ValueError("Python transformation requires 'function_name' parameter")

        elif self.transformation_type == TransformationType.SCALA:
            if not self.class_name:
                raise ValueError("Scala transformation requires 'class_name' parameter")
            if self.jar_path and not Path(self.jar_path).exists():
                raise ValueError(f"JAR file not found: {self.jar_path}")

        else:
            raise ValueError(f"Unknown transformation type: {self.transformation_type}")

    def transform(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute the transformation and return a DataFrame.

        Args:
            input_df: Optional input DataFrame
            **kwargs: Additional arguments passed to transformation

        Returns:
            Transformed DataFrame

        Raises:
            ValueError: If transformation execution fails
        """
        try:
            if self.transformation_type == TransformationType.SQL:
                return self._transform_sql(input_df, **kwargs)
            elif self.transformation_type == TransformationType.PYTHON:
                return self._transform_python(input_df, **kwargs)
            elif self.transformation_type == TransformationType.SCALA:
                return self._transform_scala(input_df, **kwargs)
            else:
                raise ValueError(f"Unknown transformation type: {self.transformation_type}")

        except Exception as e:
            raise ValueError(f"{self.transformation_type.value.upper()} transformation failed: {str(e)}") from e

    def _transform_sql(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute SQL transformation.

        Args:
            input_df: Optional input DataFrame (registered as temp view)
            **kwargs: Additional arguments (temp_view_name)

        Returns:
            Transformed DataFrame
        """
        temp_view_name = kwargs.get('temp_view_name', 'input_table')

        # Register input DataFrame as temp view if provided
        if input_df is not None:
            input_df.createOrReplaceTempView(temp_view_name)

        # Execute SQL query
        result_df = self.spark.sql(self.sql)

        # Validate result
        if len(result_df.columns) == 0:
            raise ValueError("SQL transformation returned DataFrame with no columns")

        return result_df

    def _transform_python(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute Python transformation by dynamically loading the module.

        Args:
            input_df: Optional input DataFrame
            **kwargs: Additional arguments passed to transformation function

        Returns:
            Transformed DataFrame
        """
        # Load transformation function if not already loaded
        if self._transform_func is None:
            self._transform_func = self._load_python_module()

        # Merge config into kwargs
        transform_kwargs = {**self.config, **kwargs}

        # Execute transformation
        result_df = self._transform_func(self.spark, input_df, **transform_kwargs)

        # Validate result
        if not isinstance(result_df, DataFrame):
            raise ValueError(
                f"Python transformation must return a DataFrame, got {type(result_df)}"
            )

        if len(result_df.columns) == 0:
            raise ValueError("Python transformation returned DataFrame with no columns")

        return result_df

    def _load_python_module(self) -> Callable:
        """
        Dynamically load the Python transformation (function or class).

        Supports both:
        1. Function-based: def transform(spark, input_df=None, **kwargs) -> DataFrame
        2. Class-based: class MyTransform(AbstractTransformation) with run() method

        Returns:
            Callable that executes the transformation

        Raises:
            ValueError: If module or function/class cannot be loaded
        """
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

            # Get transformation function or class
            if not hasattr(module, self.function_name):
                raise ValueError(
                    f"Module {self.module_path} does not have '{self.function_name}'"
                )

            transform_obj = getattr(module, self.function_name)

            # Check if it's a class (class-based transformation)
            if inspect.isclass(transform_obj):
                return self._create_class_wrapper(transform_obj)

            # Otherwise, it's a function (function-based transformation)
            else:
                return self._validate_function(transform_obj)

        except Exception as e:
            raise ValueError(f"Failed to load Python transformation: {str(e)}") from e

    def _create_class_wrapper(self, transform_class) -> Callable:
        """
        Create a wrapper function for class-based transformations.

        Args:
            transform_class: The transformation class

        Returns:
            Wrapper function that instantiates class and calls run()
        """
        # Validate class has run() method
        if not hasattr(transform_class, 'run'):
            raise ValueError(
                f"Transformation class '{transform_class.__name__}' must have a 'run()' method"
            )

        # Create wrapper that instantiates and calls run()
        def wrapper(spark: SparkSession, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
            instance = transform_class(spark)
            return instance.run(input_df, **kwargs)

        return wrapper

    def _validate_function(self, transform_func: Callable) -> Callable:
        """
        Validate function-based transformation signature.

        Args:
            transform_func: The transformation function

        Returns:
            The validated function

        Raises:
            ValueError: If function signature is invalid
        """
        sig = inspect.signature(transform_func)
        params = list(sig.parameters.keys())

        if len(params) < 1 or params[0] != 'spark':
            raise ValueError(
                f"Transformation function must have 'spark' as first parameter. "
                f"Found: {params}"
            )

        return transform_func

    def _transform_scala(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute Scala transformation via Py4J bridge.

        Args:
            input_df: Optional input DataFrame
            **kwargs: Additional arguments merged with config

        Returns:
            Transformed DataFrame
        """
        # Load JAR if specified
        if self.jar_path:
            self._load_scala_jar()

        # Get Scala-Python gateway
        jvm = self.spark.sparkContext._jvm

        # Instantiate Scala class
        try:
            scala_class = getattr(jvm, self.class_name)
            scala_instance = scala_class()
        except AttributeError as e:
            raise ValueError(
                f"Scala class '{self.class_name}' not found. "
                f"Ensure JAR is on classpath and class name is correct."
            ) from e

        # Convert config to Scala Map
        scala_map = self._python_dict_to_scala_map(self.config)

        # Convert input DataFrame to Java DataFrame (or None/null)
        if input_df is not None:
            java_input_df = input_df._jdf
            # Wrap in Scala Option(Some)
            scala_option = jvm.scala.Some(java_input_df)
        else:
            # Scala Option(None)
            scala_option = getattr(jvm.scala, "None$").MODULE$

        # Call transform method
        java_result_df = scala_instance.transform(
            self.spark._jsparkSession,
            scala_option,
            scala_map
        )

        # Convert Java DataFrame back to Python DataFrame
        result_df = DataFrame(java_result_df, self.spark)

        # Validate result
        if len(result_df.columns) == 0:
            raise ValueError("Scala transformation returned DataFrame with no columns")

        return result_df

    def _load_scala_jar(self) -> None:
        """
        Load JAR file onto Spark classpath.

        Raises:
            ValueError: If JAR cannot be loaded
        """
        try:
            jar_path = str(Path(self.jar_path).resolve())
            self.spark.sparkContext.addPyFile(jar_path)
        except Exception as e:
            raise ValueError(f"Failed to load JAR file: {str(e)}") from e

    def _python_dict_to_scala_map(self, python_dict: Dict[str, Any]):
        """
        Convert Python dictionary to Scala Map.

        Args:
            python_dict: Python dictionary

        Returns:
            Scala Map object
        """
        jvm = self.spark.sparkContext._jvm
        scala_map = jvm.scala.collection.mutable.HashMap()

        for key, value in python_dict.items():
            scala_map.put(key, value)

        return scala_map

    def get_metadata(self) -> Dict[str, Any]:
        """
        Return metadata about this transformation.

        Returns:
            Dictionary containing transformation metadata
        """
        metadata = {
            'type': self.transformation_type.value,
            'config': self.config,
        }

        # Add type-specific metadata
        if self.transformation_type == TransformationType.SQL:
            metadata['sql'] = self.sql
            metadata['sql_length'] = len(self.sql)

        elif self.transformation_type == TransformationType.PYTHON:
            metadata['module_path'] = self.module_path
            metadata['function_name'] = self.function_name

        elif self.transformation_type == TransformationType.SCALA:
            metadata['class_name'] = self.class_name
            metadata['jar_path'] = self.jar_path

        return metadata
