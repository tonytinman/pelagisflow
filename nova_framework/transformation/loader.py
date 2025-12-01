"""
Dynamic transformation loader.

Loads transformation strategies at runtime based on data contract specifications.
"""

from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

from nova_framework.transformation.strategy import TransformationStrategy, TransformationType
from nova_framework.transformation.registry import TransformationRegistry, TransformationMetadata


class TransformationLoader:
    """
    Loads transformation strategies dynamically based on configuration.

    The loader supports two modes:
    1. Registry-based: Load transformations by name from the registry
    2. Direct: Create transformations directly from configuration

    Example usage:
        # Registry-based loading
        loader = TransformationLoader(spark)
        strategy = loader.load_from_registry("customer_aggregation")

        # Direct SQL loading
        strategy = loader.load_sql("SELECT * FROM customers WHERE active = true")

        # Direct Python loading
        strategy = loader.load_python("aggregations/customer_rollup")
    """

    def __init__(self, spark: SparkSession, registry: Optional[TransformationRegistry] = None):
        """
        Initialize transformation loader.

        Args:
            spark: Active SparkSession
            registry: Optional TransformationRegistry (creates new if not provided)
        """
        self.spark = spark
        self.registry = registry or TransformationRegistry()

    def load_from_registry(
        self,
        name: str,
        config: Optional[Dict[str, Any]] = None
    ) -> TransformationStrategy:
        """
        Load a transformation by name from the registry.

        Args:
            name: Transformation name in registry
            config: Optional runtime configuration

        Returns:
            Instantiated transformation strategy

        Raises:
            ValueError: If transformation not found or invalid
        """
        metadata = self.registry.get(name)
        if metadata is None:
            raise ValueError(
                f"Transformation '{name}' not found in registry. "
                f"Available: {list(self.registry._transformations.keys())}"
            )

        return self._create_from_metadata(metadata, config)

    def load_from_contract(
        self,
        contract: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None
    ) -> TransformationStrategy:
        """
        Load a transformation based on data contract specification.

        The contract should contain customProperties with transformation configuration:
        - transformationType: "sql", "python", or "scala"
        - transformationName: Name in registry (for registry-based loading)
        OR
        - transformationSql: SQL query (for direct SQL)
        - transformationModule: Module path (for direct Python)
        - transformationClass: Class name (for direct Scala)

        Args:
            contract: Data contract dictionary
            config: Optional runtime configuration

        Returns:
            Instantiated transformation strategy

        Raises:
            ValueError: If contract configuration is invalid
        """
        custom_props = contract.get('customProperties', {})

        # Check for registry-based loading
        if 'transformationName' in custom_props:
            return self.load_from_registry(custom_props['transformationName'], config)

        # Direct loading based on type
        transformation_type = custom_props.get('transformationType', 'sql')

        if transformation_type == 'sql' or transformation_type == TransformationType.SQL.value:
            sql = custom_props.get('transformationSql')
            if not sql:
                raise ValueError("transformationSql required for SQL transformation")
            return self.load_sql(sql, config)

        elif transformation_type == 'python' or transformation_type == TransformationType.PYTHON.value:
            module_path = custom_props.get('transformationModule')
            if not module_path:
                raise ValueError("transformationModule required for Python transformation")
            function_name = custom_props.get('transformationFunction', 'transform')
            return self.load_python(module_path, function_name, config)

        elif transformation_type == 'scala' or transformation_type == TransformationType.SCALA.value:
            class_name = custom_props.get('transformationClass')
            if not class_name:
                raise ValueError("transformationClass required for Scala transformation")
            jar_path = custom_props.get('transformationJar')
            return self.load_scala(class_name, jar_path, config)

        else:
            raise ValueError(f"Unknown transformation type: {transformation_type}")

    def load_sql(
        self,
        sql: str,
        config: Optional[Dict[str, Any]] = None
    ) -> TransformationStrategy:
        """
        Create SQL transformation strategy directly.

        Args:
            sql: SQL query
            config: Optional configuration

        Returns:
            Transformation strategy configured for SQL
        """
        return TransformationStrategy(
            self.spark,
            transformation_type=TransformationType.SQL,
            sql=sql,
            config=config
        )

    def load_python(
        self,
        module_path: str,
        function_name: str = "transform",
        config: Optional[Dict[str, Any]] = None
    ) -> TransformationStrategy:
        """
        Create Python transformation strategy directly.

        Args:
            module_path: Path to Python module
            function_name: Transformation function name
            config: Optional configuration

        Returns:
            Transformation strategy configured for Python
        """
        return TransformationStrategy(
            self.spark,
            transformation_type=TransformationType.PYTHON,
            module_path=module_path,
            function_name=function_name,
            config=config
        )

    def load_scala(
        self,
        class_name: str,
        jar_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> TransformationStrategy:
        """
        Create Scala transformation strategy directly.

        Args:
            class_name: Fully qualified Scala class name
            jar_path: Optional path to JAR file
            config: Optional configuration

        Returns:
            Transformation strategy configured for Scala
        """
        return TransformationStrategy(
            self.spark,
            transformation_type=TransformationType.SCALA,
            class_name=class_name,
            jar_path=jar_path,
            config=config
        )

    def _create_from_metadata(
        self,
        metadata: TransformationMetadata,
        config: Optional[Dict[str, Any]] = None
    ) -> TransformationStrategy:
        """
        Create transformation strategy from registry metadata.

        Args:
            metadata: Transformation metadata from registry
            config: Optional runtime configuration

        Returns:
            Instantiated transformation strategy
        """
        if metadata.type == TransformationType.PYTHON:
            return TransformationStrategy(
                self.spark,
                transformation_type=TransformationType.PYTHON,
                module_path=metadata.module_path,
                function_name=metadata.function_name,
                config=config
            )
        elif metadata.type == TransformationType.SCALA:
            return TransformationStrategy(
                self.spark,
                transformation_type=TransformationType.SCALA,
                class_name=metadata.class_name,
                jar_path=metadata.jar_path,
                config=config
            )
        else:
            raise ValueError(
                f"Cannot create {metadata.type} transformation from registry. "
                f"SQL transformations must be provided directly."
            )
