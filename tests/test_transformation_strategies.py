"""
Unit tests for transformation strategies.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from transformation.base import TransformationType, AbstractTransformationStrategy
from transformation.sql_strategy import SQLTransformationStrategy
from transformation.python_strategy import PythonTransformationStrategy
from transformation.scala_strategy import ScalaTransformationStrategy
from transformation.registry import TransformationRegistry, TransformationMetadata
from transformation.loader import TransformationLoader


@pytest.fixture
def spark():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_transformations")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    data = [
        ("customer1", "Alice", 100),
        ("customer2", "Bob", 200),
        ("customer3", "Charlie", 150),
    ]
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("amount", IntegerType(), False),
    ])
    return spark.createDataFrame(data, schema)


class TestSQLTransformationStrategy:
    """Tests for SQL transformation strategy."""

    def test_sql_strategy_initialization(self, spark):
        """Test SQL strategy can be initialized."""
        sql = "SELECT * FROM input_table"
        strategy = SQLTransformationStrategy(spark, sql)

        assert strategy.transformation_type == TransformationType.SQL
        assert strategy.sql == sql

    def test_sql_strategy_validation_fails_empty_sql(self, spark):
        """Test SQL strategy validation fails with empty SQL."""
        with pytest.raises(ValueError, match="SQL query cannot be empty"):
            SQLTransformationStrategy(spark, "")

    def test_sql_strategy_validation_fails_none_sql(self, spark):
        """Test SQL strategy validation fails with None SQL."""
        with pytest.raises(ValueError, match="SQL query must be a non-empty string"):
            SQLTransformationStrategy(spark, None)

    def test_sql_strategy_transform(self, spark, sample_df):
        """Test SQL strategy transformation."""
        sample_df.createOrReplaceTempView("input_table")

        sql = "SELECT customer_id, name, amount * 2 as doubled_amount FROM input_table"
        strategy = SQLTransformationStrategy(spark, sql)

        result = strategy.transform(sample_df)

        assert result.count() == 3
        assert "doubled_amount" in result.columns
        assert result.filter("customer_id = 'customer1'").first().doubled_amount == 200

    def test_sql_strategy_custom_temp_view_name(self, spark, sample_df):
        """Test SQL strategy with custom temp view name."""
        sql = "SELECT * FROM my_custom_view WHERE amount > 150"
        strategy = SQLTransformationStrategy(spark, sql)

        result = strategy.transform(sample_df, temp_view_name="my_custom_view")

        assert result.count() == 1
        assert result.first().customer_id == "customer2"

    def test_sql_strategy_metadata(self, spark):
        """Test SQL strategy metadata."""
        sql = "SELECT * FROM table"
        strategy = SQLTransformationStrategy(spark, sql)

        metadata = strategy.get_metadata()

        assert metadata['type'] == 'sql'
        assert metadata['sql'] == sql
        assert metadata['sql_length'] == len(sql)


class TestPythonTransformationStrategy:
    """Tests for Python transformation strategy."""

    def test_python_strategy_initialization(self, spark):
        """Test Python strategy can be initialized."""
        strategy = PythonTransformationStrategy(
            spark,
            module_path="test_module",
            function_name="transform"
        )

        assert strategy.transformation_type == TransformationType.PYTHON
        assert strategy.module_path == "test_module"
        assert strategy.function_name == "transform"

    def test_python_strategy_validation_fails_empty_module(self, spark):
        """Test Python strategy validation fails with empty module path."""
        with pytest.raises(ValueError, match="module_path must be specified"):
            PythonTransformationStrategy(spark, "", "transform")

    def test_python_strategy_validation_fails_empty_function(self, spark):
        """Test Python strategy validation fails with empty function name."""
        with pytest.raises(ValueError, match="function_name must be specified"):
            PythonTransformationStrategy(spark, "module", "")

    def test_python_strategy_metadata(self, spark):
        """Test Python strategy metadata."""
        strategy = PythonTransformationStrategy(
            spark,
            module_path="test_module",
            function_name="my_transform"
        )

        metadata = strategy.get_metadata()

        assert metadata['type'] == 'python'
        assert metadata['module_path'] == "test_module"
        assert metadata['function_name'] == "my_transform"

    def test_python_strategy_load_fails_nonexistent_module(self, spark):
        """Test Python strategy fails to load nonexistent module."""
        strategy = PythonTransformationStrategy(
            spark,
            module_path="nonexistent_module",
            function_name="transform"
        )

        with pytest.raises(ValueError, match="Module not found"):
            strategy.transform()


class TestScalaTransformationStrategy:
    """Tests for Scala transformation strategy."""

    def test_scala_strategy_initialization(self, spark):
        """Test Scala strategy can be initialized."""
        strategy = ScalaTransformationStrategy(
            spark,
            class_name="com.example.MyTransformation"
        )

        assert strategy.transformation_type == TransformationType.SCALA
        assert strategy.class_name == "com.example.MyTransformation"

    def test_scala_strategy_validation_fails_empty_class_name(self, spark):
        """Test Scala strategy validation fails with empty class name."""
        with pytest.raises(ValueError, match="class_name must be specified"):
            ScalaTransformationStrategy(spark, "")

    def test_scala_strategy_validation_fails_nonexistent_jar(self, spark):
        """Test Scala strategy validation fails with nonexistent JAR."""
        with pytest.raises(ValueError, match="JAR file not found"):
            ScalaTransformationStrategy(
                spark,
                class_name="com.example.Test",
                jar_path="/nonexistent/path.jar"
            )

    def test_scala_strategy_metadata(self, spark):
        """Test Scala strategy metadata."""
        strategy = ScalaTransformationStrategy(
            spark,
            class_name="com.example.MyTransformation",
            jar_path="/path/to/jar.jar"
        )

        metadata = strategy.get_metadata()

        assert metadata['type'] == 'scala'
        assert metadata['class_name'] == "com.example.MyTransformation"
        assert metadata['jar_path'] == "/path/to/jar.jar"


class TestTransformationRegistry:
    """Tests for transformation registry."""

    def test_registry_initialization(self, tmp_path):
        """Test registry can be initialized."""
        registry = TransformationRegistry(registry_path=tmp_path)
        assert registry.registry_path == tmp_path

    def test_registry_register_transformation(self, tmp_path):
        """Test registering a transformation."""
        registry = TransformationRegistry(registry_path=tmp_path)

        metadata = TransformationMetadata(
            name="test_transform",
            type=TransformationType.PYTHON,
            version="1.0.0",
            description="Test transformation",
            module_path="test_module"
        )

        registry.register(metadata)

        assert registry.exists("test_transform")
        assert registry.get("test_transform") == metadata

    def test_registry_register_duplicate_fails(self, tmp_path):
        """Test registering duplicate transformation fails."""
        registry = TransformationRegistry(registry_path=tmp_path)

        metadata = TransformationMetadata(
            name="test_transform",
            type=TransformationType.PYTHON,
            version="1.0.0",
            description="Test transformation",
            module_path="test_module"
        )

        registry.register(metadata)

        with pytest.raises(ValueError, match="already registered"):
            registry.register(metadata)

    def test_registry_update_transformation(self, tmp_path):
        """Test updating a transformation."""
        registry = TransformationRegistry(registry_path=tmp_path)

        metadata = TransformationMetadata(
            name="test_transform",
            type=TransformationType.PYTHON,
            version="1.0.0",
            description="Test transformation",
            module_path="test_module"
        )

        registry.register(metadata)

        updated_metadata = TransformationMetadata(
            name="test_transform",
            type=TransformationType.PYTHON,
            version="2.0.0",
            description="Updated transformation",
            module_path="test_module_v2"
        )

        registry.update(updated_metadata)

        assert registry.get("test_transform").version == "2.0.0"

    def test_registry_list_transformations(self, tmp_path):
        """Test listing transformations."""
        registry = TransformationRegistry(registry_path=tmp_path)

        registry.register(TransformationMetadata(
            name="python_transform",
            type=TransformationType.PYTHON,
            version="1.0.0",
            description="Python test",
            module_path="module1"
        ))

        registry.register(TransformationMetadata(
            name="scala_transform",
            type=TransformationType.SCALA,
            version="1.0.0",
            description="Scala test",
            class_name="com.example.Test"
        ))

        all_transforms = registry.list()
        assert len(all_transforms) == 2

        python_transforms = registry.list(type_filter=TransformationType.PYTHON)
        assert len(python_transforms) == 1
        assert python_transforms[0].name == "python_transform"

    def test_registry_unregister_transformation(self, tmp_path):
        """Test unregistering a transformation."""
        registry = TransformationRegistry(registry_path=tmp_path)

        metadata = TransformationMetadata(
            name="test_transform",
            type=TransformationType.PYTHON,
            version="1.0.0",
            description="Test transformation",
            module_path="test_module"
        )

        registry.register(metadata)
        assert registry.exists("test_transform")

        result = registry.unregister("test_transform")
        assert result is True
        assert not registry.exists("test_transform")

    def test_registry_stats(self, tmp_path):
        """Test registry statistics."""
        registry = TransformationRegistry(registry_path=tmp_path)

        registry.register(TransformationMetadata(
            name="python_transform",
            type=TransformationType.PYTHON,
            version="1.0.0",
            description="Python test",
            module_path="module1"
        ))

        stats = registry.get_stats()

        assert stats['total_transformations'] == 1
        assert stats['by_type']['python'] == 1
        assert 'python_transform' in stats['transformations']


class TestTransformationLoader:
    """Tests for transformation loader."""

    def test_loader_initialization(self, spark, tmp_path):
        """Test loader can be initialized."""
        loader = TransformationLoader(spark)
        assert loader.spark == spark
        assert loader.registry is not None

    def test_loader_load_sql(self, spark, sample_df):
        """Test loading SQL transformation directly."""
        sample_df.createOrReplaceTempView("test_table")

        loader = TransformationLoader(spark)
        strategy = loader.load_sql("SELECT * FROM test_table WHERE amount > 150")

        result = strategy.transform()
        assert result.count() == 1

    def test_loader_load_python(self, spark):
        """Test loading Python transformation directly."""
        loader = TransformationLoader(spark)
        strategy = loader.load_python("test_module", "transform")

        assert isinstance(strategy, PythonTransformationStrategy)
        assert strategy.module_path == "test_module"

    def test_loader_load_scala(self, spark):
        """Test loading Scala transformation directly."""
        loader = TransformationLoader(spark)
        strategy = loader.load_scala("com.example.Test")

        assert isinstance(strategy, ScalaTransformationStrategy)
        assert strategy.class_name == "com.example.Test"

    def test_loader_load_from_contract_sql(self, spark, sample_df):
        """Test loading transformation from contract with SQL."""
        sample_df.createOrReplaceTempView("test_table")

        contract = {
            'customProperties': {
                'transformationSql': 'SELECT * FROM test_table'
            }
        }

        loader = TransformationLoader(spark)
        strategy = loader.load_from_contract(contract)

        assert isinstance(strategy, SQLTransformationStrategy)

    def test_loader_load_from_contract_python(self, spark):
        """Test loading transformation from contract with Python."""
        contract = {
            'customProperties': {
                'transformationType': 'python',
                'transformationModule': 'test_module',
                'transformationFunction': 'my_transform'
            }
        }

        loader = TransformationLoader(spark)
        strategy = loader.load_from_contract(contract)

        assert isinstance(strategy, PythonTransformationStrategy)
        assert strategy.function_name == 'my_transform'

    def test_loader_load_from_contract_scala(self, spark):
        """Test loading transformation from contract with Scala."""
        contract = {
            'customProperties': {
                'transformationType': 'scala',
                'transformationClass': 'com.example.MyTransform'
            }
        }

        loader = TransformationLoader(spark)
        strategy = loader.load_from_contract(contract)

        assert isinstance(strategy, ScalaTransformationStrategy)
        assert strategy.class_name == 'com.example.MyTransform'

    def test_loader_load_from_contract_invalid_type(self, spark):
        """Test loading transformation from contract with invalid type."""
        contract = {
            'customProperties': {
                'transformationType': 'invalid_type'
            }
        }

        loader = TransformationLoader(spark)

        with pytest.raises(ValueError, match="Unknown transformation type"):
            loader.load_from_contract(contract)

    def test_loader_load_from_registry(self, spark, tmp_path):
        """Test loading transformation from registry."""
        registry = TransformationRegistry(registry_path=tmp_path)
        registry.register(TransformationMetadata(
            name="test_transform",
            type=TransformationType.PYTHON,
            version="1.0.0",
            description="Test",
            module_path="test_module"
        ))

        loader = TransformationLoader(spark, registry=registry)
        strategy = loader.load_from_registry("test_transform")

        assert isinstance(strategy, PythonTransformationStrategy)
        assert strategy.module_path == "test_module"

    def test_loader_load_from_registry_not_found(self, spark, tmp_path):
        """Test loading nonexistent transformation from registry."""
        registry = TransformationRegistry(registry_path=tmp_path)
        loader = TransformationLoader(spark, registry=registry)

        with pytest.raises(ValueError, match="not found in registry"):
            loader.load_from_registry("nonexistent")
