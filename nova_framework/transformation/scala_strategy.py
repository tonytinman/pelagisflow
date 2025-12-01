"""
Scala-based transformation strategy.

Executes precompiled Scala transformations via Spark's Scala interop.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame, SparkSession

from nova_framework.transformation.base import AbstractTransformationStrategy, TransformationType


class ScalaTransformationStrategy(AbstractTransformationStrategy):
    """
    Transformation strategy that executes Scala code.

    Scala transformations must be:
    1. Pre-compiled into a JAR file
    2. Available on the Spark classpath
    3. Implement a standard interface with a transform method

    The Scala class must have a method with signature:
        def transform(spark: SparkSession, inputDF: Option[DataFrame], config: Map[String, Any]): DataFrame

    Example Scala transformation:
        package com.pelagisflow.transformations

        import org.apache.spark.sql.{DataFrame, SparkSession}

        class MyTransformation {
          def transform(
            spark: SparkSession,
            inputDF: Option[DataFrame],
            config: Map[String, Any]
          ): DataFrame = {
            // Transformation logic
            inputDF.getOrElse(spark.emptyDataFrame)
          }
        }
    """

    def __init__(
        self,
        spark: SparkSession,
        class_name: str,
        jar_path: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Scala transformation strategy.

        Args:
            spark: Active SparkSession
            class_name: Fully qualified Scala class name (e.g., 'com.pelagisflow.transformations.MyTransformation')
            jar_path: Optional path to JAR file (if not already on classpath)
            config: Optional configuration dictionary
        """
        self.class_name = class_name
        self.jar_path = jar_path
        super().__init__(spark, config)

    def _validate_config(self) -> None:
        """Validate Scala transformation configuration."""
        if not self.class_name:
            raise ValueError("class_name must be specified")

        if self.jar_path and not Path(self.jar_path).exists():
            raise ValueError(f"JAR file not found: {self.jar_path}")

    def _load_jar(self) -> None:
        """
        Load JAR file onto Spark classpath if specified.

        Raises:
            ValueError: If JAR cannot be loaded
        """
        if not self.jar_path:
            return

        try:
            # Add JAR to Spark context
            jar_path = str(Path(self.jar_path).resolve())
            self.spark.sparkContext.addPyFile(jar_path)
        except Exception as e:
            raise ValueError(f"Failed to load JAR file: {str(e)}") from e

    def transform(self, input_df: Optional[DataFrame] = None, **kwargs) -> DataFrame:
        """
        Execute Scala transformation via Py4J.

        Args:
            input_df: Optional input DataFrame
            **kwargs: Additional arguments merged with config

        Returns:
            Transformed DataFrame

        Raises:
            ValueError: If transformation fails
        """
        try:
            # Load JAR if specified
            self._load_jar()

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
                none_obj = getattr(jvm.scala, "None$")
                scala_option = getattr(none_obj, "MODULE$")

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

        except Exception as e:
            raise ValueError(f"Scala transformation failed: {str(e)}") from e

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

    @property
    def transformation_type(self) -> TransformationType:
        """Return Scala transformation type."""
        return TransformationType.SCALA

    def get_metadata(self) -> Dict[str, Any]:
        """Return metadata including Scala class info."""
        metadata = super().get_metadata()
        metadata['class_name'] = self.class_name
        metadata['jar_path'] = self.jar_path
        return metadata
