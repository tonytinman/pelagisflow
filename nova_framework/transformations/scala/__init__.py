"""
Scala transformations for PelagisFlow.

Scala transformations are precompiled into JAR files and loaded dynamically.
Place compiled JARs in this directory or specify the path in the registry.

Example Scala transformation structure:

package com.pelagisflow.transformations

import org.apache.spark.sql.{DataFrame, SparkSession}

class ExampleTransformation {
  def transform(
    spark: SparkSession,
    inputDF: Option[DataFrame],
    config: Map[String, Any]
  ): DataFrame = {
    // Transformation logic here
    inputDF.getOrElse(spark.emptyDataFrame)
  }
}
"""
