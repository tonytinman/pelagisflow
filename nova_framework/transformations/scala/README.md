# Scala Transformations

This directory contains precompiled Scala transformations for PelagisFlow.

## Requirements

Scala transformations must:
1. Be compiled into JAR files
2. Implement a standard interface with a `transform` method
3. Be registered in the transformation registry

## Implementation Template

```scala
package com.pelagisflow.transformations

import org.apache.spark.sql.{DataFrame, SparkSession}

class MyTransformation {

  /**
   * Transform method that PelagisFlow will call.
   *
   * @param spark SparkSession instance
   * @param inputDF Optional input DataFrame
   * @param config Configuration map
   * @return Transformed DataFrame
   */
  def transform(
    spark: SparkSession,
    inputDF: Option[DataFrame],
    config: Map[String, Any]
  ): DataFrame = {

    // Example: Read from a table if no input provided
    val df = inputDF.getOrElse {
      spark.table("default.source_table")
    }

    // Apply transformations
    df.filter("status = 'active'")
      .groupBy("category")
      .count()
  }
}
```

## Building Scala Transformations

### Using SBT

Create `build.sbt`:

```scala
name := "pelagisflow-transformations"
version := "1.0.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided"
)
```

Build the JAR:
```bash
sbt clean package
```

The JAR will be created in `target/scala-2.12/pelagisflow-transformations_2.12-1.0.0.jar`

### Using Maven

Create `pom.xml`:

```xml
<project>
  <groupId>com.pelagisflow</groupId>
  <artifactId>transformations</artifactId>
  <version>1.0.0</version>

  <dependencies>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-core_2.12</artifactId>
      <version>3.3.0</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.spark</groupId>
      <artifactId>spark-sql_2.12</artifactId>
      <version>3.3.0</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
</project>
```

Build:
```bash
mvn clean package
```

## Deployment

1. **Copy JAR to this directory**:
   ```bash
   cp target/scala-2.12/my-transformation.jar transformations/scala/
   ```

2. **Register in transformation registry**:

   Edit `transformations/registry/scala_transformations.yaml`:

   ```yaml
   - name: my_transformation_v1
     type: scala
     version: "1.0.0"
     description: My custom Scala transformation
     class_name: com.pelagisflow.transformations.MyTransformation
     jar_path: transformations/scala/my-transformation.jar
     author: Your Name
     tags:
       - custom
       - scala
   ```

3. **Use in data contract**:

   ```yaml
   customProperties:
     transformationName: my_transformation_v1
   ```

   Or directly:

   ```yaml
   customProperties:
     transformationType: scala
     transformationClass: com.pelagisflow.transformations.MyTransformation
     transformationJar: transformations/scala/my-transformation.jar
   ```

## Advanced Features

### Accessing Configuration

```scala
def transform(
  spark: SparkSession,
  inputDF: Option[DataFrame],
  config: Map[String, Any]
): DataFrame = {

  // Get config values with defaults
  val catalog = config.getOrElse("source_catalog", "bronze").toString
  val threshold = config.getOrElse("threshold", 100).toString.toInt

  // Use config in transformation
  spark.table(s"$catalog.orders")
    .filter(s"amount > $threshold")
}
```

### Error Handling

```scala
def transform(
  spark: SparkSession,
  inputDF: Option[DataFrame],
  config: Map[String, Any]
): DataFrame = {

  try {
    // Transformation logic
    val df = spark.table("source_table")
    df.filter("valid_record = true")
  } catch {
    case e: Exception =>
      throw new RuntimeException(s"Transformation failed: ${e.getMessage}", e)
  }
}
```

### Using Multiple Input Sources

```scala
def transform(
  spark: SparkSession,
  inputDF: Option[DataFrame],
  config: Map[String, Any]
): DataFrame = {

  val customers = spark.table("bronze.customers")
  val orders = spark.table("bronze.orders")
  val products = spark.table("bronze.products")

  // Complex multi-table transformation
  orders
    .join(customers, Seq("customer_id"), "left")
    .join(products, Seq("product_id"), "left")
    .groupBy("customer_id", "product_category")
    .agg(
      count("order_id").as("order_count"),
      sum("order_total").as("total_spent")
    )
}
```

## Performance Optimization

### Broadcast Joins

```scala
import org.apache.spark.sql.functions.broadcast

val smallDim = spark.table("dim_small")
val largeFact = spark.table("fact_large")

largeFact.join(broadcast(smallDim), Seq("id"))
```

### Partitioning

```scala
df.repartition(200, col("partition_key"))
  .write
  .mode("overwrite")
  .partitionBy("year", "month")
  .saveAsTable("target_table")
```

### Caching

```scala
val intermediateDf = df.filter("status = 'active'").cache()
// Use intermediateDf multiple times
intermediateDf.count()
```

## Testing

Create unit tests using ScalaTest:

```scala
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class MyTransformationTest extends AnyFunSuite {

  test("transformation produces expected output") {
    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val transformation = new MyTransformation()
    val result = transformation.transform(
      spark,
      None,
      Map("threshold" -> 100)
    )

    assert(result.count() > 0)
  }
}
```

## Best Practices

1. **Always validate inputs**: Check for null DataFrames and invalid config
2. **Use type-safe config**: Parse config values with proper error handling
3. **Log important operations**: Use Spark's built-in logging
4. **Optimize joins**: Use broadcast for small tables
5. **Handle nulls**: Use Option types and null-safe operations
6. **Test thoroughly**: Write unit tests and integration tests
7. **Document config schema**: Specify all configuration parameters in registry
8. **Version your JARs**: Use semantic versioning for transformation JARs
