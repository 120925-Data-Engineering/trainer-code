# Spark Session Entrypoint

## Learning Objectives
- Understand SparkSession as the unified entry point for Spark applications
- Learn why SparkSession was introduced and what problems it solves
- Recognize how SparkSession simplifies Spark application development

## Why This Matters

Before Spark 2.0, developers had to manage multiple entry points depending on their use case: `SparkContext` for core RDD operations, `SQLContext` for Spark SQL, `HiveContext` for Hive integration, and `StreamingContext` for streaming. This fragmentation made code more complex and prone to configuration inconsistencies.

**SparkSession** was introduced as the single, unified entry point that consolidates all these functionalities. As you progress through this week's journey of *Mastering Spark SQL and DataFrames*, SparkSession will be your primary interface for creating DataFrames, running SQL queries, and accessing Spark functionality.

In modern data engineering workflows, understanding SparkSession is essential because:
- Every Spark SQL application starts with a SparkSession
- It manages the lifecycle of your Spark application
- It provides access to all DataFrame and SQL operations
- Most code examples, documentation, and tutorials assume SparkSession usage

## The Concept

### What is SparkSession?

SparkSession is the entry point to programming with DataFrame and Dataset APIs. Think of it as the "front door" to Spark: you create a SparkSession at the start of your application, and through it, you access all of Spark's capabilities for structured data processing.

```
+--------------------------------+
|        SparkSession            |
|  (Unified Entry Point)         |
+--------------------------------+
        |           |          |
        v           v          v
   DataFrame    SQL API    Catalog
     API       (queries)   (metadata)
        \          |          /
         \         |         /
          v        v        v
       +---------------------+
       |    SparkContext     |
       | (Core RDD Engine)   |
       +---------------------+
```

### The Builder Pattern

SparkSession uses the **builder pattern** to create instances. This is a design pattern where you chain configuration methods together before calling a final `getOrCreate()` method:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApplication") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

The builder pattern provides several benefits:
1. **Fluent interface**: Chain multiple configurations in a readable way
2. **Flexibility**: Add only the configurations you need
3. **Reuse**: `getOrCreate()` returns an existing session if one exists with compatible settings

### Key Components Accessible Through SparkSession

| Component | Access Method | Purpose |
|-----------|---------------|---------|
| SparkContext | `spark.sparkContext` | Access RDD operations and low-level Spark functionality |
| SQL Execution | `spark.sql()` | Run SQL queries on registered tables/views |
| DataFrame Creation | `spark.read`, `spark.createDataFrame()` | Load data and create DataFrames |
| Catalog | `spark.catalog` | Access metadata about tables, databases, and functions |
| Configuration | `spark.conf` | Get and set Spark configuration properties |
| UDF Registration | `spark.udf.register()` | Register user-defined functions for SQL |

### Why "getOrCreate()"?

The `getOrCreate()` method is intelligent:
- If no SparkSession exists, it creates a new one with your specified configuration
- If a SparkSession already exists, it returns that existing session
- This prevents accidental creation of multiple sessions in a single application

This is particularly useful in notebooks (like Jupyter or Databricks) where cells may be run multiple times.

## Code Example

### Basic SparkSession Creation

```python
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week2Demo") \
    .getOrCreate()

# Verify the session is active
print(f"Spark Version: {spark.version}")
print(f"App Name: {spark.sparkContext.appName}")

# Create a simple DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

# Run SQL on the DataFrame
df.createOrReplaceTempView("people")
result = spark.sql("SELECT name, age FROM people WHERE age > 30")
result.show()

# Stop the session when done
spark.stop()
```

### Accessing Underlying Components

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ComponentAccess") \
    .getOrCreate()

# Access SparkContext (for RDD operations if needed)
sc = spark.sparkContext
print(f"SparkContext version: {sc.version}")

# Access the catalog (metadata about tables)
print("Current database:", spark.catalog.currentDatabase())

# Access configuration
print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
```

### Configuration at Creation Time

```python
from pyspark.sql import SparkSession

# Configure session with multiple options
spark = SparkSession.builder \
    .appName("ConfiguredSession") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

# The session now has all specified configurations
```

## Summary

- **SparkSession** is the unified entry point for all Spark SQL functionality, introduced in Spark 2.0
- It uses the **builder pattern** for flexible, readable configuration
- The `getOrCreate()` method ensures you do not accidentally create multiple sessions
- Through SparkSession, you can access DataFrames, SQL execution, the Catalog, and the underlying SparkContext
- SparkSession consolidates the functionality previously spread across SQLContext, HiveContext, and SparkContext

## Additional Resources

- [PySpark SparkSession API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html)
- [Databricks: Introduction to SparkSession](https://docs.databricks.com/en/getting-started/spark-session.html)
- [Spark SQL Programming Guide: Getting Started](https://spark.apache.org/docs/latest/sql-getting-started.html)
