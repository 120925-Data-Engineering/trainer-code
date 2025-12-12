# SparkSession vs. SparkContext

## Learning Objectives
- Understand the differences between SparkSession and SparkContext
- Know when to use each in your applications
- Learn how to migrate from SparkContext-based code to SparkSession

## Why This Matters

If you have followed Week 1's RDD content, you are already familiar with SparkContext. It was the original entry point to Spark and is still the underlying engine powering all Spark operations. However, as Spark evolved to include structured data processing (Spark SQL), a new abstraction was needed.

Understanding the relationship between SparkSession and SparkContext is crucial because:
- You will encounter legacy code that uses SparkContext directly
- Some advanced operations still require SparkContext access
- Interview questions commonly ask about this distinction
- Knowing both helps you choose the right tool for the job

This knowledge directly supports our Weekly Epic of *Mastering Spark SQL and DataFrames* by clarifying where DataFrame abstractions sit relative to the core Spark engine you learned in Week 1.

## The Concept

### Historical Evolution

```
Spark 1.x Era                    Spark 2.x+ Era
+---------------+                +------------------+
| SparkContext  | <-- RDDs       |   SparkSession   |
+---------------+                +------------------+
       +                                  |
+---------------+                +--------+--------+
|  SQLContext   | <-- SQL               Uses
+---------------+                         |
       +                                  v
+---------------+                +------------------+
| HiveContext   | <-- Hive       |  SparkContext    |
+---------------+                +------------------+
```

### SparkContext: The Low-Level Engine

**SparkContext** (often called `sc`) is:
- The original entry point to Spark (since version 1.0)
- The connection to the Spark cluster
- Required for creating RDDs
- Responsible for coordinating job execution

```python
from pyspark import SparkContext

sc = SparkContext("local", "MyApp")
rdd = sc.parallelize([1, 2, 3, 4, 5])
```

### SparkSession: The Unified Interface

**SparkSession** (often called `spark`) is:
- The unified entry point introduced in Spark 2.0
- Built on top of SparkContext
- Required for DataFrame and SQL operations
- The recommended way to start Spark applications

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
```

### Key Differences

| Aspect | SparkContext | SparkSession |
|--------|--------------|--------------|
| Introduced | Spark 1.0 | Spark 2.0 |
| Primary Use | RDD operations | DataFrame/SQL operations |
| Creation | `SparkContext()` | `SparkSession.builder.getOrCreate()` |
| Singleton | One per JVM | Multiple possible (shared Context) |
| Configuration | At creation only | At creation and runtime |
| SQL Support | No (needs SQLContext) | Yes (built-in) |
| Hive Support | No (needs HiveContext) | Yes (with `enableHiveSupport()`) |

### The Relationship

SparkSession wraps SparkContext. When you create a SparkSession, it either:
1. Creates a new SparkContext internally, or
2. Uses an existing SparkContext if one exists

You can always access the underlying SparkContext from a SparkSession:

```python
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext  # Access the underlying SparkContext
```

### When to Use Each

**Use SparkSession when:**
- Starting a new Spark SQL or DataFrame application
- Reading structured data (CSV, JSON, Parquet)
- Running SQL queries
- Working with the Catalog (tables, views)
- Modern Spark development (Spark 2.0+)

**Access SparkContext when:**
- Working directly with RDDs
- Using broadcast variables or accumulators
- Accessing low-level Spark functionality
- Interfacing with legacy code
- Needing cluster resource information

### Common Pattern: Best of Both Worlds

```python
from pyspark.sql import SparkSession

# Create SparkSession (recommended entry point)
spark = SparkSession.builder \
    .appName("HybridApp") \
    .getOrCreate()

# For DataFrame operations, use spark
df = spark.read.csv("data.csv", header=True)

# For RDD operations, access SparkContext
sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3])

# You can convert between RDD and DataFrame
df_from_rdd = rdd.map(lambda x: (x,)).toDF(["value"])
rdd_from_df = df.rdd
```

## Code Example

### Migrating from SparkContext to SparkSession

**Old Style (SparkContext-based)**
```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# Old way: Create SparkContext, then SQLContext
conf = SparkConf().setAppName("OldStyle").setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# Load data
df = sqlContext.read.json("data.json")
df.registerTempTable("data")  # Old method name
result = sqlContext.sql("SELECT * FROM data WHERE value > 10")

sc.stop()
```

**New Style (SparkSession-based)**
```python
from pyspark.sql import SparkSession

# New way: Just create SparkSession
spark = SparkSession.builder \
    .appName("NewStyle") \
    .master("local[*]") \
    .getOrCreate()

# Load data with simpler API
df = spark.read.json("data.json")
df.createOrReplaceTempView("data")  # New method name
result = spark.sql("SELECT * FROM data WHERE value > 10")

spark.stop()
```

### Accessing Both in the Same Application

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DualAccess") \
    .getOrCreate()

# Get SparkContext reference
sc = spark.sparkContext

# Demonstrate both working together
print("Using SparkSession:")
df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "letter"])
df.show()

print("\nUsing SparkContext:")
rdd = sc.parallelize([10, 20, 30])
print(f"RDD sum: {rdd.sum()}")

# Convert RDD to DataFrame
df2 = spark.createDataFrame(rdd.map(lambda x: (x, x*2)), ["original", "doubled"])
df2.show()

# Access cluster info via SparkContext
print(f"\nSpark Version: {sc.version}")
print(f"Master: {sc.master}")
print(f"Default Parallelism: {sc.defaultParallelism}")

spark.stop()
```

### Checking What You Have

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ContextCheck").getOrCreate()
sc = spark.sparkContext

# SparkSession information
print("SparkSession Properties:")
print(f"  Version: {spark.version}")
print(f"  Has SparkContext: {spark.sparkContext is not None}")

# SparkContext information  
print("\nSparkContext Properties:")
print(f"  App Name: {sc.appName}")
print(f"  App ID: {sc.applicationId}")
print(f"  Master: {sc.master}")
print(f"  UI Web URL: {sc.uiWebUrl}")

spark.stop()
```

## Summary

- **SparkContext** is the original, low-level entry point for RDD operations and cluster management
- **SparkSession** is the modern, unified entry point that encompasses SparkContext plus SQL/DataFrame capabilities
- SparkSession internally manages a SparkContext, accessible via `spark.sparkContext`
- For new applications, always start with SparkSession and access SparkContext only when needed for RDD operations
- Understanding both is important for working with legacy code and for interview preparation

## Additional Resources

- [PySpark SparkContext API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.html)
- [Migrating to SparkSession (Databricks)](https://docs.databricks.com/en/getting-started/spark-session.html)
- [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
