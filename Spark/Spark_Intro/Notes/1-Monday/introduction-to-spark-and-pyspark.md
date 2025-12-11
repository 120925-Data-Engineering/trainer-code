# Introduction to Spark and PySpark

## Learning Objectives
- Understand what Apache Spark is and its core purpose
- Explain why PySpark exists and its advantages
- Identify basic PySpark concepts and terminology
- Recognize the benefits of using Python with Spark

## Why This Matters
Apache Spark has become the de facto standard for big data processing. PySpark brings Spark's power to Python developers, combining the simplicity of Python with the distributed computing capabilities of Spark. As a data engineer, PySpark will be one of your most frequently used tools for building ETL pipelines, data transformations, and analytics at scale.

## The Concept

### What is Apache Spark?

Apache Spark is an open-source, distributed computing system designed for fast, large-scale data processing. Key characteristics include:

**Distributed Processing:**
- Splits data across multiple machines in a cluster
- Processes partitions in parallel
- Scales horizontally by adding more nodes

**In-Memory Computing:**
- Keeps data in RAM across operations
- Dramatically reduces I/O overhead
- Enables interactive query speeds

**Fault Tolerance:**
- Automatically recovers from node failures
- Uses lineage information to rebuild lost partitions
- No need for manual checkpoint management

**Unified Engine:**
- Same framework for batch, streaming, ML, and graph processing
- Write once, use across different workloads

### What is PySpark?

PySpark is the Python API for Apache Spark. It allows you to write Spark applications using Python instead of Scala or Java.

**Why PySpark?**

1. **Python's Popularity:** Python is the most popular language for data science and engineering
2. **Familiar Syntax:** Leverage existing Python knowledge
3. **Rich Ecosystem:** Integrate with NumPy, Pandas, and other Python libraries
4. **Rapid Development:** Less verbose than Scala/Java equivalents
5. **Interactive Development:** Works seamlessly with Jupyter notebooks

### How PySpark Works

```
+-------------------+
|  Your Python Code |
+-------------------+
         |
         v
+-------------------+
|   PySpark API     |  <-- Python wrapper
+-------------------+
         |
         v
+-------------------+
|    Py4J Bridge    |  <-- Python-to-Java communication
+-------------------+
         |
         v
+-------------------+
|   Spark JVM       |  <-- Actual execution
+-------------------+
         |
         v
+-------------------+
|  Cluster Nodes    |  <-- Distributed processing
+-------------------+
```

PySpark translates your Python code into Java calls via Py4J. The actual distributed computation happens on the JVM, giving you Python's ease of use with Spark's performance.

### Core PySpark Concepts

**SparkContext (sc):**
- The entry point for Spark functionality
- Connects your application to a Spark cluster
- Used to create RDDs

**SparkSession (spark):**
- Unified entry point (Spark 2.0+)
- Combines SparkContext, SQLContext, and HiveContext
- Preferred way to initialize Spark applications

**RDD (Resilient Distributed Dataset):**
- Fundamental data structure in Spark
- Immutable, distributed collection of objects
- Supports parallel operations

**DataFrame:**
- Distributed collection with named columns
- Similar to a database table or Pandas DataFrame
- Optimized execution through Catalyst optimizer

**Transformations vs Actions:**
- **Transformations:** Define operations (lazy, not executed immediately)
- **Actions:** Trigger computation and return results

### PySpark Application Structure

A typical PySpark application follows this pattern:

```python
# 1. Import necessary modules
from pyspark.sql import SparkSession

# 2. Create SparkSession
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

# 3. Load data
df = spark.read.csv("data.csv", header=True)

# 4. Transform data (transformations are lazy)
result = df.filter(df.age > 21).select("name", "age")

# 5. Trigger computation (action)
result.show()

# 6. Clean up
spark.stop()
```

## Code Example

Here is a complete example demonstrating basic PySpark operations:

```python
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IntroductionDemo") \
    .master("local[*]") \
    .getOrCreate()

# Access the underlying SparkContext
sc = spark.sparkContext
print(f"Spark Version: {spark.version}")
print(f"App Name: {sc.appName}")

# Create an RDD from a Python list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# Apply transformations
evens = numbers.filter(lambda x: x % 2 == 0)
squared = evens.map(lambda x: x ** 2)

# Trigger action to see results
print("Even numbers squared:", squared.collect())
# Output: [4, 16, 36, 64, 100]

# Create a DataFrame
employees = [
    ("Alice", "Engineering", 75000),
    ("Bob", "Engineering", 80000),
    ("Charlie", "Sales", 60000),
    ("Diana", "Sales", 65000)
]

df = spark.createDataFrame(employees, ["name", "department", "salary"])

# Show the DataFrame
df.show()

# Perform aggregation
df.groupBy("department").avg("salary").show()

# Stop the session
spark.stop()
```

**Output:**
```
Spark Version: 3.5.0
App Name: IntroductionDemo
Even numbers squared: [4, 16, 36, 64, 100]

+-------+-----------+------+
|   name| department|salary|
+-------+-----------+------+
|  Alice|Engineering| 75000|
|    Bob|Engineering| 80000|
|Charlie|      Sales| 60000|
|  Diana|      Sales| 65000|
+-------+-----------+------+

+-----------+-----------+
| department|avg(salary)|
+-----------+-----------+
|Engineering|    77500.0|
|      Sales|    62500.0|
+-----------+-----------+
```

## Summary
- Apache Spark is a distributed computing engine for large-scale data processing
- PySpark is the Python API that makes Spark accessible to Python developers
- SparkSession is the unified entry point for Spark applications
- RDDs and DataFrames are the core data abstractions
- Transformations are lazy; Actions trigger actual computation
- PySpark combines Python's simplicity with Spark's distributed power

## Additional Resources
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [PySpark Getting Started Guide](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)
- [Databricks PySpark Introduction](https://docs.databricks.com/en/pyspark/index.html)
