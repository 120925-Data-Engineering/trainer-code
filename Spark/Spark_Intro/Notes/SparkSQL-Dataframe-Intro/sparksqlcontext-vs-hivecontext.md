# SQLContext vs. HiveContext

## Learning Objectives
- Understand the historical role of SQLContext and HiveContext in Spark
- Learn how SparkSession unifies these legacy contexts
- Know how to handle legacy code that uses these older APIs

## Why This Matters

When reading older Spark documentation, tutorials, or existing codebases, you will encounter references to `SQLContext` and `HiveContext`. These were the primary interfaces for Spark SQL before SparkSession was introduced in Spark 2.0.

Understanding these legacy contexts helps you:
- Maintain and migrate older Spark applications
- Understand Stack Overflow answers and older tutorials
- Appreciate why SparkSession was designed the way it was
- Be prepared for interview questions about Spark's evolution

This completes our Monday foundation for *Mastering Spark SQL and DataFrames* by ensuring you understand both the current best practices and the historical context.

## The Concept

### The Pre-Spark 2.0 World

Before Spark 2.0, the entry points were fragmented:

```
+----------------+     +----------------+     +----------------+
|  SparkContext  |     |   SQLContext   |     |  HiveContext   |
|  (Core RDDs)   |     | (Basic SQL)    |     | (Hive + SQL)   |
+----------------+     +----------------+     +----------------+
        |                     |                      |
        |                     |                      |
        v                     v                      v
  RDD Operations      Basic DataFrame        Full SQL + Hive
                       Operations            Support
```

### SQLContext

**SQLContext** was introduced in Spark 1.0 as the entry point for Spark SQL operations. It provided:
- The ability to create DataFrames
- Basic SQL query execution
- Reading from data sources (JSON, Parquet)
- UDF registration

```python
# Legacy code (Spark 1.x style)
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext("local", "SQLContextExample")
sqlContext = SQLContext(sc)

df = sqlContext.read.json("data.json")
df.registerTempTable("data")
result = sqlContext.sql("SELECT * FROM data")
```

**Limitations of SQLContext:**
- No Hive support (no HiveQL, no Hive metastore access)
- Limited window functions
- No SerDe support
- Required separate SparkContext creation

### HiveContext

**HiveContext** extended SQLContext to provide full Hive compatibility:
- HiveQL support (Hive's SQL dialect)
- Access to Hive metastore (tables, partitions)
- Hive SerDe (serialization/deserialization)
- Window functions
- Full subquery support

```python
# Legacy code with Hive support
from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext("local", "HiveContextExample")
hiveContext = HiveContext(sc)

# Could access Hive tables
hiveContext.sql("SELECT * FROM hive_database.hive_table")
```

**Limitations of HiveContext:**
- Required Hive dependencies
- Slower initialization due to Hive setup
- Still required separate SparkContext

### How SparkSession Unifies Everything

SparkSession consolidates all functionality:

```
   Spark 2.0+ Architecture
   
+--------------------------------+
|        SparkSession            |
|  .builder.getOrCreate()        |
+--------------------------------+
|  * DataFrame API (SQLContext)  |
|  * SQL Queries (SQLContext)    |
|  * Hive Support (HiveContext)  |
|  * Catalog API                 |
|  * Configuration               |
+--------------------------------+
              |
              v
+--------------------------------+
|        SparkContext            |
|    (managed internally)        |
+--------------------------------+
```

### Enabling Hive Support in SparkSession

To get HiveContext functionality in SparkSession, use `enableHiveSupport()`:

```python
from pyspark.sql import SparkSession

# Without Hive support (equivalent to old SQLContext)
spark_basic = SparkSession.builder \
    .appName("BasicSQL") \
    .getOrCreate()

# With Hive support (equivalent to old HiveContext)
spark_hive = SparkSession.builder \
    .appName("WithHive") \
    .enableHiveSupport() \
    .getOrCreate()
```

### Compatibility Layer

For backward compatibility, you can still access SQLContext from SparkSession:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# These are equivalent in Spark 2.0+
df1 = spark.read.json("data.json")
df2 = spark.sql("SELECT * FROM table")

# Legacy access (deprecated but works)
# sqlContext = spark._wrapped  # Internal, not recommended
```

## Code Example

### Migrating SQLContext Code

**Before (Spark 1.x)**
```python
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

# Create SparkContext first
conf = SparkConf().setAppName("LegacyApp").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Then create SQLContext
sqlContext = SQLContext(sc)

# Read data
df = sqlContext.read.json("employees.json")

# Register as temp table (old method)
df.registerTempTable("employees")

# Query
result = sqlContext.sql("""
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
""")

result.show()
sc.stop()
```

**After (Spark 2.0+)**
```python
from pyspark.sql import SparkSession

# SparkSession handles everything
spark = SparkSession.builder \
    .appName("ModernApp") \
    .master("local[*]") \
    .getOrCreate()

# Read data (same API, cleaner context)
df = spark.read.json("employees.json")

# Register as temp view (new method)
df.createOrReplaceTempView("employees")

# Query using SparkSession
result = spark.sql("""
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
""")

result.show()
spark.stop()
```

### Migrating HiveContext Code

**Before (Spark 1.x with Hive)**
```python
from pyspark import SparkContext
from pyspark.sql import HiveContext

sc = SparkContext("local", "HiveApp")
hiveContext = HiveContext(sc)

# Create Hive table
hiveContext.sql("""
    CREATE TABLE IF NOT EXISTS sales (
        product STRING,
        amount DOUBLE
    )
    STORED AS PARQUET
""")

# Query Hive table
result = hiveContext.sql("SELECT * FROM sales")
result.show()

sc.stop()
```

**After (Spark 2.0+ with Hive)**
```python
from pyspark.sql import SparkSession

# Enable Hive support
spark = SparkSession.builder \
    .appName("HiveApp") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

# Same Hive SQL, simpler setup
spark.sql("""
    CREATE TABLE IF NOT EXISTS sales (
        product STRING,
        amount DOUBLE
    )
    STORED AS PARQUET
""")

result = spark.sql("SELECT * FROM sales")
result.show()

spark.stop()
```

### Checking Hive Support Status

```python
from pyspark.sql import SparkSession

# Create session with Hive support
spark = SparkSession.builder \
    .appName("HiveCheck") \
    .enableHiveSupport() \
    .getOrCreate()

# Check if Hive is enabled
catalog = spark.catalog

# List databases (requires Hive)
try:
    databases = catalog.listDatabases()
    print("Hive is enabled. Available databases:")
    for db in databases:
        print(f"  - {db.name}")
except Exception as e:
    print(f"Hive may not be available: {e}")

# Check current database
print(f"\nCurrent database: {catalog.currentDatabase()}")

spark.stop()
```

## Summary

- **SQLContext** (Spark 1.x) provided basic DataFrame and SQL functionality
- **HiveContext** (Spark 1.x) extended SQLContext with full Hive support
- **SparkSession** (Spark 2.0+) unifies both contexts into a single, simpler entry point
- Use `enableHiveSupport()` on SparkSession to get HiveContext functionality
- When migrating legacy code, replace SQLContext/HiveContext with SparkSession
- Method changes: `registerTempTable()` becomes `createOrReplaceTempView()`

## Additional Resources

- [Spark SQL Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html)
- [Hive Tables in Spark](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)
- [PySpark SQL Module Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)
