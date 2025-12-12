# DataFrame Operations

## Learning Objectives
- Master essential DataFrame operations: select, filter, withColumn, and show
- Understand how to inspect DataFrame structure and contents
- Learn the difference between transformations and actions in the DataFrame API

## Why This Matters

DataFrames are the workhorse of modern Spark applications. While RDDs give you low-level control, DataFrames provide a structured, optimized interface for working with tabular data. In most production data engineering pipelines, you will spend the majority of your time manipulating DataFrames.

As we continue our Weekly Epic of *Mastering Spark SQL and DataFrames*, today marks the transition from understanding the entry points (SparkSession) to actively working with data. The operations you learn today will be used in virtually every Spark SQL application you build.

## The Concept

### What is a DataFrame?

A DataFrame is a distributed collection of data organized into named columns, conceptually equivalent to:
- A table in a relational database
- A pandas DataFrame in Python
- An Excel spreadsheet with headers

The key difference is that Spark DataFrames are:
- **Distributed**: Data is spread across multiple nodes in a cluster
- **Immutable**: Operations create new DataFrames rather than modifying existing ones
- **Lazily evaluated**: Transformations are not executed until an action is called
- **Optimized**: The Catalyst optimizer automatically improves your queries

### Transformations vs. Actions

| Type | Description | Examples | Execution |
|------|-------------|----------|-----------|
| **Transformation** | Creates a new DataFrame from an existing one | `select`, `filter`, `withColumn`, `join` | Lazy (deferred) |
| **Action** | Returns a result to the driver or writes data | `show`, `count`, `collect`, `write` | Immediate |

### Core DataFrame Operations

#### 1. Viewing Data: `show()`

The `show()` action displays DataFrame contents:

```python
df.show()          # Default: 20 rows, truncated columns
df.show(5)         # Show 5 rows
df.show(5, False)  # Show 5 rows, do not truncate columns
df.show(5, 100)    # Show 5 rows, truncate at 100 characters
```

#### 2. Column Selection: `select()`

Select specific columns to create a new DataFrame:

```python
# Select single column
df.select("name")

# Select multiple columns
df.select("name", "age", "department")

# Select with column objects
from pyspark.sql.functions import col
df.select(col("name"), col("age"))

# Select all columns
df.select("*")
```

#### 3. Filtering Rows: `filter()` / `where()`

Both methods are equivalent for filtering:

```python
# Using filter with string expression
df.filter("age > 30")

# Using filter with column condition
df.filter(df.age > 30)
df.filter(col("age") > 30)

# Using where (alias for filter)
df.where("age > 30")

# Multiple conditions
df.filter((df.age > 30) & (df.department == "Engineering"))
df.filter((df.age > 30) | (df.age < 20))
```

#### 4. Adding/Modifying Columns: `withColumn()`

Create new columns or modify existing ones:

```python
# Add a new column
df.withColumn("senior", df.age > 40)

# Modify an existing column
df.withColumn("age", df.age + 1)

# Add column with calculation
df.withColumn("annual_salary", df.monthly_salary * 12)
```

### Schema Inspection Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `df.columns` | List[str] | Column names |
| `df.dtypes` | List[Tuple] | Column names and types |
| `df.schema` | StructType | Full schema object |
| `df.printSchema()` | None | Pretty-prints schema |
| `df.describe()` | DataFrame | Statistical summary |

## Code Example

### Complete DataFrame Operations Demo

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, length

# Create SparkSession
spark = SparkSession.builder \
    .appName("DataFrame Operations") \
    .getOrCreate()

# Create sample DataFrame
data = [
    ("Alice", 34, "Engineering", 75000),
    ("Bob", 45, "Marketing", 65000),
    ("Charlie", 29, "Engineering", 80000),
    ("Diana", 31, "Sales", 55000),
    ("Eve", 38, "Marketing", 70000)
]

df = spark.createDataFrame(data, ["name", "age", "department", "salary"])

# View the data
print("Original DataFrame:")
df.show()

# Inspect schema
print("Schema:")
df.printSchema()

print("Columns:", df.columns)
print("Data types:", df.dtypes)

# Select specific columns
print("\nSelect name and salary:")
df.select("name", "salary").show()

# Filter rows
print("\nEmployees older than 30:")
df.filter(df.age > 30).show()

# Filter with multiple conditions
print("\nEngineers earning more than 70000:")
df.filter((df.department == "Engineering") & (df.salary > 70000)).show()

# Add new columns
print("\nWith new columns:")
df.withColumn("name_upper", upper(col("name"))) \
  .withColumn("name_length", length(col("name"))) \
  .show()

# Combine operations (chaining)
print("\nChained operations:")
df.select("name", "department", "salary") \
  .filter(df.salary >= 65000) \
  .withColumn("bonus", col("salary") * 0.1) \
  .show()

# Statistical summary
print("\nStatistical summary:")
df.describe().show()

spark.stop()
```

### Common Patterns

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark = SparkSession.builder.appName("Patterns").getOrCreate()

# Sample data
data = [("Alice", 34, None), ("Bob", None, "NY"), ("Charlie", 29, "CA")]
df = spark.createDataFrame(data, ["name", "age", "state"])

# Handle nulls with filter
print("Non-null ages:")
df.filter(df.age.isNotNull()).show()

# Conditional column with when/otherwise
print("\nWith age category:")
df.withColumn("age_group",
    when(col("age") < 30, "Young")
    .when(col("age") < 40, "Middle")
    .otherwise("Senior")
).show()

# Add constant column
print("\nWith constant column:")
df.withColumn("country", lit("USA")).show()

# Rename column using alias in select
print("\nWith renamed column:")
df.select(
    col("name").alias("employee_name"),
    col("age"),
    col("state").alias("location")
).show()

spark.stop()
```

## Summary

- **DataFrames** are distributed, immutable collections of structured data with named columns
- **Transformations** (select, filter, withColumn) are lazy and create new DataFrames
- **Actions** (show, count, collect) trigger execution and return results
- Use `show()` to display data, with optional row count and truncation settings
- Use `select()` to choose specific columns
- Use `filter()` or `where()` to filter rows based on conditions
- Use `withColumn()` to add or modify columns
- Chain operations together for readable, efficient data transformations

## Additional Resources

- [PySpark DataFrame API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Spark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [DataFrame Operations Guide](https://spark.apache.org/docs/latest/sql-getting-started.html#dataframe-operations)
