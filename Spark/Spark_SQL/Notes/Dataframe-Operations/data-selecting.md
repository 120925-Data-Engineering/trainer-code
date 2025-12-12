# Data Selecting

## Learning Objectives

- Master column selection techniques in Spark DataFrames
- Learn the differences between select(), selectExpr(), and column expressions
- Understand when to use each approach for optimal readability and performance

## Why This Matters

Selecting the right columns is the first step in most DataFrame operations. Efficient column selection:

- Reduces memory usage by reading only needed data
- Improves query performance through column pruning
- Makes code more readable and maintainable
- Enables complex transformations in a single step

Understanding the full range of selection techniques helps you write cleaner, more efficient PySpark code as part of our Weekly Epic.

## The Concept

### Column Selection Methods

| Method | Use Case | Example |
|--------|----------|---------|
| `select("col")` | Simple column selection | `df.select("name", "age")` |
| `select(col(...))` | Column expressions | `df.select(col("name").alias("n"))` |
| `selectExpr("expr")` | SQL expressions | `df.selectExpr("name", "age * 2 as double_age")` |
| `df["col"]` | Column access | `df[["name", "age"]]` |
| `df.col` | Attribute access | `df.name` (returns Column object) |

### Methods Comparison

```python
# All of these select the same columns, different styles:

df.select("name", "age")
df.select(col("name"), col("age"))
df.select(df.name, df.age)
df.select(df["name"], df["age"])

# For expressions, choose based on complexity:
df.select(col("age") * 2)           # Simple expression
df.selectExpr("age * 2 as new_age") # SQL style
```

### Column Pruning

Spark's optimizer uses **column pruning** to read only necessary columns from source files:

```
Query: df.select("name", "age")

Physical Plan:
+- FileScan parquet [name, age]  <- Only reads 2 columns!
   Location: /data/users.parquet
   PushedFilters: []
   ReadSchema: struct<name:string,age:int>
```

This is automatic, just select only what you need.

## Code Example

### Basic Column Selection

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Data Selecting").getOrCreate()

# Sample DataFrame
data = [
    (1, "Alice", 34, "Engineering", 75000),
    (2, "Bob", 45, "Marketing", 65000),
    (3, "Charlie", 29, "Engineering", 80000)
]

df = spark.createDataFrame(data, ["id", "name", "age", "department", "salary"])

print("All columns:")
df.show()

# Select by column name strings
print("Select by name:")
df.select("name", "age").show()

# Select using col() function
print("Select using col():")
df.select(col("name"), col("age")).show()

# Select using DataFrame attribute
print("Select using attribute:")
df.select(df.name, df.age).show()

# Select using bracket notation
print("Select using brackets:")
df.select(df["name"], df["age"]).show()

# Select all columns
print("Select all:")
df.select("*").show()

# Select all except some
cols_to_keep = [c for c in df.columns if c not in ["salary"]]
df.select(cols_to_keep).show()
```

### Using selectExpr()

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("selectExpr").getOrCreate()

data = [
    ("Alice", 34, 75000),
    ("Bob", 45, 65000),
    ("Charlie", 29, 80000)
]

df = spark.createDataFrame(data, ["name", "age", "salary"])

# selectExpr allows SQL expressions as strings
print("Using selectExpr:")
df.selectExpr(
    "name",
    "age",
    "salary",
    "salary * 0.1 as bonus",
    "age > 30 as is_senior",
    "UPPER(name) as name_upper"
).show()

# Complex expressions
print("Complex expressions:")
df.selectExpr(
    "name",
    "CASE WHEN age < 30 THEN 'Young' WHEN age < 40 THEN 'Mid' ELSE 'Senior' END as age_group",
    "ROUND(salary / 12, 2) as monthly_salary"
).show()

# Aggregations in selectExpr (when no groupBy needed)
print("Aggregation:")
df.selectExpr(
    "COUNT(*) as total_employees",
    "AVG(salary) as avg_salary",
    "MAX(age) as max_age"
).show()
```

### Column Expressions with select()

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, upper, lower, 
    concat, substring, length,
    round as spark_round
)

spark = SparkSession.builder.appName("Column Expressions").getOrCreate()

data = [
    ("Alice", 34, 75000),
    ("Bob", 45, 65000),
    ("Charlie", 29, 80000)
]

df = spark.createDataFrame(data, ["name", "age", "salary"])

# String functions
print("String operations:")
df.select(
    col("name"),
    upper(col("name")).alias("name_upper"),
    lower(col("name")).alias("name_lower"),
    length(col("name")).alias("name_length"),
    substring(col("name"), 1, 3).alias("name_short")
).show()

# Arithmetic operations
print("Arithmetic operations:")
df.select(
    col("name"),
    col("salary"),
    (col("salary") * 0.1).alias("bonus"),
    spark_round(col("salary") / 12, 2).alias("monthly"),
    (col("salary") + 5000).alias("with_raise")
).show()

# Conditional logic
print("Conditional logic:")
df.select(
    col("name"),
    col("age"),
    when(col("age") < 30, "Young")
        .when(col("age") < 40, "Mid")
        .otherwise("Senior")
        .alias("age_category")
).show()

# Adding literals
print("With literal values:")
df.select(
    col("name"),
    lit("USA").alias("country"),
    lit(2024).alias("year"),
    (col("salary") > 70000).alias("high_earner")
).show()
```

### Alternative Selection Patterns

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Selection Patterns").getOrCreate()

data = [
    ("Alice", 34, 75000, "Engineering"),
    ("Bob", 45, 65000, "Marketing")
]

df = spark.createDataFrame(data, ["name", "age", "salary", "department"])

# Pattern 1: Select columns dynamically
numeric_cols = [f.name for f in df.schema.fields if str(f.dataType) in ["IntegerType()", "LongType()"]]
print(f"Numeric columns: {numeric_cols}")
df.select(numeric_cols).show()

# Pattern 2: Select columns matching pattern
cols_with_a = [c for c in df.columns if 'a' in c.lower()]
print(f"Columns containing 'a': {cols_with_a}")
df.select(cols_with_a).show()

# Pattern 3: Select with rename
print("Rename during select:")
df.select(
    col("name").alias("employee_name"),
    col("salary").alias("base_salary")
).show()

# Pattern 4: Reorder columns
print("Reordered columns:")
df.select("department", "name", "salary", "age").show()

# Pattern 5: Duplicate column with new name
df.select("*", col("salary").alias("salary_copy")).show()
```

### Performance Considerations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Performance").getOrCreate()

# Assume large Parquet file with many columns
# df = spark.read.parquet("large_dataset.parquet")
df = spark.range(1000000).selectExpr(
    "id",
    "id % 100 as category",
    "id * 1.5 as value",
    "concat('user_', id) as user_id",
    "concat('extra_', id) as unused_col"
)

# Good: Select only needed columns early
# Column pruning happens at file read
result = df.select("id", "value").filter(col("value") > 500)

# Show the plan - notice PushedFilters and pruned columns
result.explain(True)

# Avoid: Selecting all columns then filtering later
# This reads more data than necessary
# bad = df.select("*").filter(col("value") > 500).select("id", "value")
```

## Summary

- **select()** is the primary method for choosing columns, accepting strings or Column objects
- **selectExpr()** enables SQL expressions as strings, useful for complex transformations
- **col()** function creates Column objects for expressions and transformations
- Column selection benefits from **column pruning** - only needed columns are read from storage
- Select columns early in your pipeline to minimize data movement
- Use aliases (`.alias()`) to rename columns during selection
- Choose the selection style that balances readability with your team's preferences

## Additional Resources

- [PySpark select() Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html)
- [PySpark Column Class](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html)
- [SQL Functions Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
