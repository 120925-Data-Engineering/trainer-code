# Aggregate Functions

## Learning Objectives
- Master the groupBy operation for grouping data
- Learn common aggregate functions: sum, avg, count, min, max
- Understand how to apply multiple aggregations simultaneously
- Use the agg() function for flexible aggregation

## Why This Matters

Aggregation is a fundamental data operation. Whether you are calculating total sales, average response times, or counting unique users, aggregate functions are the tools that turn raw data into meaningful insights.

In data engineering, aggregations are everywhere:
- Building summary tables for dashboards
- Calculating metrics for reporting
- Creating features for machine learning
- Validating data quality

Understanding aggregation deeply is essential for our Weekly Epic of *Mastering Spark SQL and DataFrames* because most business questions require some form of grouping and summarization.

## The Concept

### The groupBy Operation

The `groupBy()` method groups rows that have the same values in specified columns:

```python
df.groupBy("column_name")  # Returns GroupedData object
```

After grouping, you apply aggregate functions to summarize each group:

```python
df.groupBy("department").count()  # Count rows per department
df.groupBy("department").sum("salary")  # Sum salaries per department
```

### Built-in Aggregate Methods

After `groupBy()`, you can call these methods directly:

| Method | Description |
|--------|-------------|
| `count()` | Count rows in each group |
| `sum(col)` | Sum values of a column |
| `avg(col)` / `mean(col)` | Average value |
| `min(col)` | Minimum value |
| `max(col)` | Maximum value |

### The agg() Function

For more complex aggregations, use `agg()` with functions from `pyspark.sql.functions`:

```python
from pyspark.sql.functions import sum, avg, count, min, max

df.groupBy("department").agg(
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    count("*").alias("employee_count")
)
```

### Common Aggregate Functions

| Function | Import | Description |
|----------|--------|-------------|
| `count()` | `pyspark.sql.functions` | Count of values |
| `countDistinct()` | `pyspark.sql.functions` | Count of unique values |
| `sum()` | `pyspark.sql.functions` | Sum of values |
| `avg()` | `pyspark.sql.functions` | Average of values |
| `min()` | `pyspark.sql.functions` | Minimum value |
| `max()` | `pyspark.sql.functions` | Maximum value |
| `first()` | `pyspark.sql.functions` | First value in group |
| `last()` | `pyspark.sql.functions` | Last value in group |
| `collect_list()` | `pyspark.sql.functions` | Collect all values into a list |
| `collect_set()` | `pyspark.sql.functions` | Collect unique values into a set |
| `stddev()` | `pyspark.sql.functions` | Standard deviation |
| `variance()` | `pyspark.sql.functions` | Variance |

### Grouping by Multiple Columns

You can group by multiple columns to create finer-grained groups:

```python
df.groupBy("department", "region").agg(
    sum("sales").alias("total_sales")
)
```

## Code Example

### Basic Aggregations

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Aggregate Functions").getOrCreate()

# Create sample data
data = [
    ("Engineering", "Alice", 75000, "NY"),
    ("Engineering", "Bob", 80000, "CA"),
    ("Engineering", "Charlie", 70000, "NY"),
    ("Marketing", "Diana", 65000, "NY"),
    ("Marketing", "Eve", 60000, "CA"),
    ("Sales", "Frank", 55000, "TX"),
    ("Sales", "Grace", 58000, "TX"),
    ("Sales", "Henry", 52000, "NY")
]

df = spark.createDataFrame(data, ["department", "name", "salary", "state"])

print("Original Data:")
df.show()

# Simple count by department
print("Employee count by department:")
df.groupBy("department").count().show()

# Sum of salaries by department
print("Total salary by department:")
df.groupBy("department").sum("salary").show()

# Average salary by department
print("Average salary by department:")
df.groupBy("department").avg("salary").show()

# Min and max salary by department
print("Min salary by department:")
df.groupBy("department").min("salary").show()

print("Max salary by department:")
df.groupBy("department").max("salary").show()
```

### Multiple Aggregations with agg()

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum, avg, count, min, max, 
    countDistinct, round as spark_round
)

spark = SparkSession.builder.appName("Multiple Aggregations").getOrCreate()

# Same sample data
data = [
    ("Engineering", "Alice", 75000, "NY"),
    ("Engineering", "Bob", 80000, "CA"),
    ("Engineering", "Charlie", 70000, "NY"),
    ("Marketing", "Diana", 65000, "NY"),
    ("Marketing", "Eve", 60000, "CA"),
    ("Sales", "Frank", 55000, "TX"),
    ("Sales", "Grace", 58000, "TX"),
    ("Sales", "Henry", 52000, "NY")
]

df = spark.createDataFrame(data, ["department", "name", "salary", "state"])

# Multiple aggregations at once
print("Department Summary:")
df.groupBy("department").agg(
    count("*").alias("employee_count"),
    sum("salary").alias("total_salary"),
    spark_round(avg("salary"), 2).alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    countDistinct("state").alias("states_count")
).show()

# Grouping by multiple columns
print("\nSalary by Department and State:")
df.groupBy("department", "state").agg(
    count("*").alias("count"),
    sum("salary").alias("total_salary")
).orderBy("department", "state").show()
```

### Advanced Aggregations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    collect_list, collect_set, first, last,
    stddev, variance, expr, sum, count
)

spark = SparkSession.builder.appName("Advanced Aggregations").getOrCreate()

# Sales data
sales_data = [
    ("2023-01", "Electronics", 1200),
    ("2023-01", "Electronics", 800),
    ("2023-01", "Clothing", 300),
    ("2023-02", "Electronics", 1500),
    ("2023-02", "Clothing", 400),
    ("2023-02", "Clothing", 350)
]

df = spark.createDataFrame(sales_data, ["month", "category", "amount"])

print("Sales Data:")
df.show()

# Collect values into lists
print("Products sold by month:")
df.groupBy("month").agg(
    collect_list("category").alias("categories_sold"),
    collect_set("category").alias("unique_categories"),
    count("*").alias("transaction_count"),
    sum("amount").alias("total_sales")
).show(truncate=False)

# Statistical aggregations
print("\nSales statistics by category:")
df.groupBy("category").agg(
    count("*").alias("transactions"),
    sum("amount").alias("total"),
    stddev("amount").alias("std_dev"),
    variance("amount").alias("variance")
).show()

# Using expr for SQL-like aggregations
print("\nUsing expr() for complex aggregations:")
df.groupBy("category").agg(
    expr("sum(amount) as total_sales"),
    expr("avg(amount) as avg_sale"),
    expr("percentile_approx(amount, 0.5) as median_sale")
).show()
```

### Aggregations without groupBy

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, min, max

spark = SparkSession.builder.appName("Global Aggregations").getOrCreate()

data = [
    ("Alice", 75000),
    ("Bob", 80000),
    ("Charlie", 70000)
]

df = spark.createDataFrame(data, ["name", "salary"])

# Global aggregations (no groupBy)
print("Global statistics:")
df.agg(
    count("*").alias("total_employees"),
    sum("salary").alias("total_payroll"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
).show()

# Or simply
print("Total salary:", df.agg(sum("salary")).collect()[0][0])
```

## Summary

- Use `groupBy()` to group rows by one or more columns
- Built-in methods like `count()`, `sum()`, `avg()`, `min()`, `max()` provide quick aggregations
- Use `agg()` with functions from `pyspark.sql.functions` for multiple aggregations in one pass
- `countDistinct()` counts unique values, useful for cardinality analysis
- `collect_list()` and `collect_set()` aggregate values into arrays
- Statistical functions like `stddev()` and `variance()` are available for advanced analysis
- Always alias aggregated columns for readability: `.alias("column_name")`

## Additional Resources

- [PySpark Aggregate Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#aggregate-functions)
- [GroupedData API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html)
- [SQL Aggregate Functions](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#aggregate-functions)
