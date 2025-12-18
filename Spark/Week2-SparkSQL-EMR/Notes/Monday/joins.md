# Joins in Spark SQL

## Learning Objectives
- Understand all join types available in Spark SQL
- Learn the syntax for joining DataFrames
- Know when to use each join type based on your data requirements
- Recognize performance considerations for different join strategies

## Why This Matters

Joins are the foundation of relational data processing. In the real world, data is rarely contained in a single table. Customers are in one dataset, orders in another, and products in a third. Joining these datasets together is essential for creating complete views of your data.

As data engineers, you will join datasets constantly:
- Combining fact and dimension tables in a data warehouse
- Enriching event data with reference data
- Matching records between systems
- Building denormalized tables for analytics

Mastering joins is a critical skill for our Weekly Epic of *Mastering Spark SQL and DataFrames*.

## The Concept

### Join Types Overview

Spark SQL supports all standard SQL join types:

| Join Type | Description | When to Use |
|-----------|-------------|-------------|
| **Inner** | Only matching rows from both tables | Most common; need data that exists in both |
| **Left Outer** | All rows from left + matching from right | Keep all left data, enrich with right |
| **Right Outer** | All rows from right + matching from left | Keep all right data, enrich with left |
| **Full Outer** | All rows from both, matched where possible | Need complete picture from both sides |
| **Cross** | Cartesian product (every combination) | Rarely used; generates many rows |
| **Left Semi** | Left rows that have a match in right | Filter left table by existence in right |
| **Left Anti** | Left rows that do NOT have a match | Filter left table by absence in right |

### Visual Representation

```
LEFT TABLE          RIGHT TABLE
+----+------+       +----+-------+
| id | name |       | id | value |
+----+------+       +----+-------+
| 1  | A    |       | 1  | 100   |
| 2  | B    |       | 3  | 300   |
| 3  | C    |       | 4  | 400   |
+----+------+       +----+-------+

INNER JOIN (id)     LEFT OUTER         RIGHT OUTER        FULL OUTER
+--+----+-----+     +--+----+-----+    +--+----+-----+    +--+----+-----+
|1 | A  | 100 |     |1 | A  | 100 |    |1 | A  | 100 |    |1 | A  | 100 |
|3 | C  | 300 |     |2 | B  | null|    |3 | C  | 300 |    |2 | B  | null|
+--+----+-----+     |3 | C  | 300 |    |4 |null| 400 |    |3 | C  | 300 |
                    +--+----+-----+    +--+----+-----+    |4 |null| 400 |
                                                          +--+----+-----+
```

### Join Syntax

**Method 1: Using join() with condition**
```python
df1.join(df2, df1.id == df2.id, "inner")
```

**Method 2: Using join() with column name (when column names match)**
```python
df1.join(df2, "id", "inner")  # Automatically matches on 'id'
```

**Method 3: Using join() with multiple columns**
```python
df1.join(df2, ["id", "date"], "inner")
```

### Join Type Parameter Values

| Parameter Value | Join Type |
|----------------|-----------|
| `"inner"` | Inner join (default) |
| `"left"` or `"left_outer"` | Left outer join |
| `"right"` or `"right_outer"` | Right outer join |
| `"outer"` or `"full"` or `"full_outer"` | Full outer join |
| `"cross"` | Cross join |
| `"left_semi"` | Left semi join |
| `"left_anti"` | Left anti join |

## Code Example

### All Join Types Demonstration

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Joins Demo").getOrCreate()

# Create sample DataFrames
employees = spark.createDataFrame([
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Charlie", 103),
    (4, "Diana", None)
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (101, "Engineering"),
    (102, "Marketing"),
    (104, "Sales")
], ["dept_id", "dept_name"])

print("Employees:")
employees.show()

print("Departments:")
departments.show()

# -----------------
# INNER JOIN
# -----------------
print("INNER JOIN (only matching rows):")
employees.join(
    departments, 
    employees.dept_id == departments.dept_id, 
    "inner"
).select(
    employees.emp_id,
    employees.name,
    departments.dept_name
).show()

# -----------------
# LEFT OUTER JOIN
# -----------------
print("LEFT OUTER JOIN (all employees, matched departments):")
employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "left"
).select(
    employees.emp_id,
    employees.name,
    departments.dept_name
).show()

# -----------------
# RIGHT OUTER JOIN
# -----------------
print("RIGHT OUTER JOIN (all departments, matched employees):")
employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "right"
).select(
    employees.emp_id,
    employees.name,
    departments.dept_name
).show()

# -----------------
# FULL OUTER JOIN
# -----------------
print("FULL OUTER JOIN (all from both sides):")
employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "outer"
).show()

spark.stop()
```

### Semi and Anti Joins

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Semi Anti Joins").getOrCreate()

# Orders and shipped items
orders = spark.createDataFrame([
    (1, "Alice", "Laptop"),
    (2, "Bob", "Phone"),
    (3, "Charlie", "Tablet"),
    (4, "Diana", "Monitor")
], ["order_id", "customer", "product"])

shipped = spark.createDataFrame([
    (1, "2023-01-15"),
    (3, "2023-01-16")
], ["order_id", "ship_date"])

print("Orders:")
orders.show()

print("Shipped:")
shipped.show()

# -----------------
# LEFT SEMI JOIN
# -----------------
# "Show me orders that HAVE been shipped"
print("LEFT SEMI JOIN (orders that have been shipped):")
orders.join(shipped, "order_id", "left_semi").show()
# Notice: Only columns from left table are returned

# -----------------
# LEFT ANTI JOIN
# -----------------
# "Show me orders that have NOT been shipped"
print("LEFT ANTI JOIN (orders not yet shipped):")
orders.join(shipped, "order_id", "left_anti").show()

spark.stop()
```

### Cross Join

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Cross Join").getOrCreate()

# Products and colors
products = spark.createDataFrame([
    ("Shirt",),
    ("Pants",)
], ["product"])

colors = spark.createDataFrame([
    ("Red",),
    ("Blue",),
    ("Green",)
], ["color"])

print("Products:")
products.show()

print("Colors:")
colors.show()

# Cross join creates every combination
print("CROSS JOIN (all combinations):")
products.crossJoin(colors).show()

# Or using join with "cross" type
products.join(colors, how="cross").show()

spark.stop()
```

### Joining on Multiple Conditions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Complex Joins").getOrCreate()

# Sales data
sales = spark.createDataFrame([
    ("2023-01", "Electronics", 1000),
    ("2023-01", "Clothing", 500),
    ("2023-02", "Electronics", 1200)
], ["month", "category", "amount"])

# Targets data
targets = spark.createDataFrame([
    ("2023-01", "Electronics", 900),
    ("2023-01", "Clothing", 600),
    ("2023-02", "Electronics", 1100)
], ["month", "category", "target"])

# Join on multiple columns
print("Join on month AND category:")
sales.join(
    targets,
    (sales.month == targets.month) & (sales.category == targets.category),
    "inner"
).select(
    sales.month,
    sales.category,
    sales.amount,
    targets.target,
    (col("amount") - col("target")).alias("variance")
).show()

# Simpler syntax when column names match
sales.join(targets, ["month", "category"], "inner").show()

spark.stop()
```

### Handling Duplicate Column Names

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Duplicate Columns").getOrCreate()

df1 = spark.createDataFrame([
    (1, "Alice", 100),
], ["id", "name", "value"])

df2 = spark.createDataFrame([
    (1, "Bob", 200),
], ["id", "name", "value"])

# Problem: Both have 'name' and 'value' columns
joined = df1.join(df2, "id", "inner")
print("Joined (ambiguous columns):")
joined.show()

# Solution 1: Rename before joining
df2_renamed = df2.withColumnRenamed("name", "name_2") \
                 .withColumnRenamed("value", "value_2")
df1.join(df2_renamed, "id").show()

# Solution 2: Use aliases and select specific columns
df1_alias = df1.alias("a")
df2_alias = df2.alias("b")

df1_alias.join(df2_alias, df1.id == df2.id).select(
    "a.id",
    "a.name",
    "a.value",
    "b.name",
    "b.value"
).show()

spark.stop()
```

## Summary

- **Inner join** returns only matching rows from both DataFrames
- **Left/Right outer joins** preserve all rows from one side and match from the other
- **Full outer join** preserves all rows from both sides
- **Cross join** creates a Cartesian product (use with caution on large datasets)
- **Left semi join** filters the left table by existence in the right table
- **Left anti join** filters the left table by absence in the right table
- Use column list syntax `["col1", "col2"]` when column names match in both DataFrames
- Handle duplicate column names by renaming or using aliases

## Additional Resources

- [PySpark Join Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
- [Spark SQL Join Optimization](https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints-for-sql-queries)
- [Understanding Join Strategies](https://www.databricks.com/blog/2017/02/28/deep-dive-into-spark-sqls-catalyst-optimizer.html)
