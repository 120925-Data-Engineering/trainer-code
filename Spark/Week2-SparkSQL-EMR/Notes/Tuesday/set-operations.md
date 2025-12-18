# Set Operations

## Learning Objectives
- Understand set operations available for DataFrames: union, intersect, except, and distinct
- Learn the syntax and behavior of each operation
- Know when to apply set operations in data engineering workflows

## Why This Matters

Set operations are fundamental to data manipulation. They allow you to combine, compare, and deduplicate datasets, which are common tasks in data engineering:

- **Union**: Combining data from multiple sources (daily files, regional tables)
- **Intersect**: Finding common records between datasets (shared customers)
- **Except**: Identifying records unique to one dataset (new customers, missing records)
- **Distinct**: Removing duplicates for data quality

These operations directly support our Weekly Epic of *Mastering Spark SQL and DataFrames* by expanding your toolkit for data manipulation.

## The Concept

### Overview of Set Operations

| Operation | Description | Duplicates |
|-----------|-------------|------------|
| `union()` | Combines rows from two DataFrames | Keeps all (including duplicates) |
| `unionByName()` | Union matching columns by name | Keeps all (including duplicates) |
| `intersect()` | Rows appearing in both DataFrames | Removes duplicates |
| `intersectAll()` | Rows appearing in both (keeps duplicates) | Preserves duplicates |
| `exceptAll()` | Rows in first but not in second (keeps duplicates) | Preserves duplicates |
| `subtract()` / `except()` | Rows in first but not in second | Removes duplicates |
| `distinct()` | Unique rows only | Removes duplicates |

### Important Requirements

For set operations (except `unionByName()`), both DataFrames must have:
1. The same number of columns
2. Corresponding columns with compatible data types

### union() vs. unionByName()

**union()**: Combines by column position
```python
# Columns matched by position, not name
df1.union(df2)  # First column with first column, etc.
```

**unionByName()**: Combines by column name
```python
# Columns matched by name (order does not matter)
df1.unionByName(df2)
```

### Deduplication Behavior

- Most set operations (`intersect`, `except`) automatically deduplicate results
- Use `intersectAll()` and `exceptAll()` when you need to preserve duplicate counts
- `union()` preserves all rows; use `distinct()` afterward if needed

## Code Example

### Union Operations

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Set Operations").getOrCreate()

# Two DataFrames with same schema
jan_sales = spark.createDataFrame([
    ("Alice", "Laptop", 1200),
    ("Bob", "Phone", 800),
    ("Charlie", "Tablet", 500)
], ["customer", "product", "amount"])

feb_sales = spark.createDataFrame([
    ("Diana", "Laptop", 1100),
    ("Bob", "Phone", 800),    # Same as January
    ("Eve", "Monitor", 300)
], ["customer", "product", "amount"])

print("January Sales:")
jan_sales.show()

print("February Sales:")
feb_sales.show()

# Union combines all rows (keeps duplicates)
print("UNION (all rows, duplicates preserved):")
jan_sales.union(feb_sales).show()

# Union followed by distinct (removes duplicates)
print("UNION + DISTINCT (unique rows only):")
jan_sales.union(feb_sales).distinct().show()
```

### unionByName()

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Union By Name").getOrCreate()

# DataFrames with same columns but different order
df1 = spark.createDataFrame([
    ("Alice", 100, "NY")
], ["name", "amount", "state"])

df2 = spark.createDataFrame([
    ("CA", 200, "Bob")
], ["state", "amount", "name"])  # Different column order

print("DataFrame 1:")
df1.show()

print("DataFrame 2 (different column order):")
df2.show()

# Regular union would produce wrong results
print("Regular union (WRONG - matches by position):")
df1.union(df2).show()

# unionByName matches by column name
print("unionByName (CORRECT - matches by name):")
df1.unionByName(df2).show()

# With allowMissingColumns for different schemas
df3 = spark.createDataFrame([
    ("Charlie", 300)
], ["name", "amount"])  # Missing 'state' column

print("unionByName with missing columns:")
df1.unionByName(df3, allowMissingColumns=True).show()
```

### Intersect Operations

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Intersect").getOrCreate()

# Customers from two regions
region_a = spark.createDataFrame([
    ("Alice", "alice@email.com"),
    ("Bob", "bob@email.com"),
    ("Charlie", "charlie@email.com"),
    ("Bob", "bob@email.com")  # Duplicate
], ["name", "email"])

region_b = spark.createDataFrame([
    ("Bob", "bob@email.com"),
    ("Diana", "diana@email.com"),
    ("Bob", "bob@email.com")  # Duplicate
], ["name", "email"])

print("Region A customers:")
region_a.show()

print("Region B customers:")
region_b.show()

# Intersect finds common rows (removes duplicates)
print("INTERSECT (common customers, deduplicated):")
region_a.intersect(region_b).show()

# IntersectAll preserves duplicate counts
print("INTERSECT ALL (preserves duplicates):")
region_a.intersectAll(region_b).show()
# Bob appears twice because he appears twice in both
```

### Except (Subtract) Operations

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Except").getOrCreate()

# All users vs active users
all_users = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie"),
    (4, "Diana"),
    (2, "Bob")  # Duplicate
], ["id", "name"])

active_users = spark.createDataFrame([
    (1, "Alice"),
    (3, "Charlie")
], ["id", "name"])

print("All users:")
all_users.show()

print("Active users:")
active_users.show()

# Except finds rows in first but not in second (except is alias for subtract)
print("EXCEPT (users NOT in active, deduplicated):")
all_users.exceptAll(active_users).distinct().show()
# Actually, use subtract() or except() for deduplication

# subtract() is the method name, returns rows in first but not second
print("SUBTRACT (inactive users):")
all_users.subtract(active_users).show()
# Note: subtract() removes duplicates

# exceptAll preserves duplicates
print("EXCEPT ALL (preserves duplicates):")
all_users.exceptAll(active_users).show()
```

### Distinct Operation

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Distinct").getOrCreate()

# Data with duplicates
data = spark.createDataFrame([
    ("Alice", "NY"),
    ("Bob", "CA"),
    ("Alice", "NY"),  # Duplicate
    ("Charlie", "TX"),
    ("Bob", "CA"),    # Duplicate
    ("Alice", "CA")   # Different state, not a duplicate
], ["name", "state"])

print("Original data:")
data.show()

# Distinct removes duplicate rows
print("DISTINCT (unique rows only):")
data.distinct().show()

# Count vs distinct count
print(f"Total rows: {data.count()}")
print(f"Distinct rows: {data.distinct().count()}")

# dropDuplicates with specific columns
print("Drop duplicates based on 'name' only:")
data.dropDuplicates(["name"]).show()
# Keeps first occurrence for each unique name
```

### Practical Example: Data Reconciliation

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Reconciliation").getOrCreate()

# Source system data
source = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 200),
    (3, "Product C", 300),
    (4, "Product D", 400)
], ["id", "name", "price"])

# Target system data
target = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 250),  # Price differs
    (3, "Product C", 300),
    (5, "Product E", 500)   # New record
], ["id", "name", "price"])

print("Source system:")
source.show()

print("Target system:")
target.show()

# Records matching exactly
print("Matching records:")
source.intersect(target).show()

# Records in source but not target (or different)
print("Source-only or different:")
source.subtract(target).show()

# Records in target but not source (or different)
print("Target-only or different:")
target.subtract(source).show()

spark.stop()
```

## Summary

- **union()** combines DataFrames by column position; use `unionByName()` to match by column name
- **intersect()** returns rows present in both DataFrames with automatic deduplication
- **subtract()** / **except()** returns rows in the first DataFrame but not the second
- **distinct()** removes duplicate rows from a single DataFrame
- Use `*All()` variants (intersectAll, exceptAll) to preserve duplicate counts
- Set operations require compatible schemas (same number and types of columns)
- These operations are essential for data reconciliation, deduplication, and combining datasets

## Additional Resources

- [PySpark DataFrame API: Set Operations](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [SQL Set Operators](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-setops.html)
- [Handling Duplicates in Spark](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html)
