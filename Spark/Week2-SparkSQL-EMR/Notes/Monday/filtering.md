# Filtering

## Learning Objectives
- Master filter() and where() for row filtering
- Learn to construct complex filter conditions with multiple predicates
- Understand null handling in filter operations
- Apply best practices for efficient filtering

## Why This Matters

Filtering is one of the most common DataFrame operations. Efficient filtering:
- Reduces data volume early in the pipeline
- Benefits from predicate pushdown to data sources
- Forms the basis of data quality checks and validation
- Enables targeted analysis and reporting

Understanding filtering deeply, including edge cases like null handling, is essential for building robust data pipelines as part of our Weekly Epic.

## The Concept

### filter() vs where()

These methods are **identical** - `where()` is an alias for `filter()`:

```python
df.filter(df.age > 30)  # Same result
df.where(df.age > 30)   # Same result
```

Choose based on readability and team preference. SQL users often prefer `where()`.

### Filter Expression Types

| Type | Example |
|------|---------|
| String expression | `df.filter("age > 30")` |
| Column condition | `df.filter(col("age") > 30)` |
| DataFrame column | `df.filter(df.age > 30)` |
| Boolean column | `df.filter(df["is_active"])` |

### Comparison Operators

| Operator | Python | SQL String |
|----------|--------|------------|
| Equal | `col("a") == 1` | `"a = 1"` |
| Not equal | `col("a") != 1` | `"a != 1"` or `"a <> 1"` |
| Greater | `col("a") > 1` | `"a > 1"` |
| Less | `col("a") < 1` | `"a < 1"` |
| Greater or equal | `col("a") >= 1` | `"a >= 1"` |
| Less or equal | `col("a") <= 1` | `"a <= 1"` |

### Logical Operators

| Operator | Python | SQL String |
|----------|--------|------------|
| AND | `(cond1) & (cond2)` | `"a > 1 AND b < 5"` |
| OR | `(cond1) \| (cond2)` | `"a > 1 OR b < 5"` |
| NOT | `~condition` | `"NOT (a > 1)"` |

**Important**: Python bitwise operators (`&`, `|`, `~`) require parentheses around each condition!

## Code Example

### Basic Filtering

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Filtering").getOrCreate()

# Sample data
data = [
    ("Alice", 34, "Engineering", 75000),
    ("Bob", 45, "Marketing", 65000),
    ("Charlie", 29, "Engineering", 80000),
    ("Diana", 31, "Sales", 55000),
    ("Eve", 38, "Marketing", 70000)
]

df = spark.createDataFrame(data, ["name", "age", "department", "salary"])

print("Original DataFrame:")
df.show()

# Filter with string expression
print("Age > 30 (string):")
df.filter("age > 30").show()

# Filter with column condition
print("Age > 30 (column):")
df.filter(col("age") > 30).show()

# Filter with DataFrame attribute
print("Age > 30 (attribute):")
df.filter(df.age > 30).show()

# Using where() - same as filter()
print("Using where():")
df.where("age > 30").show()
```

### Multiple Conditions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Multiple Conditions").getOrCreate()

data = [
    ("Alice", 34, "Engineering", 75000),
    ("Bob", 45, "Marketing", 65000),
    ("Charlie", 29, "Engineering", 80000),
    ("Diana", 31, "Sales", 55000),
    ("Eve", 38, "Marketing", 70000)
]

df = spark.createDataFrame(data, ["name", "age", "department", "salary"])

# AND condition (note the parentheses!)
print("Age > 30 AND salary > 70000:")
df.filter((col("age") > 30) & (col("salary") > 70000)).show()

# OR condition
print("Department is Engineering OR salary > 70000:")
df.filter((col("department") == "Engineering") | (col("salary") > 70000)).show()

# NOT condition
print("NOT in Marketing:")
df.filter(~(col("department") == "Marketing")).show()

# Complex combination
print("(Age > 30 AND Engineering) OR salary > 75000:")
df.filter(
    ((col("age") > 30) & (col("department") == "Engineering")) |
    (col("salary") > 75000)
).show()

# String expression for complex conditions
print("Using SQL string:")
df.filter("age > 30 AND (department = 'Engineering' OR salary > 70000)").show()
```

### Special Filter Functions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Special Filters").getOrCreate()

data = [
    ("Alice", 34, "Engineering"),
    ("Bob", None, "Marketing"),
    ("Charlie", 29, None),
    ("Diana", 31, "Sales")
]

df = spark.createDataFrame(data, ["name", "age", "department"])

print("Original with nulls:")
df.show()

# Check for null values
print("Age is NOT null:")
df.filter(col("age").isNotNull()).show()

print("Department IS null:")
df.filter(col("department").isNull()).show()

# isin() for multiple values
print("Department in ['Engineering', 'Sales']:")
df.filter(col("department").isin(["Engineering", "Sales"])).show()

# NOT in list
print("Department NOT in ['Marketing']:")
df.filter(~col("department").isin(["Marketing"])).show()

# between() for ranges
print("Age between 30 and 40:")
df.filter(col("age").between(30, 40)).show()
```

### String Filtering

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("String Filters").getOrCreate()

data = [
    ("Alice Smith", "alice@company.com"),
    ("Bob Johnson", "bob.j@example.org"),
    ("Charlie Brown", "charlie@company.com"),
    ("Diana Prince", "diana.p@other.net")
]

df = spark.createDataFrame(data, ["name", "email"])

# startswith
print("Name starts with 'A':")
df.filter(col("name").startswith("A")).show()

# endswith
print("Email ends with '.com':")
df.filter(col("email").endswith(".com")).show()

# contains
print("Name contains 'own':")
df.filter(col("name").contains("own")).show()

# like (SQL pattern)
print("Name like 'A%' (starts with A):")
df.filter(col("name").like("A%")).show()

print("Email like '%@company%':")
df.filter(col("email").like("%@company%")).show()

# rlike (regex)
print("Email matches regex (gmail or company):")
df.filter(col("email").rlike("(company|example)")).show()
```

### Null Handling in Filters

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

spark = SparkSession.builder.appName("Null Handling").getOrCreate()

data = [
    ("Alice", 34),
    ("Bob", None),
    ("Charlie", 29),
    ("Diana", None)
]

df = spark.createDataFrame(data, ["name", "age"])

print("Original data:")
df.show()

# WARNING: Comparisons with null return null (not False!)
print("age > 30 (nulls are excluded):")
df.filter(col("age") > 30).show()

print("age <= 30 (nulls STILL excluded):")
df.filter(col("age") <= 30).show()

print("NOT (age > 30) - still excludes nulls!")
df.filter(~(col("age") > 30)).show()

# To include nulls, check explicitly
print("age > 30 OR age IS NULL:")
df.filter((col("age") > 30) | col("age").isNull()).show()

# Or use coalesce with a default
print("Using coalesce (treat null as 0):")
df.filter(coalesce(col("age"), lit(0)) > 30).show()
```

### Efficient Filtering

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Efficient Filtering").getOrCreate()

# Best Practice 1: Filter early in the pipeline
# Good: Filter -> Select -> Transform
df = spark.range(1000000)
result = df.filter(col("id") < 1000).select("id")

# Bad: Select everything -> Transform -> Filter at the end
# This processes more data unnecessarily

# Best Practice 2: Predicate pushdown
# When reading from Parquet, filters are pushed to the file scan
# df = spark.read.parquet("data.parquet").filter(col("date") == "2023-01-01")
# The filter is applied during file reading, not after

# Best Practice 3: Use appropriate operators
# isin() is optimized better than multiple OR conditions
# Good:
df.filter(col("id").isin([1, 2, 3, 4, 5]))
# Less optimal:
# df.filter((col("id") == 1) | (col("id") == 2) | ...)

# Best Practice 4: Check explain plan
result = df.filter(col("id") > 500).filter(col("id") < 600)
print("Explain plan (filters may be combined):")
result.explain()
```

## Summary

- **filter()** and **where()** are identical; choose based on preference
- Use parentheses with **&** (AND) and **|** (OR) operators: `(cond1) & (cond2)`
- **isNull()** and **isNotNull()** handle null checks explicitly
- **isin()** efficiently filters for multiple values
- **between()** filters for range inclusion
- String methods: **startswith()**, **endswith()**, **contains()**, **like()**, **rlike()**
- Nulls are excluded from most comparisons - check explicitly if needed
- Filter early in pipelines to benefit from predicate pushdown

## Additional Resources

- [PySpark filter() Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html)
- [Column Predicates](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html)
- [Predicate Pushdown Optimization](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
