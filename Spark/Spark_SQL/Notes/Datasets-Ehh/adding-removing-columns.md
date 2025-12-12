# Adding and Removing Columns

## Learning Objectives
- Master `withColumn()` for adding and modifying columns
- Learn `drop()` for removing columns
- Understand `withColumnRenamed()` for renaming columns
- Apply column expression techniques for complex transformations

## Why This Matters

Data rarely arrives in the exact format you need. As a data engineer, you will constantly reshape DataFrames by:

- Adding calculated fields (profit margins, age from birth date)
- Removing sensitive or unnecessary columns (PII, temporary fields)
- Renaming columns to match target schema conventions
- Transforming column values for business logic

These column manipulation skills are fundamental to ETL pipelines and data transformation, core activities in our Weekly Epic of *Mastering Spark SQL and DataFrames*.

## The Concept

### Column Manipulation Methods

| Method | Purpose | Usage |
|--------|---------|-------|
| `withColumn(name, col)` | Add or replace a column | `df.withColumn("new_col", expr)` |
| `drop(cols)` | Remove one or more columns | `df.drop("col1", "col2")` |
| `withColumnRenamed(old, new)` | Rename a column | `df.withColumnRenamed("old", "new")` |
| `withColumnsRenamed(mapping)` | Rename multiple columns | `df.withColumnsRenamed({"old1": "new1"})` |
| `select()` | Select and transform columns | `df.select("col1", expr.alias("new"))` |

### withColumn() Behavior

- If the column name exists, it **replaces** the column
- If the column name does not exist, it **adds** a new column
- Always returns a new DataFrame (DataFrames are immutable)

### Column References

There are multiple ways to reference columns:

```python
# String reference
df.withColumn("new", df["existing"] * 2)

# Attribute reference
df.withColumn("new", df.existing * 2)

# col() function (recommended for clarity)
from pyspark.sql.functions import col
df.withColumn("new", col("existing") * 2)

# expr() for SQL expressions
from pyspark.sql.functions import expr
df.withColumn("new", expr("existing * 2"))
```

### Chaining Column Operations

Since each operation returns a new DataFrame, you can chain them:

```python
df.withColumn("col1", expr1) \
  .withColumn("col2", expr2) \
  .drop("old_col") \
  .withColumnRenamed("temp", "final")
```

## Code Example

### Adding Columns with withColumn()

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, when, upper, current_date, datediff

spark = SparkSession.builder.appName("Adding Columns").getOrCreate()

# Sample employee data
data = [
    ("Alice", "Engineering", 75000, "2020-01-15"),
    ("Bob", "Marketing", 65000, "2019-06-01"),
    ("Charlie", "Engineering", 80000, "2021-03-20")
]

df = spark.createDataFrame(data, ["name", "department", "salary", "hire_date"])

print("Original DataFrame:")
df.show()

# Add a constant column
print("With constant column:")
df.withColumn("country", lit("USA")).show()

# Add a calculated column
print("With annual bonus (10% of salary):")
df.withColumn("bonus", col("salary") * 0.10).show()

# Add a conditional column
print("With salary tier:")
df.withColumn("salary_tier",
    when(col("salary") < 70000, "Entry")
    .when(col("salary") < 80000, "Mid")
    .otherwise("Senior")
).show()

# Add column using expr (SQL syntax)
print("With monthly salary:")
df.withColumn("monthly_salary", expr("salary / 12")).show()

# Add column based on string manipulation
print("With uppercase name:")
df.withColumn("name_upper", upper(col("name"))).show()

# Add column with date calculation
print("With tenure days:")
df.withColumn("hire_date", col("hire_date").cast("date")) \
  .withColumn("tenure_days", datediff(current_date(), col("hire_date"))) \
  .show()
```

### Modifying Existing Columns

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, trim, lower

spark = SparkSession.builder.appName("Modifying Columns").getOrCreate()

data = [
    ("  Alice  ", 75432.789),
    (" BOB ", 65000.123),
    ("CHARLIE", 80999.456)
]

df = spark.createDataFrame(data, ["name", "salary"])

print("Original DataFrame:")
df.show()

# Overwrite existing column (clean up name)
print("Clean name (trim and lowercase):")
df.withColumn("name", lower(trim(col("name")))).show()

# Round salary to 2 decimal places
print("Rounded salary:")
df.withColumn("salary", spark_round(col("salary"), 2)).show()

# Multiple modifications
print("Multiple modifications:")
df.withColumn("name", lower(trim(col("name")))) \
  .withColumn("salary", spark_round(col("salary"), 0)) \
  .show()
```

### Removing Columns with drop()

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Removing Columns").getOrCreate()

data = [
    (1, "Alice", "alice@email.com", "secret123", "Engineering"),
    (2, "Bob", "bob@email.com", "password456", "Marketing")
]

df = spark.createDataFrame(data, ["id", "name", "email", "password", "department"])

print("Original DataFrame:")
df.show()

# Drop a single column
print("Drop password (single column):")
df.drop("password").show()

# Drop multiple columns
print("Drop password and email:")
df.drop("password", "email").show()

# Drop using column object
from pyspark.sql.functions import col
df.drop(col("password")).show()

# Drop columns that may not exist (no error)
# If column does not exist, drop() silently ignores it
df.drop("nonexistent_column").show()

# Keep only specific columns (alternative approach)
print("Keep only id and name (using select):")
df.select("id", "name").show()
```

### Renaming Columns

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Renaming Columns").getOrCreate()

data = [
    (1, "Alice", 75000),
    (2, "Bob", 65000)
]

df = spark.createDataFrame(data, ["emp_id", "emp_name", "emp_salary"])

print("Original DataFrame:")
df.show()

# Rename single column
print("Rename emp_name to name:")
df.withColumnRenamed("emp_name", "name").show()

# Chain multiple renames
print("Rename multiple columns:")
df.withColumnRenamed("emp_id", "id") \
  .withColumnRenamed("emp_name", "name") \
  .withColumnRenamed("emp_salary", "salary") \
  .show()

# Rename multiple at once (Spark 3.4+)
# df.withColumnsRenamed({"emp_id": "id", "emp_name": "name"}).show()

# Alternative: Use select with alias
print("Using select with alias:")
df.select(
    col("emp_id").alias("id"),
    col("emp_name").alias("name"),
    col("emp_salary").alias("salary")
).show()

# Using toDF to rename all columns at once
print("Using toDF:")
df.toDF("id", "name", "salary").show()
```

### Complex Column Transformations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat, concat_ws, split, 
    when, coalesce, lit, 
    regexp_replace, substring
)

spark = SparkSession.builder.appName("Complex Transforms").getOrCreate()

data = [
    ("Alice", "Smith", "123-456-7890", None),
    ("Bob", "Johnson", "987-654-3210", "bob@work.com"),
    ("Charlie", "Brown", None, "charlie@email.com")
]

df = spark.createDataFrame(data, ["first_name", "last_name", "phone", "email"])

print("Original DataFrame:")
df.show()

# Concatenate columns
print("Full name (concatenated):")
df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))).show()

# Better concatenation with separator
df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))).show()

# Handle nulls with coalesce
print("Contact info (email or phone, whichever exists):")
df.withColumn("contact", coalesce(col("email"), col("phone"), lit("N/A"))).show()

# Clean phone format
print("Clean phone number:")
df.withColumn("phone_clean", regexp_replace(col("phone"), "-", "")).show()

# Extract area code
print("Area code:")
df.withColumn("area_code", substring(col("phone"), 1, 3)).show()

# Split into array and access element
df.withColumn("phone_parts", split(col("phone"), "-")) \
  .withColumn("area_code", col("phone_parts")[0]) \
  .show()
```

## Summary

- **withColumn()** adds a new column or replaces an existing one with the same name
- **drop()** removes one or more columns; silently ignores non-existent columns
- **withColumnRenamed()** renames a single column; chain for multiple renames
- Use **col()** or **expr()** for column references and expressions
- Chain operations together for readable, efficient transformations
- **select()** with **alias()** is an alternative for restructuring DataFrames
- Column operations return new DataFrames; the original is unchanged (immutability)

## Additional Resources

- [PySpark Column Class](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html)
- [PySpark Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [DataFrame withColumn Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html)
