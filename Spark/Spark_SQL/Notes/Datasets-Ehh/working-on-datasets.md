# Working on Datasets

## Learning Objectives
- Apply practical techniques for working with typed patterns in PySpark
- Understand encoders and their role in the Dataset API
- Learn to create, transform, and query data with type-aware approaches

## Why This Matters

Building on the introduction to Datasets, this topic focuses on practical application. While full Dataset functionality is a Scala/Java feature, understanding the underlying mechanics helps you:

- Write more robust PySpark code with explicit schemas
- Understand how Spark optimizes typed operations
- Work effectively in polyglot Spark environments
- Apply best practices from typed programming to Python code

This completes our Wednesday journey in the Weekly Epic, bridging DataFrames with more structured, type-conscious programming patterns.

## The Concept

### Encoders Explained

**Encoders** are the mechanism that converts between:
- JVM objects (case classes, POJOs)
- Spark's internal binary format (Tungsten)

```
+---------------+     +----------+     +------------------+
| Domain Object | --> | Encoder  | --> | Binary Format    |
| Person(...)   |     |          |     | (Tungsten Row)   |
+---------------+     +----------+     +------------------+
```

Benefits of Encoders over Java Serialization:
- **Performance**: Direct memory access, no serialization overhead
- **Efficiency**: Columnar storage, better compression
- **Optimization**: Catalyst can reason about the data structure

### Python Approach: Schema as Contract

Since Python lacks compile-time type checking, treat schemas as contracts:

```python
# The schema IS your type definition
person_schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False)
])

# Enforce at creation
df = spark.createDataFrame(data, schema=person_schema)
```

### Typed Operations Pattern

Even in Python, you can think in typed terms:

```
Operation         | Typed Thinking
------------------|--------------------------------------------------
map               | Lambda takes Row, returns transformed value
filter            | Predicate on typed fields
groupBy + agg     | Group by typed key, aggregate typed values
join              | Combine two typed structures on common field
```

### Row Objects

In PySpark, Row is the fundamental unit:

```python
from pyspark.sql import Row

# Create Row objects
person = Row(name="Alice", age=30)
print(person.name)  # Access like attributes
print(person["age"])  # Or like dictionary
```

## Code Example

### Working with Row Objects

```python
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Row Objects").getOrCreate()

# Method 1: Create Row objects directly
rows = [
    Row(name="Alice", age=30, department="Engineering"),
    Row(name="Bob", age=25, department="Marketing"),
    Row(name="Charlie", age=35, department="Engineering")
]

df = spark.createDataFrame(rows)
print("DataFrame from Row objects:")
df.show()
df.printSchema()

# Method 2: Row factory pattern
Person = Row("name", "age", "department")
persons = [
    Person("Diana", 28, "Sales"),
    Person("Eve", 32, "Marketing")
]

df2 = spark.createDataFrame(persons)
print("DataFrame from Row factory:")
df2.show()

# Accessing Row data
print("Accessing Row elements:")
first_row = df.first()
print(f"  Name (attribute): {first_row.name}")
print(f"  Age (index): {first_row[1]}")
print(f"  Department (key): {first_row['department']}")
```

### Typed Transformations with map

```python
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("Typed Map").getOrCreate()

# Create DataFrame
data = [("Alice", 30, 75000), ("Bob", 25, 65000), ("Charlie", 35, 80000)]
df = spark.createDataFrame(data, ["name", "age", "salary"])

# Convert to RDD for typed map operations
def calculate_tax_bracket(row):
    """Type-aware transformation function."""
    salary = row.salary
    if salary < 70000:
        bracket = "Low"
    elif salary < 80000:
        bracket = "Medium"
    else:
        bracket = "High"
    
    return Row(
        name=row.name,
        age=row.age,
        salary=row.salary,
        tax_bracket=bracket
    )

# Apply typed transformation
result_rdd = df.rdd.map(calculate_tax_bracket)
result_df = result_rdd.toDF()

print("With tax bracket (via typed map):")
result_df.show()

# Type-safe aggregation via RDD
totals_rdd = df.rdd.map(lambda row: (row.name, row.salary)) \
                   .reduceByKey(lambda a, b: a + b)
print("Salary by name:", totals_rdd.collect())
```

### Schema Evolution and Compatibility

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("Schema Evolution").getOrCreate()

# Version 1 schema
schema_v1 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True)
])

# Version 2 schema (added field)
schema_v2 = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)  # New field
])

# Data with v1 schema
data_v1 = [(1, "Alice"), (2, "Bob")]
df_v1 = spark.createDataFrame(data_v1, schema_v1)

# Data with v2 schema
data_v2 = [(3, "Charlie", "charlie@email.com"), (4, "Diana", "diana@email.com")]
df_v2 = spark.createDataFrame(data_v2, schema_v2)

print("V1 Data:")
df_v1.show()

print("V2 Data:")
df_v2.show()

# Evolve v1 to v2 schema (add missing column)
df_v1_evolved = df_v1.withColumn("email", lit(None).cast(StringType()))

# Now they can be unioned
combined = df_v1_evolved.unionByName(df_v2)
print("Combined (evolved schema):")
combined.show()
```

### Validation Layer Pattern

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, lit
from typing import Tuple, List

spark = SparkSession.builder.appName("Validation Layer").getOrCreate()

class PersonValidator:
    """Type-aware validation for Person data."""
    
    expected_schema = StructType([
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("email", StringType(), True)
    ])
    
    @classmethod
    def validate_schema(cls, df: DataFrame) -> bool:
        """Check if DataFrame schema matches expected."""
        actual_fields = {f.name: f.dataType for f in df.schema.fields}
        expected_fields = {f.name: f.dataType for f in cls.expected_schema.fields}
        return actual_fields == expected_fields
    
    @classmethod
    def validate_data(cls, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Split into valid and invalid records."""
        valid = df.filter(
            (col("name").isNotNull()) &
            (col("name") != "") &
            (col("age") >= 0) &
            (col("age") <= 150)
        )
        
        invalid = df.filter(
            (col("name").isNull()) |
            (col("name") == "") |
            (col("age") < 0) |
            (col("age") > 150)
        ).withColumn("validation_error", 
            when(col("name").isNull() | (col("name") == ""), "Invalid name")
            .when((col("age") < 0) | (col("age") > 150), "Invalid age")
            .otherwise("Unknown error")
        )
        
        return valid, invalid
    
    @classmethod
    def create_validated(cls, data: List, spark: SparkSession) -> DataFrame:
        """Create DataFrame with schema validation."""
        return spark.createDataFrame(data, cls.expected_schema)


# Test the validator
test_data = [
    ("Alice", 30, "alice@email.com"),
    ("", 25, "invalid@email.com"),  # Invalid name
    ("Bob", -5, "bob@email.com"),   # Invalid age
    ("Charlie", 35, None)           # Valid (email nullable)
]

df = spark.createDataFrame(test_data, ["name", "age", "email"])

print("Schema valid:", PersonValidator.validate_schema(df))

valid_df, invalid_df = PersonValidator.validate_data(df)

print("\nValid records:")
valid_df.show()

print("Invalid records:")
invalid_df.show()
```

### Converting Between DataFrame and RDD for Typed Operations

```python
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("DF RDD Conversion").getOrCreate()

# Start with DataFrame
df = spark.createDataFrame([
    ("Alice", 30),
    ("Bob", 25),
    ("Charlie", 35)
], ["name", "age"])

# Convert to RDD for typed operations
rdd = df.rdd

# RDD operations with type awareness
# Each element is a Row object
ages = rdd.map(lambda row: row.age).collect()
print("Ages:", ages)

# Filter with typed predicate
seniors_rdd = rdd.filter(lambda row: row.age >= 30)

# Convert back to DataFrame
seniors_df = seniors_rdd.toDF(["name", "age"])
print("Seniors:")
seniors_df.show()

# With explicit schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
seniors_df_typed = spark.createDataFrame(seniors_rdd, schema)
seniors_df_typed.printSchema()
```

## Summary

- **Encoders** convert between domain objects and Spark's binary format efficiently
- In PySpark, treat **schemas as type contracts** that define your data structure
- Use **Row objects** for access to individual records with attribute-style access
- The **RDD API** (`df.rdd.map()`) enables typed transformations when needed
- Implement **validation layers** to enforce type and data quality constraints
- **Schema evolution** patterns help manage changing data structures over time
- While Python cannot provide compile-time safety, disciplined schema usage achieves similar benefits

## Additional Resources

- [PySpark Row Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/row.html)
- [RDD to DataFrame Conversion](https://spark.apache.org/docs/latest/sql-getting-started.html#interoperating-with-rdds)
- [Structured Streaming Type Handling](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
