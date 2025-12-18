# Introduction to Datasets

## Learning Objectives
- Understand what Datasets are and how they differ from DataFrames
- Learn about type safety and when it matters
- Recognize when to use Datasets vs DataFrames in your projects

## Why This Matters

While DataFrames provide a powerful API for structured data, they are essentially untyped: the schema is only enforced at runtime. **Datasets** add compile-time type safety, meaning errors are caught before your job runs rather than during execution.

Understanding the Dataset API is important because:
- Many Spark applications in Scala and Java use Datasets extensively
- The concepts of type safety apply to good software engineering practices
- Interview questions often compare DataFrames and Datasets
- Certain use cases benefit from typed operations

This topic extends our Weekly Epic of *Mastering Spark SQL and DataFrames* by introducing the typed counterpart to DataFrames.

## The Concept

### DataFrame vs. Dataset

In Spark, the relationship between DataFrame and Dataset is:

```
Dataset[Row] = DataFrame
```

A **DataFrame** is actually a **Dataset of Row objects**. The Row type is generic, so column types are only checked at runtime.

A **Dataset** with a specific type (like `Dataset[Person]` in Scala) provides compile-time type checking.

### Type Safety Comparison

| Aspect | DataFrame (Dataset[Row]) | Typed Dataset |
|--------|-------------------------|---------------|
| Column Types | Checked at runtime | Checked at compile time |
| Error Discovery | When job runs | When code compiles |
| API Style | Select by string names | Typed methods |
| Language Support | Python, Scala, Java, R | Scala, Java only |
| Performance | Optimized equally | Optimized equally |

### The Python Reality

**Important**: In PySpark, there is no typed Dataset API. Python's dynamic typing does not support compile-time type checking. In PySpark:
- DataFrame is the only API
- Type hints can document expected types but are not enforced
- Runtime errors still occur for type mismatches

However, understanding Dataset concepts is valuable because:
1. You may work with Scala/Java Spark code
2. The concepts inform good data engineering practices
3. You can use Python type hints and validation patterns

### Conceptual Benefits of Type Safety

Even without Scala's compile-time benefits, thinking in typed terms helps:

```
Untyped Thinking:           Typed Thinking:
"This column has data"      "This column is an Integer"
"Select the name"           "Access person.name: String"
"Add the values"            "Sum integers, not strings"
```

### Encoders

In Scala/Java, Datasets use **Encoders** to convert between JVM objects and Spark's internal binary format. Encoders enable:
- Efficient serialization without Java serialization overhead
- Type-safe operations on domain objects
- Schema inference from case classes

In Python, you simulate this with schemas and careful type handling.

## Code Example

### Python: Working with Typed Patterns

While Python does not have true Datasets, you can use typed patterns:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Typed Patterns").getOrCreate()

# Define explicit schema (simulates typed approach)
person_schema = StructType([
    StructField("name", StringType(), False),  # Not nullable
    StructField("age", IntegerType(), False),
    StructField("city", StringType(), True)    # Nullable
])

# Create DataFrame with schema enforcement
data = [("Alice", 30, "NYC"), ("Bob", 25, "LA"), ("Charlie", 35, None)]
df = spark.createDataFrame(data, person_schema)

print("Schema-enforced DataFrame:")
df.printSchema()
df.show()

# Type errors caught at schema creation
try:
    bad_data = [("Alice", "thirty", "NYC")]  # Age is string, not int
    spark.createDataFrame(bad_data, person_schema).show()
except Exception as e:
    print(f"Type error caught: {type(e).__name__}")
```

### Python Type Hints (Documentation)

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import List, Tuple

spark = SparkSession.builder.appName("Type Hints").getOrCreate()

# Type hints document expected types (not enforced at runtime)
def create_person_df(
    data: List[Tuple[str, int, str]]
) -> DataFrame:
    """Create a Person DataFrame.
    
    Args:
        data: List of (name, age, city) tuples
        
    Returns:
        DataFrame with person schema
    """
    schema = StructType([
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("city", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)

def get_adults(df: DataFrame) -> DataFrame:
    """Filter to adults only (age >= 18)."""
    return df.filter(df.age >= 18)

# Usage
people_data = [("Alice", 30, "NYC"), ("Bob", 17, "LA")]
people_df = create_person_df(people_data)
adults_df = get_adults(people_df)

print("Adults only:")
adults_df.show()
```

### Conceptual Scala Dataset Example (For Reference)

This is how typed Datasets work in Scala (shown for understanding):

```scala
// Scala example (for conceptual understanding)
// NOT runnable in PySpark

case class Person(name: String, age: Int, city: Option[String])

val spark = SparkSession.builder.appName("Typed Dataset").getOrCreate()
import spark.implicits._

// Create typed Dataset
val people: Dataset[Person] = Seq(
  Person("Alice", 30, Some("NYC")),
  Person("Bob", 25, Some("LA"))
).toDS()

// Compile-time type safety
val names: Dataset[String] = people.map(_.name)  // Returns Dataset[String]
val adults: Dataset[Person] = people.filter(_.age >= 18)  // Type preserved

// This would NOT compile (caught before runtime):
// val wrong = people.map(_.nonExistentField)  // Compile error!
```

### Simulating Type Safety with Validation

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Validation").getOrCreate()

def validate_person_schema(df: DataFrame) -> bool:
    """Validate that DataFrame matches expected Person schema."""
    expected_fields = {
        "name": StringType(),
        "age": IntegerType(),
        "city": StringType()
    }
    
    for field in df.schema.fields:
        if field.name in expected_fields:
            if field.dataType != expected_fields[field.name]:
                print(f"Type mismatch: {field.name} is {field.dataType}, expected {expected_fields[field.name]}")
                return False
    
    return True

def validate_data_quality(df: DataFrame) -> DataFrame:
    """Validate data constraints and return only valid rows."""
    return df.filter(
        (col("name").isNotNull()) &
        (col("age") >= 0) &
        (col("age") <= 150)
    )

# Test validation
data = [("Alice", 30, "NYC"), ("Bob", -5, "LA"), (None, 25, "Chicago")]
df = spark.createDataFrame(data, ["name", "age", "city"])

print("Original data:")
df.show()

print("Schema valid:", validate_person_schema(df))

print("Valid rows only:")
validate_data_quality(df).show()
```

## Summary

- **DataFrame** is `Dataset[Row]` with runtime type checking only
- **Typed Datasets** (Scala/Java) provide compile-time type safety
- PySpark does not have typed Datasets due to Python's dynamic nature
- Use **explicit schemas** in PySpark to enforce types at DataFrame creation
- Use **type hints** to document expected types (not enforced)
- Implement **validation functions** to catch type and data quality issues
- Understanding Dataset concepts is valuable for working with Scala/Java code and interviews

## Additional Resources

- [Spark Datasets Documentation](https://spark.apache.org/docs/latest/sql-getting-started.html#datasets-and-dataframes)
- [Scala Case Classes and Datasets](https://spark.apache.org/docs/latest/sql-getting-started.html#creating-datasets)
- [Python Type Hints PEP 484](https://peps.python.org/pep-0484/)
