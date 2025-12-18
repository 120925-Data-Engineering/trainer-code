# Working on DataFrames

## Learning Objectives
- Learn how to read data from various sources into DataFrames
- Understand how to write DataFrames to different output formats
- Master the end-to-end workflow of loading, manipulating, and saving data

## Why This Matters

Data engineering is fundamentally about moving data: extracting it from sources, transforming it, and loading it into destinations. The DataFrame API provides a unified interface for working with data regardless of where it comes from or where it is going.

In real-world pipelines, you will encounter data in many formats: CSV files from legacy systems, JSON from APIs, Parquet files in data lakes, and tables in databases. Mastering how to read and write these formats is essential for building robust ETL pipelines as part of our Weekly Epic.

## The Concept

### The DataFrameReader API

SparkSession provides a `read` property that returns a `DataFrameReader`:

```python
spark.read  # Returns DataFrameReader
```

The DataFrameReader follows a consistent pattern:

```python
df = spark.read \
    .format("format_name") \
    .option("key", "value") \
    .load("path")
```

Or using format-specific shortcuts:

```python
df = spark.read.csv("path")
df = spark.read.json("path")
df = spark.read.parquet("path")
```

### Supported Data Formats

| Format | Method | Description | Common Use |
|--------|--------|-------------|------------|
| CSV | `spark.read.csv()` | Comma-separated values | Legacy systems, exports |
| JSON | `spark.read.json()` | JSON files (one object per line) | API data, logs |
| Parquet | `spark.read.parquet()` | Columnar binary format | Data lakes, analytics |
| ORC | `spark.read.orc()` | Optimized columnar format | Hive ecosystems |
| Text | `spark.read.text()` | Plain text files | Log processing |
| JDBC | `spark.read.jdbc()` | Database connections | Enterprise databases |

### Common Read Options

#### CSV Options

| Option | Default | Description |
|--------|---------|-------------|
| `header` | false | First row contains column names |
| `inferSchema` | false | Automatically infer column types |
| `sep` | `,` | Field delimiter |
| `nullValue` | (empty) | String to interpret as null |
| `quote` | `"` | Quote character |
| `escape` | `\` | Escape character |
| `multiLine` | false | Allow multiline values |

#### JSON Options

| Option | Default | Description |
|--------|---------|-------------|
| `multiLine` | false | Parse multiline JSON |
| `primitivesAsString` | false | Read primitives as strings |
| `allowComments` | false | Allow comments in JSON |

### Schema Handling

**Infer Schema (Quick but slower)**
```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

**Define Schema (Faster, recommended for production)**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

df = spark.read.csv("data.csv", header=True, schema=schema)
```

### The DataFrameWriter API

DataFrames have a `write` property that returns a `DataFrameWriter`:

```python
df.write \
    .format("format_name") \
    .option("key", "value") \
    .mode("mode") \
    .save("path")
```

### Write Modes

| Mode | Description |
|------|-------------|
| `error` / `errorIfExists` | Throw error if data exists (default) |
| `overwrite` | Overwrite existing data |
| `append` | Append to existing data |
| `ignore` | Silently skip if data exists |

## Code Example

### Reading from Various Sources

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("Working on DataFrames") \
    .getOrCreate()

# ---------------------
# Reading CSV Files
# ---------------------

# Simple read with header
df_csv = spark.read.csv("employees.csv", header=True, inferSchema=True)

# With explicit options
df_csv_opts = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .option("nullValue", "NA") \
    .csv("employees.csv")

# With predefined schema (recommended for production)
employee_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

df_csv_schema = spark.read \
    .option("header", "true") \
    .schema(employee_schema) \
    .csv("employees.csv")

# ---------------------
# Reading JSON Files
# ---------------------

# Single-line JSON (default)
df_json = spark.read.json("events.json")

# Multi-line JSON (entire file is one JSON object)
df_json_multi = spark.read \
    .option("multiLine", "true") \
    .json("config.json")

# ---------------------
# Reading Parquet Files
# ---------------------

# Parquet is self-describing (schema included in file)
df_parquet = spark.read.parquet("sales_data.parquet")

# Reading multiple parquet files
df_parquet_multiple = spark.read.parquet("data/year=2023/month=*/*.parquet")

# ---------------------
# Reading from Directory
# ---------------------

# Read all CSV files in a directory
df_dir = spark.read.csv("data/csv_files/", header=True, inferSchema=True)

# Read with glob patterns
df_glob = spark.read.parquet("data/*/output/*.parquet")
```

### Writing DataFrames

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Writing DataFrames").getOrCreate()

# Create sample DataFrame
data = [
    (1, "Alice", "Engineering", 75000.0),
    (2, "Bob", "Marketing", 65000.0),
    (3, "Charlie", "Engineering", 80000.0)
]
df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

# ---------------------
# Writing CSV
# ---------------------

df.write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("output/employees_csv")

# Single file output (small data only)
df.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv("output/employees_single_csv")

# ---------------------
# Writing JSON
# ---------------------

df.write \
    .mode("overwrite") \
    .json("output/employees_json")

# ---------------------
# Writing Parquet (Recommended for data lakes)
# ---------------------

df.write \
    .mode("overwrite") \
    .parquet("output/employees_parquet")

# With compression
df.write \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .parquet("output/employees_compressed")

# ---------------------
# Partitioned Writing
# ---------------------

# Partition by department (creates subdirectories)
df.write \
    .partitionBy("department") \
    .mode("overwrite") \
    .parquet("output/employees_partitioned")

# Result structure:
# output/employees_partitioned/
#   department=Engineering/
#     part-00000.parquet
#   department=Marketing/
#     part-00000.parquet
```

### End-to-End ETL Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, current_timestamp

spark = SparkSession.builder.appName("ETL Example").getOrCreate()

# EXTRACT: Read raw data
raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("raw_data/sales.csv")

print("Raw data:")
raw_df.show(5)
print("Row count:", raw_df.count())

# TRANSFORM: Clean and enrich data
transformed_df = raw_df \
    .filter(col("amount").isNotNull()) \
    .filter(col("amount") > 0) \
    .withColumn("region", upper(col("region"))) \
    .withColumn("amount_category",
        when(col("amount") < 100, "SMALL")
        .when(col("amount") < 1000, "MEDIUM")
        .otherwise("LARGE")) \
    .withColumn("processed_at", current_timestamp())

print("\nTransformed data:")
transformed_df.show(5)

# LOAD: Write processed data
transformed_df.write \
    .partitionBy("region") \
    .mode("overwrite") \
    .parquet("processed_data/sales")

print("\nData written to processed_data/sales")

spark.stop()
```

## Summary

- Use `spark.read` to access the DataFrameReader for loading data
- Common formats include CSV, JSON, Parquet, ORC, and JDBC
- Always define schemas explicitly for production workflows instead of relying on `inferSchema`
- Use `df.write` to access the DataFrameWriter for saving data
- Choose the appropriate write mode: `error`, `overwrite`, `append`, or `ignore`
- Parquet is the recommended format for data lakes due to columnar storage and built-in compression
- Use `partitionBy()` to organize output data into subdirectories for efficient querying

## Additional Resources

- [Data Sources in Spark SQL](https://spark.apache.org/docs/latest/sql-data-sources.html)
- [Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [CSV Files](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)
