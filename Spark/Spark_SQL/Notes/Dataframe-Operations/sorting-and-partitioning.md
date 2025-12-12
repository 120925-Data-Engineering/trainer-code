# Sorting and Partitioning

## Learning Objectives
- Master sorting operations with orderBy and sort
- Understand DataFrame partitioning with repartition and coalesce
- Learn when and how to apply partitioning strategies for performance

## Why This Matters

Performance optimization is a critical skill for data engineers. Two key techniques for improving Spark job performance are:

- **Sorting**: Organizing data for efficient queries, reports, and downstream processing
- **Partitioning**: Controlling how data is distributed across the cluster

Poor partitioning leads to data skew, where some nodes do much more work than others. Proper sorting and partitioning strategies directly impact job execution time, making this essential knowledge for our Weekly Epic of *Mastering Spark SQL and DataFrames*.

## The Concept

### Sorting Operations

| Method | Description |
|--------|-------------|
| `orderBy(cols)` | Sort by one or more columns |
| `sort(cols)` | Alias for orderBy |
| `sortWithinPartitions(cols)` | Sort within each partition (no shuffle) |

**Ascending vs Descending:**
```python
df.orderBy("column")           # Ascending (default)
df.orderBy(col("column").asc())    # Explicit ascending
df.orderBy(col("column").desc())   # Descending
```

### Partitioning Operations

| Method | Description | Use Case |
|--------|-------------|----------|
| `repartition(n)` | Increase or decrease partitions (full shuffle) | Increase parallelism |
| `repartition(n, cols)` | Repartition by column values | Co-locate related data |
| `coalesce(n)` | Reduce partitions (no shuffle) | Reduce output files |

### Understanding Partitions

```
DataFrame with 4 partitions:
+------------+  +------------+  +------------+  +------------+
| Partition 1|  | Partition 2|  | Partition 3|  | Partition 4|
| Rows 1-100 |  | Rows 101-200| | Rows 201-300| | Rows 301-400|
+------------+  +------------+  +------------+  +------------+
      |               |               |               |
   Worker 1        Worker 2        Worker 3        Worker 4
```

- More partitions = more parallelism (up to available cores)
- Fewer partitions = less overhead, potentially larger files
- Right-sizing partitions is key to performance

### When to Use Each

| Scenario | Use |
|----------|-----|
| Too few partitions (large partitions) | `repartition(n)` to increase |
| Too many small partitions | `coalesce(n)` to decrease |
| Pre-join optimization | `repartition(n, join_key)` |
| Writing fewer output files | `coalesce(1)` or `coalesce(n)` |
| Data skew issues | `repartition()` with good key |

## Code Example

### Sorting Operations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, asc

spark = SparkSession.builder.appName("Sorting").getOrCreate()

# Sample data
data = [
    ("Alice", "Engineering", 75000),
    ("Bob", "Marketing", 65000),
    ("Charlie", "Engineering", 80000),
    ("Diana", "Sales", 55000),
    ("Eve", "Marketing", 70000)
]

df = spark.createDataFrame(data, ["name", "department", "salary"])

print("Original DataFrame:")
df.show()

# Sort by single column (ascending by default)
print("Sorted by salary (ascending):")
df.orderBy("salary").show()

# Sort descending
print("Sorted by salary (descending):")
df.orderBy(col("salary").desc()).show()
# Or equivalently:
df.orderBy(desc("salary")).show()

# Sort by multiple columns
print("Sorted by department, then salary descending:")
df.orderBy("department", desc("salary")).show()

# Using sort() - alias for orderBy()
df.sort("department", "salary").show()

# Null handling in sorting
data_with_nulls = [
    ("Alice", 100),
    ("Bob", None),
    ("Charlie", 50)
]
df_nulls = spark.createDataFrame(data_with_nulls, ["name", "value"])

print("Nulls first:")
df_nulls.orderBy(col("value").asc_nulls_first()).show()

print("Nulls last:")
df_nulls.orderBy(col("value").asc_nulls_last()).show()
```

### Partitioning Operations

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.appName("Partitioning").getOrCreate()

# Create DataFrame with 200 rows
df = spark.range(200)

print(f"Initial partition count: {df.rdd.getNumPartitions()}")

# Add partition ID to see distribution
df_with_id = df.withColumn("partition_id", spark_partition_id())
print("Data distribution across partitions:")
df_with_id.groupBy("partition_id").count().orderBy("partition_id").show()

# Repartition to more partitions (causes shuffle)
df_8 = df.repartition(8)
print(f"After repartition(8): {df_8.rdd.getNumPartitions()} partitions")

# Coalesce to fewer partitions (no shuffle)
df_2 = df_8.coalesce(2)
print(f"After coalesce(2): {df_2.rdd.getNumPartitions()} partitions")

# Important: coalesce cannot increase partitions
df_attempt = df_2.coalesce(10)
print(f"coalesce(10) from 2: {df_attempt.rdd.getNumPartitions()} partitions")
# Still 2! Use repartition to increase
```

### Partitioning by Column

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.appName("Partition By Column").getOrCreate()

data = [
    ("Alice", "Engineering", 75000),
    ("Bob", "Marketing", 65000),
    ("Charlie", "Engineering", 80000),
    ("Diana", "Sales", 55000),
    ("Eve", "Marketing", 70000),
    ("Frank", "Engineering", 72000)
]

df = spark.createDataFrame(data, ["name", "department", "salary"])

# Repartition by department
df_by_dept = df.repartition(3, "department")

# See how data is distributed
df_by_dept.withColumn("partition_id", spark_partition_id()) \
          .orderBy("partition_id", "department") \
          .show()

# All rows with same department value are in same partition
# This is useful for:
# 1. Joining on department (co-located data)
# 2. Writing partitioned output
# 3. Aggregating by department
```

### sortWithinPartitions

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.appName("Sort Within Partitions").getOrCreate()

data = [
    ("Alice", 30), ("Bob", 25), ("Charlie", 35),
    ("Diana", 28), ("Eve", 32), ("Frank", 27)
]

df = spark.createDataFrame(data, ["name", "age"])

# Repartition into 2 partitions
df_partitioned = df.repartition(2)

# Sort within each partition (no shuffle between partitions)
df_sorted = df_partitioned.sortWithinPartitions("age")

# Compare with global sort
print("sortWithinPartitions (local sort, no shuffle):")
df_sorted.withColumn("partition_id", spark_partition_id()).show()

print("orderBy (global sort, requires shuffle):")
df_partitioned.orderBy("age").withColumn("partition_id", spark_partition_id()).show()

# sortWithinPartitions is faster but not globally sorted
# Use when you need sorted data within each partition only
```

### Practical: Optimizing Output Files

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Output Optimization").getOrCreate()

# Imagine a large DataFrame
large_df = spark.range(1000000)

print(f"Initial partitions: {large_df.rdd.getNumPartitions()}")

# Writing with many partitions creates many small files
# large_df.write.mode("overwrite").parquet("output/many_files")

# Use coalesce to reduce output files
optimized_df = large_df.coalesce(4)
print(f"Coalesced partitions: {optimized_df.rdd.getNumPartitions()}")

# Now writing creates only 4 files
# optimized_df.write.mode("overwrite").parquet("output/few_files")

# For partitioned writes, control files per partition
# large_df.repartition(4, "category").write.partitionBy("category").parquet("output")
```

## Summary

- **orderBy()** and **sort()** sort DataFrames globally across all partitions
- **sortWithinPartitions()** sorts within each partition without shuffle (faster but not globally sorted)
- **repartition(n)** changes partition count with a full shuffle (can increase or decrease)
- **repartition(n, cols)** partitions by column values for co-location
- **coalesce(n)** reduces partitions without shuffle (cannot increase)
- Right-sizing partitions balances parallelism with overhead
- Use coalesce before writing to control output file count

## Additional Resources

- [Spark Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Understanding Partitioning](https://www.databricks.com/blog/2020/03/17/how-to-use-spark-rdd-partition.html)
- [PySpark Repartition Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html)
