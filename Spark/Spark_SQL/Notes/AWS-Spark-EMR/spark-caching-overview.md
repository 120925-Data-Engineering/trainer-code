# Spark Caching Overview

## Learning Objectives
- Understand when and why to cache DataFrames
- Learn the difference between cache() and persist()
- Master storage levels and their trade-offs
- Know how to manage cached data effectively

## Why This Matters

Spark recomputes DataFrames from scratch each time an action is called. For iterative algorithms, machine learning pipelines, or multi-step transformations, this recomputation is wasteful. **Caching** stores intermediate results in memory (or disk) for reuse.

Effective caching can:
- Reduce execution time by 10x or more
- Prevent redundant computation in complex pipelines
- Enable interactive data exploration

However, improper caching wastes memory and can slow down jobs. Understanding caching strategies is essential for optimizing Spark applications in our Weekly Epic.

## The Concept

### The Caching Problem

Without caching:
```
Action 1: df.count()
  - Read source data
  - Apply transformations
  - Execute action

Action 2: df.show()
  - Read source data (again!)
  - Apply transformations (again!)
  - Execute action
```

With caching:
```
df.cache()

Action 1: df.count()
  - Read source data
  - Apply transformations
  - Store in memory
  - Execute action

Action 2: df.show()
  - Read from memory (fast!)
  - Execute action
```

### cache() vs persist()

| Method | Storage Level | Description |
|--------|--------------|-------------|
| `cache()` | MEMORY_AND_DISK | Convenient shorthand |
| `persist()` | Configurable | Full control over storage |
| `persist(level)` | Specified level | Choose memory, disk, or both |

### Storage Levels

| Storage Level | Memory | Disk | Serialized | Replicated |
|---------------|:------:|:----:|:----------:|:----------:|
| MEMORY_ONLY | Yes | No | No | No |
| MEMORY_AND_DISK | Yes | Spillover | No | No |
| MEMORY_ONLY_SER | Yes | No | Yes | No |
| MEMORY_AND_DISK_SER | Yes | Spillover | Yes | No |
| DISK_ONLY | No | Yes | Yes | No |
| MEMORY_ONLY_2 | Yes | No | No | Yes (2x) |
| OFF_HEAP | Off-heap | No | Yes | No |

### Choosing a Storage Level

```
              Start Here
                  |
    +-------------+-------------+
    |                           |
Does it fit        Does it fit with
in memory?         serialization?
    |                    |
   Yes                  Yes
    |                    |
MEMORY_ONLY       MEMORY_ONLY_SER
    |                    |
   No                   No
    |                    |
MEMORY_AND_DISK   MEMORY_AND_DISK_SER
    |                    |
   (spills to disk when memory full)
```

### Lazy Caching

Caching is **lazy**: calling `cache()` does not immediately store data. Data is cached when an action is triggered:

```python
df.cache()           # Marks for caching (no work yet)
df.count()           # First action: computes AND caches
df.show()            # Second action: reads from cache
```

## Code Example

### Basic Caching

```python
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Caching Demo").getOrCreate()

# Create a DataFrame with some computation
df = spark.range(10000000).withColumn("value", (spark.range(1).selectExpr("id * 2").first()[0]))

# Without caching: each action recomputes
print("Without caching:")
start = time.time()
df.count()
print(f"First count: {time.time() - start:.2f}s")

start = time.time()
df.count()
print(f"Second count: {time.time() - start:.2f}s")

# With caching: first action caches, subsequent are faster
print("\nWith caching:")
df_cached = df.cache()

start = time.time()
df_cached.count()  # Computes and caches
print(f"First count (caches): {time.time() - start:.2f}s")

start = time.time()
df_cached.count()  # Reads from cache
print(f"Second count (from cache): {time.time() - start:.2f}s")

# Clean up
df_cached.unpersist()
```

### Using persist() with Storage Levels

```python
from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder.appName("Persist Demo").getOrCreate()

df = spark.range(1000000)

# MEMORY_ONLY: Fastest, but may lose data if memory insufficient
df_memory = df.persist(StorageLevel.MEMORY_ONLY)

# MEMORY_AND_DISK: Safe default, spills to disk if needed
df_disk = df.persist(StorageLevel.MEMORY_AND_DISK)

# MEMORY_ONLY_SER: Less memory, more CPU (serialization)
df_ser = df.persist(StorageLevel.MEMORY_ONLY_SER)

# DISK_ONLY: When memory is precious
df_disk_only = df.persist(StorageLevel.DISK_ONLY)

# Trigger caching
df_memory.count()
df_disk.count()
df_ser.count()
df_disk_only.count()

# Check storage info
print("Cached DataFrames:")
for (id, rdd) in spark.sparkContext._jsc.getPersistentRDDs().items():
    print(f"  RDD ID: {id}, Storage Level: {rdd.getStorageLevel()}")

# Clean up
df_memory.unpersist()
df_disk.unpersist()
df_ser.unpersist()
df_disk_only.unpersist()
```

### Cache Management

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Cache Management").getOrCreate()

df = spark.range(1000000).cache()
df.count()  # Trigger caching

# Check if cached
print(f"Is cached: {df.is_cached}")
print(f"Storage level: {df.storageLevel}")

# View cache in Spark UI
print(f"Spark UI: {spark.sparkContext.uiWebUrl}")

# Unpersist when done
df.unpersist()
print(f"After unpersist - Is cached: {df.is_cached}")

# Clear all cached data
spark.catalog.clearCache()
print("All caches cleared")
```

### When to Cache

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("When to Cache").getOrCreate()

# Scenario 1: Multiple actions on same DataFrame
# CACHE THIS
raw_data = spark.read.parquet("large_dataset.parquet")
cleaned = raw_data.filter(col("valid") == True).cache()

# Used multiple times
print(f"Count: {cleaned.count()}")
print(f"Avg value: {cleaned.agg({'amount': 'avg'}).collect()}")
cleaned.write.parquet("output")

# Scenario 2: Iterative algorithms
# CACHE THIS
df = spark.range(1000).cache()
for i in range(10):
    df = df.withColumn(f"col_{i}", col("id") * i)
    # Without cache, each iteration would recompute from scratch

# Scenario 3: Interactive exploration
# CACHE THIS
exploration_df = spark.read.csv("data.csv").cache()
exploration_df.show(5)
exploration_df.describe().show()
exploration_df.groupBy("category").count().show()

# Scenario 4: Read once, use once
# DO NOT CACHE THIS
once_df = spark.read.json("events.json")
once_df.write.parquet("output")  # Only one use
```

### Checkpoint vs Cache

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Checkpoint").getOrCreate()

# Set checkpoint directory (required)
spark.sparkContext.setCheckpointDir("hdfs:///checkpoints")

df = spark.range(1000000)

# Cache: Keeps lineage, faster but lost on failure
df_cached = df.cache()

# Checkpoint: Breaks lineage, writes to reliable storage
# Use for very long lineages or fault tolerance
df_checkpointed = df.checkpoint()

# Comparison:
# Cache:
#   + Faster (memory/local disk)
#   + Keeps lineage for recomputation
#   - Lost if executor fails
#
# Checkpoint:
#   + Survives failures (reliable storage)
#   + Breaks lineage (good for long pipelines)
#   - Slower (network write)
#   - Requires checkpoint directory
```

## Summary

- **cache()** stores DataFrames in memory (with disk spillover) for reuse
- **persist(level)** allows choosing specific storage levels
- Caching is **lazy**: data is cached on first action, not when cache() is called
- **Storage levels** trade off between memory, disk, serialization, and replication
- Use **unpersist()** to free cached data when no longer needed
- Cache when: multiple actions on same data, iterative algorithms, interactive exploration
- Do not cache when: data is used only once, memory is constrained
- **Checkpoint** breaks lineage and writes to reliable storage for fault tolerance

## Additional Resources

- [Spark RDD Persistence](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
- [Storage Levels Documentation](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.StorageLevel.html)
- [Caching Best Practices (Databricks)](https://docs.databricks.com/en/optimizations/caching.html)
