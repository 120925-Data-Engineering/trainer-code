# Introduction to RDD

## Learning Objectives
- Understand what Resilient Distributed Datasets (RDDs) are
- Explain the properties of RDDs: immutability, partitioning, and lineage
- Understand lazy evaluation and its benefits
- Create RDDs from various data sources

## Why This Matters
RDDs are the foundational data structure in Apache Spark. While higher-level APIs like DataFrames are more commonly used today, understanding RDDs is essential because they underpin all Spark operations. RDDs give you fine-grained control over distributed data processing and help you understand how Spark achieves fault tolerance and parallel execution.

Building on what you learned yesterday about the Spark ecosystem, today you will dive into the core abstraction that makes distributed processing possible.

## The Concept

### What is an RDD?

A **Resilient Distributed Dataset (RDD)** is an immutable, partitioned collection of elements that can be processed in parallel across a cluster.

Breaking down the name:
- **Resilient:** Fault-tolerant through lineage tracking
- **Distributed:** Data is partitioned across multiple nodes
- **Dataset:** A collection of records/elements

### Key Properties of RDDs

#### 1. Immutability
RDDs cannot be modified once created. Instead, transformations create new RDDs.

```python
# Original RDD is never modified
original = sc.parallelize([1, 2, 3, 4, 5])
doubled = original.map(lambda x: x * 2)  # New RDD created

# original still contains [1, 2, 3, 4, 5]
# doubled contains [2, 4, 6, 8, 10]
```

**Benefits of immutability:**
- Simplifies parallel processing (no race conditions)
- Enables lineage tracking for fault recovery
- Supports functional programming patterns

#### 2. Partitioning
RDDs are divided into partitions, each processed on different nodes.

```
RDD: [1, 2, 3, 4, 5, 6, 7, 8]

Partition 0: [1, 2]     -> Worker Node 1
Partition 1: [3, 4]     -> Worker Node 2
Partition 2: [5, 6]     -> Worker Node 3
Partition 3: [7, 8]     -> Worker Node 4
```

The number of partitions determines parallelism. More partitions enable more parallel tasks, up to the available cores.

#### 3. Lineage (Fault Tolerance)
Spark tracks the sequence of transformations used to build each RDD. If a partition is lost (node failure), Spark can recompute it from the original data.

```
textFile("data.txt")
    |
   map(parse_line)
    |
  filter(is_valid)
    |
   reduceByKey(sum)
```

This lineage graph is the "recipe" to rebuild any lost partition.

### Lazy Evaluation

Transformations on RDDs are **lazy** - they do not execute immediately. Spark builds a computation plan and only executes when an **action** is called.

```python
# These lines define transformations (nothing executed yet)
rdd = sc.textFile("large_file.txt")
words = rdd.flatMap(lambda line: line.split())
filtered = words.filter(lambda word: len(word) > 5)

# Only this line triggers actual computation
count = filtered.count()  # ACTION - now Spark executes the plan
```

**Benefits of lazy evaluation:**
- Enables query optimization
- Reduces unnecessary computations
- Allows pipelining of operations

### Creating RDDs

There are two ways to create RDDs:

#### 1. From Collections (Parallelizing)
```python
# Create from a Python list
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Specify number of partitions
rdd = sc.parallelize(data, numSlices=4)
```

#### 2. From External Storage
```python
# From a text file
rdd = sc.textFile("path/to/file.txt")

# From multiple files using wildcards
rdd = sc.textFile("path/to/logs/*.log")

# From HDFS
rdd = sc.textFile("hdfs://namenode:9000/data/input.txt")
```

### RDD Operations Overview

RDDs support two types of operations:

| Type | Behavior | Examples |
|------|----------|----------|
| **Transformations** | Lazy, returns new RDD | map, filter, flatMap |
| **Actions** | Eager, triggers computation | count, collect, reduce |

We will explore these operations in depth throughout this week.

## Code Example

```python
from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local[*]", "RDDIntroduction")

# Create an RDD from a list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# Check partition count
print(f"Number of partitions: {numbers.getNumPartitions()}")

# View data in each partition
def show_partition(index, iterator):
    return [(index, list(iterator))]

partitioned_data = numbers.mapPartitionsWithIndex(show_partition).collect()
print("Data by partition:")
for part_id, data in partitioned_data:
    print(f"  Partition {part_id}: {data}")

# Demonstrate immutability
evens = numbers.filter(lambda x: x % 2 == 0)
print(f"\nOriginal RDD count: {numbers.count()}")
print(f"Filtered RDD count: {evens.count()}")

# Show lineage
print(f"\nLineage (Debug String):")
print(evens.toDebugString().decode('utf-8'))

sc.stop()
```

**Output:**
```
Number of partitions: 4

Data by partition:
  Partition 0: [1, 2]
  Partition 1: [3, 4, 5]
  Partition 2: [6, 7]
  Partition 3: [8, 9, 10]

Original RDD count: 10
Filtered RDD count: 5

Lineage (Debug String):
(4) PythonRDD[2] at RDD at PythonRDD.scala:53 []
 |  ParallelCollectionRDD[0] at parallelize at <stdin>:1 []
```

## Summary
- RDDs are immutable, distributed collections partitioned across cluster nodes
- Immutability enables safe parallel processing and fault tolerance
- Partitioning determines the degree of parallelism
- Lineage tracking allows Spark to recover lost partitions without replication
- Lazy evaluation defers computation until an action is called
- RDDs can be created from collections or external storage

## Additional Resources
- [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [PySpark RDD API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html)
- [Understanding Spark Partitioning](https://spark.apache.org/docs/latest/rdd-programming-guide.html#parallelized-collections)
