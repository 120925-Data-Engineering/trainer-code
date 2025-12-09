# Executors

## Learning Objectives
- Understand the executor role in Spark architecture
- Explain task execution within executors
- Manage executor memory effectively
- Monitor executor performance and lifecycle

## Why This Matters
Executors are the workhorses of Spark applications. They run tasks, store cached data, and perform the actual computation. Understanding how executors work helps you configure resources appropriately, troubleshoot performance issues, and optimize job execution.

## The Concept

### What are Executors?

Executors are JVM processes that run on worker nodes in a Spark cluster. Each executor:
- Runs tasks assigned by the driver
- Stores data for caching
- Reports results and status to the driver
- Manages memory for computation and storage

### Executor Architecture

```
+--------------------------------------------------+
|                    Executor                       |
|   +------------------------------------------+   |
|   |              JVM Process                  |   |
|   |  +------------+  +------------+           |   |
|   |  |   Task 1   |  |   Task 2   |  <-- Slots|   |
|   |  +------------+  +------------+           |   |
|   |                                           |   |
|   |  +------------------------------------+   |   |
|   |  |          Memory Pool               |   |   |
|   |  |  +-----------+  +-----------+      |   |   |
|   |  |  | Execution |  |  Storage  |      |   |   |
|   |  |  +-----------+  +-----------+      |   |   |
|   |  +------------------------------------+   |   |
|   |                                           |   |
|   |  +------------------------------------+   |   |
|   |  |        Block Manager                |   |   |
|   |  |  (cached RDDs, shuffle blocks)     |   |   |
|   |  +------------------------------------+   |   |
|   +------------------------------------------+   |
+--------------------------------------------------+
```

### Task Execution

Each executor can run multiple tasks concurrently:

```
Executor (4 cores)
    |
    +-- Task slot 1 --> Processing partition 0
    +-- Task slot 2 --> Processing partition 1
    +-- Task slot 3 --> Processing partition 2
    +-- Task slot 4 --> Processing partition 3
```

**Task slots = executor cores (spark.executor.cores)**

### Executor Lifecycle

```
1. Application starts
       |
       v
2. Driver requests executors from cluster manager
       |
       v
3. Cluster manager allocates containers
       |
       v
4. Executors launch and register with driver
       |
       v
5. Driver sends tasks to executors
       |
       v
6. Executors process tasks, report results
       |
       v
7. Application completes, executors terminate
```

### Executor Memory Layout

```
+------------------------------------------+
|           Total Executor Memory          |
|  (spark.executor.memory = 8g)            |
+------------------------------------------+
|                                          |
|  +------------------------------------+  |
|  |        Reserved Memory (300MB)     |  |
|  +------------------------------------+  |
|                                          |
|  +------------------------------------+  |
|  |         User Memory (~40%)         |  |
|  |  (data structures, UDFs, etc.)     |  |
|  +------------------------------------+  |
|                                          |
|  +------------------------------------+  |
|  |      Spark Memory (~60%)           |  |
|  |  +------------+ +------------+     |  |
|  |  | Execution  | |  Storage   |     |  |
|  |  | (shuffles, | | (cached    |     |  |
|  |  |  joins)    | |  RDDs)     |     |  |
|  |  +------------+ +------------+     |  |
|  +------------------------------------+  |
|                                          |
+------------------------------------------+
```

### Configuring Executors

#### Number of Executors
```python
spark = SparkSession.builder \
    .config("spark.executor.instances", "10") \
    .getOrCreate()
```

Or via spark-submit:
```bash
spark-submit --num-executors 10 app.py
```

#### Executor Memory
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()
```

#### Executor Cores
```python
spark = SparkSession.builder \
    .config("spark.executor.cores", "4") \
    .getOrCreate()
```

### Resource Planning

**Rule of thumb for sizing:**

```
Total cores needed = Data partitions / Target parallelism factor

Example:
- 100 partitions, want 4x parallelism
- Need 25 executor cores
- With 4 cores per executor -> 6-7 executors
```

**Memory considerations:**
- Executor memory = (Container memory - overhead)
- Overhead = max(384MB, 0.1 * executor memory)

```
Container memory = 4g
Overhead = max(384MB, 0.4g) = 0.4g
Executor memory = 4g - 0.4g = 3.6g
```

### Dynamic Allocation

Let Spark adjust executor count based on workload:

```python
spark = SparkSession.builder \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.dynamicAllocation.initialExecutors", "5") \
    .getOrCreate()
```

### Monitoring Executors

#### Via Spark UI
Navigate to the Executors tab:
- Active executors count
- Memory usage per executor
- Tasks completed/failed
- Shuffle read/write
- Storage memory used

#### Via SparkContext
```python
# Get executor information
sc = spark.sparkContext

# Number of executors (excluding driver)
executors = sc._jsc.sc().getExecutorMemoryStatus().size() - 1
print(f"Active executors: {executors}")
```

### Common Executor Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| Executor OOM | Task failures with OutOfMemoryError | Increase executor memory or reduce partition size |
| Executors lost | "Executor lost" in logs | Check node health, increase memory overhead |
| Slow tasks | A few tasks much slower than others | Data skew, increase partitions |
| Underutilization | Low CPU/memory usage | Reduce executor size, increase parallelism |

### Executor Settings Comparison

| Scenario | Executors | Memory | Cores |
|----------|-----------|--------|-------|
| Small jobs | 2-4 | 2g | 2 |
| Medium ETL | 10-20 | 4g | 4 |
| Large jobs | 50-100 | 8g | 4 |
| ML workloads | 20-50 | 16g | 4 |

## Code Example

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

def demonstrate_executors():
    """Demonstrate executor configuration and monitoring."""
    
    spark = SparkSession.builder \
        .appName("ExecutorDemo") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
    print("=" * 60)
    print("EXECUTOR INFORMATION")
    print("=" * 60)
    
    # Get configuration
    print("\nConfiguration:")
    print(f"  Master: {sc.master}")
    print(f"  Default parallelism: {sc.defaultParallelism}")
    
    executor_memory = spark.conf.get("spark.executor.memory", "Not set")
    print(f"  Executor memory: {executor_memory}")
    
    # In local mode, we simulate executor behavior
    print("\nNote: In local mode, driver acts as executor")
    
    # Create sample data
    print("\n" + "=" * 60)
    print("TASK DISTRIBUTION")
    print("=" * 60)
    
    data = list(range(1000000))
    rdd = sc.parallelize(data, numSlices=8)
    
    print(f"\nRDD partitions: {rdd.getNumPartitions()}")
    print("Each partition will be processed by a task")
    
    # Show partition distribution
    def count_partition(index, iterator):
        count = sum(1 for _ in iterator)
        return [(index, count)]
    
    partition_counts = rdd.mapPartitionsWithIndex(count_partition).collect()
    
    print("\nPartition distribution:")
    for part_id, count in partition_counts:
        print(f"  Partition {part_id}: {count:,} elements")
    
    # Demonstrate parallel processing
    print("\n" + "=" * 60)
    print("PARALLEL EXECUTION")
    print("=" * 60)
    
    def slow_operation(x):
        """Simulate CPU-intensive work."""
        result = x
        for _ in range(1000):
            result = (result * 17 + 31) % 10000
        return result
    
    # Time parallel execution
    start = time.time()
    result = rdd.map(slow_operation).reduce(lambda a, b: a + b)
    parallel_time = time.time() - start
    
    print(f"\nParallel execution time: {parallel_time:.2f}s")
    print(f"Result: {result}")
    print(f"Tasks ran on {rdd.getNumPartitions()} partitions")
    
    # Memory usage demonstration
    print("\n" + "=" * 60)
    print("MEMORY USAGE")
    print("=" * 60)
    
    # Create DataFrame for caching demo
    df = spark.range(100000).select(
        F.col("id"),
        F.rand(42).alias("value"),
        F.lit("sample data").alias("text")
    )
    
    # Cache the DataFrame
    df.cache()
    df.count()  # Materialize cache
    
    print("\nDataFrame cached in memory")
    print(f"Record count: {df.count()}")
    
    # Show storage info
    print("\nStorage Level: MEMORY_AND_DISK")
    
    # Clean up
    df.unpersist()
    
    print("\n" + "=" * 60)
    print("EXECUTOR GUIDELINES")
    print("=" * 60)
    
    guidelines = """
    Resource Planning Rules:
    
    1. Executor Memory:
       - 2-4g for small jobs
       - 4-8g for typical ETL
       - 8-16g for memory-intensive (ML, large joins)
    
    2. Executor Cores:
       - 2-5 cores per executor is optimal
       - More cores = more concurrent tasks
       - But also more memory contention
    
    3. Number of Executors:
       - Start with: total_partitions / executor_cores
       - Adjust based on data size
       - Consider dynamic allocation
    
    4. Memory Overhead:
       - Add 10% for YARN/K8s overhead
       - Container memory = executor memory + overhead
    
    5. Parallelism:
       - Aim for 2-4 partitions per core
       - sql.shuffle.partitions = 2-4 * total_cores
    """
    print(guidelines)
    
    spark.stop()
    print("Demo complete.")

if __name__ == "__main__":
    demonstrate_executors()
```

**Output:**
```
============================================================
EXECUTOR INFORMATION
============================================================

Configuration:
  Master: local[*]
  Default parallelism: 8
  Executor memory: 2g

Note: In local mode, driver acts as executor

============================================================
TASK DISTRIBUTION
============================================================

RDD partitions: 8
Each partition will be processed by a task

Partition distribution:
  Partition 0: 125,000 elements
  Partition 1: 125,000 elements
  Partition 2: 125,000 elements
  Partition 3: 125,000 elements
  Partition 4: 125,000 elements
  Partition 5: 125,000 elements
  Partition 6: 125,000 elements
  Partition 7: 125,000 elements

============================================================
PARALLEL EXECUTION
============================================================

Parallel execution time: 2.34s
Result: 4999500000
Tasks ran on 8 partitions

============================================================
MEMORY USAGE
============================================================

DataFrame cached in memory
Record count: 100000

Storage Level: MEMORY_AND_DISK

============================================================
EXECUTOR GUIDELINES
============================================================
...
```

## Summary
- Executors are JVM processes that run tasks and store cached data
- Each executor has task slots equal to its configured cores
- Executor memory is divided into reserved, user, and Spark pools
- Configure executor count, memory, and cores based on workload
- Dynamic allocation adjusts executors based on demand
- Monitor executors via Spark UI for memory and task metrics
- Common issues include OOM errors, executor loss, and underutilization

## Additional Resources
- [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Memory Management](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)
- [Dynamic Allocation](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation)
