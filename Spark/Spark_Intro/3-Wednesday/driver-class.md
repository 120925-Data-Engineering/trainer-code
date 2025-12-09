# Driver Class

## Learning Objectives
- Understand the SparkContext and its role as the driver
- Explain driver program responsibilities
- Manage the Spark application lifecycle properly
- Handle common driver-related issues

## Why This Matters
The driver is the control center of every Spark application. It creates the SparkContext, coordinates job execution, and manages resources. Understanding the driver's role helps you write reliable applications and debug issues when they arise.

## The Concept

### What is the Driver?

The driver is the process that runs your main program. It performs several critical functions:

1. **Creates SparkContext:** The entry point to Spark functionality
2. **Builds the DAG:** Translates transformations into an execution plan
3. **Schedules Tasks:** Assigns work to executors
4. **Coordinates Execution:** Manages job, stage, and task progress
5. **Collects Results:** Gathers output from executors

```
+---------------------------------------------+
|                  DRIVER                      |
|  +---------------------------------------+   |
|  |           Your Application            |   |
|  |   - main() function                   |   |
|  |   - Creates SparkContext              |   |
|  |   - Defines transformations           |   |
|  |   - Triggers actions                  |   |
|  +---------------------------------------+   |
|                     |                        |
|  +---------------------------------------+   |
|  |            SparkContext               |   |
|  |   - Connects to cluster manager       |   |
|  |   - Requests resources                |   |
|  |   - Manages job execution             |   |
|  +---------------------------------------+   |
+---------------------------------------------+
              |             |
    +---------+             +---------+
    |                                 |
+---+---+                         +---+---+
|Executor|                         |Executor|
+--------+                         +--------+
```

### SparkContext

SparkContext is the legacy entry point to Spark (pre-2.0). It remains the foundation for RDD operations.

```python
from pyspark import SparkContext, SparkConf

# Create configuration
conf = SparkConf().setAppName("MyApp").setMaster("local[*]")

# Create SparkContext
sc = SparkContext(conf=conf)

# Use SparkContext for RDD operations
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Stop when done
sc.stop()
```

**Key SparkContext methods:**

| Method | Description |
|--------|-------------|
| `parallelize(data)` | Create RDD from collection |
| `textFile(path)` | Load text file as RDD |
| `broadcast(value)` | Create broadcast variable |
| `accumulator(value)` | Create accumulator |
| `stop()` | Shut down SparkContext |

### SparkSession (Modern Entry Point)

SparkSession (Spark 2.0+) is the unified entry point that combines SparkContext, SQLContext, and HiveContext.

```python
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.some.option", "value") \
    .getOrCreate()

# Access underlying SparkContext
sc = spark.sparkContext

# Use for both RDD and DataFrame operations
rdd = sc.parallelize([1, 2, 3])
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])

# Stop when done
spark.stop()
```

### Application Lifecycle

A proper Spark application follows this lifecycle:

```python
# 1. INITIALIZATION
spark = SparkSession.builder \
    .appName("ProperLifecycle") \
    .getOrCreate()

try:
    # 2. PROCESSING
    # Load data
    data = spark.read.csv("input.csv", header=True)
    
    # Transform
    result = data.filter(data.age > 21)
    
    # Action
    result.write.parquet("output/")
    
except Exception as e:
    # 3. ERROR HANDLING
    print(f"Job failed: {e}")
    raise

finally:
    # 4. CLEANUP
    spark.stop()
```

### Driver Responsibilities

#### 1. Resource Management
The driver negotiates resources with the cluster manager.

```python
spark = SparkSession.builder \
    .appName("ResourceManagement") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()
```

#### 2. Task Distribution
The driver divides work into tasks and assigns them to executors.

```python
# Driver analyzes this and creates tasks
rdd = sc.parallelize(range(1000000), numSlices=100)
# Creates 100 tasks, one per partition
```

#### 3. Scheduling
The driver schedules jobs based on dependencies and available resources.

```python
# Job 1
result1 = rdd1.map(expensive_op).count()

# Job 2 (runs after Job 1 completes)
result2 = rdd2.filter(another_op).count()
```

#### 4. Fault Recovery
The driver detects failed tasks and reschedules them.

```python
# If an executor fails, driver:
# 1. Detects missing executor
# 2. Recomputes lost partitions using lineage
# 3. Reschedules tasks to other executors
```

### Common Driver Issues

#### 1. Driver Out of Memory
Collecting too much data to the driver.

```python
# WRONG - can crash driver
all_data = huge_rdd.collect()

# RIGHT - limit data sent to driver
sample = huge_rdd.take(1000)

# RIGHT - write results to storage
huge_rdd.saveAsTextFile("output/")
```

#### 2. Large Closures
Sending large objects to executors.

```python
# WRONG - sends entire large_dict to each task
large_dict = load_huge_dictionary()
result = rdd.map(lambda x: large_dict.get(x))

# RIGHT - use broadcast variable
large_dict = load_huge_dictionary()
bc_dict = spark.sparkContext.broadcast(large_dict)
result = rdd.map(lambda x: bc_dict.value.get(x))
```

#### 3. Single SparkContext Rule
Only one SparkContext can be active per JVM.

```python
# WRONG - creates multiple contexts
sc1 = SparkContext()
sc2 = SparkContext()  # ERROR!

# RIGHT - use getOrCreate
spark = SparkSession.builder.getOrCreate()
```

### Configuration Options

Important driver-related configurations:

| Configuration | Description | Default |
|--------------|-------------|---------|
| `spark.driver.memory` | Driver memory | 1g |
| `spark.driver.maxResultSize` | Max result size for actions | 1g |
| `spark.driver.cores` | Driver cores (cluster mode) | 1 |
| `spark.driver.extraJavaOptions` | Extra JVM options | - |

## Code Example

```python
from pyspark.sql import SparkSession
import time

def main():
    # Initialize with explicit configuration
    spark = SparkSession.builder \
        .appName("DriverClassDemo") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.maxResultSize", "1g") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
    # Display driver information
    print("=" * 50)
    print("DRIVER INFORMATION")
    print("=" * 50)
    print(f"Application ID: {sc.applicationId}")
    print(f"Application Name: {sc.appName}")
    print(f"Master: {sc.master}")
    print(f"Spark Version: {spark.version}")
    print(f"Default Parallelism: {sc.defaultParallelism}")
    print("=" * 50)
    
    # Demonstrate driver collecting results
    print("\n=== Collecting Data to Driver ===")
    data = sc.parallelize(range(100))
    
    # Safe: small collect
    small_result = data.take(10)
    print(f"take(10): {small_result}")
    
    # Demonstrate accumulator (driver reads final value)
    print("\n=== Accumulator (Driver Reads) ===")
    error_count = sc.accumulator(0)
    
    def process_with_error_tracking(x):
        if x % 10 == 0:
            error_count.add(1)
        return x * 2
    
    processed = data.map(process_with_error_tracking).collect()
    print(f"Total elements: {len(processed)}")
    print(f"Error count (read by driver): {error_count.value}")
    
    # Demonstrate broadcast (driver sends to executors)
    print("\n=== Broadcast (Driver Sends) ===")
    lookup_table = {"A": 1, "B": 2, "C": 3}
    bc_lookup = sc.broadcast(lookup_table)
    
    keys = sc.parallelize(["A", "B", "C", "A", "B"])
    values = keys.map(lambda k: bc_lookup.value.get(k, 0)).collect()
    print(f"Lookup results: {values}")
    
    # Demonstrate job scheduling
    print("\n=== Job Execution Timeline ===")
    
    start = time.time()
    
    # Job 1
    job1_start = time.time()
    count1 = sc.parallelize(range(10000)).map(lambda x: x * 2).count()
    print(f"Job 1 completed: {count1} elements in {time.time() - job1_start:.3f}s")
    
    # Job 2
    job2_start = time.time()
    count2 = sc.parallelize(range(10000)).filter(lambda x: x % 2 == 0).count()
    print(f"Job 2 completed: {count2} elements in {time.time() - job2_start:.3f}s")
    
    print(f"Total time: {time.time() - start:.3f}s")
    
    # Proper cleanup
    print("\n=== Shutting Down ===")
    spark.stop()
    print("SparkSession stopped successfully")

if __name__ == "__main__":
    main()
```

**Output:**
```
==================================================
DRIVER INFORMATION
==================================================
Application ID: local-1234567890
Application Name: DriverClassDemo
Master: local[*]
Spark Version: 3.5.0
Default Parallelism: 8
==================================================

=== Collecting Data to Driver ===
take(10): [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

=== Accumulator (Driver Reads) ===
Total elements: 100
Error count (read by driver): 10

=== Broadcast (Driver Sends) ===
Lookup results: [1, 2, 3, 1, 2]

=== Job Execution Timeline ===
Job 1 completed: 10000 elements in 0.245s
Job 2 completed: 5000 elements in 0.123s
Total time: 0.368s

=== Shutting Down ===
SparkSession stopped successfully
```

## Summary
- The driver runs your main program and coordinates Spark execution
- SparkContext is the core entry point for RDD operations
- SparkSession (Spark 2.0+) provides a unified entry point for all Spark APIs
- The driver creates the DAG, schedules tasks, and collects results
- Always call spark.stop() to properly release resources
- Avoid collecting large datasets to the driver to prevent memory issues
- Use broadcast variables to efficiently share large read-only data

## Additional Resources
- [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [SparkContext API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.html)
- [SparkSession API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html)
