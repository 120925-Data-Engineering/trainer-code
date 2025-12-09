# Launching a Spark Job

## Learning Objectives
- Understand the job submission process in Spark
- Explain the driver and executor lifecycle
- Monitor job execution through Spark UI
- Identify common job submission patterns

## Why This Matters
Knowing how to submit and monitor Spark jobs is essential for moving from development to production. Understanding the driver-executor relationship helps you debug issues, optimize performance, and ensure your jobs run reliably at scale.

## The Concept

### What is a Spark Job?

When you call an action on an RDD or DataFrame, Spark creates a **job**. A job is a sequence of computations triggered by an action. Jobs are divided into:

- **Stages:** Groups of tasks that can be executed together
- **Tasks:** Individual units of work executed on partitions

```
Action (e.g., count())
    |
    v
+-------+
|  Job  |
+-------+
    |
    +---> Stage 1 (narrow transformations)
    |         |
    |         +---> Task 1.1, Task 1.2, Task 1.3...
    |
    +---> Stage 2 (after shuffle)
              |
              +---> Task 2.1, Task 2.2, Task 2.3...
```

### Driver and Executor Model

#### The Driver
- Runs the main() function of your application
- Creates the SparkContext/SparkSession
- Builds the execution plan (DAG)
- Schedules tasks on executors
- Collects results from executors

#### The Executors
- Run on worker nodes
- Execute tasks assigned by the driver
- Store data for caching
- Report results back to the driver

```
+------------------+         +------------------+
|     Driver       |         |  Cluster Manager |
|  (Your Program)  |<------->|  (YARN, K8s, etc)|
+------------------+         +------------------+
         |                            |
         v                            v
+--------+--------+          +--------+--------+
|   Worker Node   |          |   Worker Node   |
| +-------------+ |          | +-------------+ |
| |  Executor 1 | |          | |  Executor 2 | |
| +-------------+ |          | +-------------+ |
+-----------------+          +-----------------+
```

### Job Lifecycle

1. **Application Submission:** User submits application to cluster
2. **Resource Allocation:** Cluster manager assigns resources
3. **Executor Launch:** Driver launches executors on workers
4. **Task Execution:** Driver sends tasks to executors
5. **Result Collection:** Executors return results to driver
6. **Cleanup:** Resources are released after completion

### Running a Spark Job

#### Interactive Mode (Development)
```bash
# PySpark shell
pyspark --master local[*]

# Or with cluster
pyspark --master yarn
```

#### Script Submission
```bash
# Local mode
spark-submit --master local[*] my_script.py

# Cluster mode
spark-submit --master yarn my_script.py
```

### Basic Job Example

```python
# my_job.py
from pyspark.sql import SparkSession

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("MyFirstJob") \
        .getOrCreate()
    
    # Get SparkContext
    sc = spark.sparkContext
    
    # Create RDD and perform operations
    data = sc.parallelize(range(1, 1000001))
    
    # Transformations (lazy)
    squared = data.map(lambda x: x ** 2)
    filtered = squared.filter(lambda x: x % 2 == 0)
    
    # Action triggers job
    result = filtered.reduce(lambda a, b: a + b)
    
    print(f"Result: {result}")
    
    # Clean up
    spark.stop()

if __name__ == "__main__":
    main()
```

### Monitoring with Spark UI

When a Spark application runs, it provides a web UI at port 4040 (by default):

```
http://localhost:4040
```

The UI shows:
- **Jobs:** List of all jobs with status
- **Stages:** Breakdown of stages within jobs
- **Storage:** Cached RDDs and DataFrames
- **Environment:** Configuration settings
- **Executors:** Resource usage per executor

Key metrics to watch:
- Duration per job/stage
- Shuffle read/write amounts
- Task distribution across executors
- Failed tasks and error messages

### Job Submission Patterns

#### Pattern 1: Simple Script
```python
# run_analysis.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analysis").getOrCreate()
df = spark.read.csv("data.csv", header=True)
result = df.groupBy("category").count()
result.write.parquet("output/")
spark.stop()
```

Submit:
```bash
spark-submit run_analysis.py
```

#### Pattern 2: Configurable Script
```python
# configurable_job.py
import argparse
from pyspark.sql import SparkSession

def main(input_path, output_path):
    spark = SparkSession.builder.appName("ConfigurableJob").getOrCreate()
    df = spark.read.parquet(input_path)
    # ... processing ...
    df.write.parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    main(args.input, args.output)
```

Submit:
```bash
spark-submit configurable_job.py --input data/input --output data/output
```

#### Pattern 3: With Dependencies
```bash
# Package dependencies in a zip file
spark-submit --py-files deps.zip my_job.py
```

### Common Issues and Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| Job hangs | Action on large collect() | Use take() or write to file |
| Out of memory | Driver memory too small | Increase spark.driver.memory |
| Slow execution | Insufficient parallelism | Increase partitions |
| Task failures | Serialization errors | Check closure variables |

## Code Example

```python
# complete_job.py
from pyspark.sql import SparkSession
import time

def main():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ComprehensiveJobExample") \
        .config("spark.ui.showConsoleProgress", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
    print("=" * 50)
    print(f"Application ID: {sc.applicationId}")
    print(f"Spark Version: {spark.version}")
    print(f"Master: {sc.master}")
    print(f"Default Parallelism: {sc.defaultParallelism}")
    print("=" * 50)
    
    # Job 1: Basic computation
    print("\n[Job 1] Starting basic computation...")
    start = time.time()
    
    numbers = sc.parallelize(range(1, 100001), numSlices=8)
    count = numbers.count()
    
    print(f"[Job 1] Counted {count} elements in {time.time() - start:.2f}s")
    
    # Job 2: Multi-stage computation
    print("\n[Job 2] Starting multi-stage computation...")
    start = time.time()
    
    words = sc.parallelize([
        "Spark is fast",
        "Spark is distributed",
        "Spark processes big data",
        "Big data is everywhere"
    ])
    
    word_counts = words \
        .flatMap(lambda line: line.lower().split()) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False)
    
    top_words = word_counts.take(5)
    print(f"[Job 2] Top 5 words: {top_words}")
    print(f"[Job 2] Completed in {time.time() - start:.2f}s")
    
    # Job 3: Aggregation
    print("\n[Job 3] Starting aggregation...")
    start = time.time()
    
    sales = sc.parallelize([
        ("Electronics", 1200),
        ("Clothing", 450),
        ("Electronics", 800),
        ("Food", 200),
        ("Clothing", 350),
        ("Electronics", 950),
        ("Food", 180)
    ])
    
    category_totals = sales \
        .reduceByKey(lambda a, b: a + b) \
        .collect()
    
    print(f"[Job 3] Category totals: {dict(category_totals)}")
    print(f"[Job 3] Completed in {time.time() - start:.2f}s")
    
    print("\n" + "=" * 50)
    print("All jobs completed successfully!")
    print(f"View Spark UI at: http://localhost:4040")
    print("=" * 50)
    
    # Allow time to view Spark UI
    input("Press Enter to exit and stop Spark...")
    
    spark.stop()

if __name__ == "__main__":
    main()
```

Run:
```bash
spark-submit complete_job.py
```

## Summary
- A Spark job is triggered when an action is called on an RDD or DataFrame
- Jobs are divided into stages and tasks for parallel execution
- The driver coordinates execution while executors perform the work
- Use spark-submit to run applications on local or cluster mode
- Spark UI at port 4040 provides monitoring and debugging information
- Proper job structure includes initialization, processing, and cleanup

## Additional Resources
- [Spark Job Scheduling](https://spark.apache.org/docs/latest/job-scheduling.html)
- [Monitoring and Instrumentation](https://spark.apache.org/docs/latest/monitoring.html)
- [Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)
