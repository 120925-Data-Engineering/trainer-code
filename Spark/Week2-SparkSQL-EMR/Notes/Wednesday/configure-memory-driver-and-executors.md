# Configure Memory Driver and Executors

## Learning Objectives
- Understand memory allocation for Spark driver and executors
- Configure memory settings appropriately
- Apply tuning guidelines for different workloads

## Why This Matters
Memory configuration is critical for Spark performance. Incorrect settings lead to OOM errors or wasted resources.

## The Concept

### Driver Memory

The driver needs memory for collecting results, broadcast variables, and job metadata.

```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()
```

### Executor Memory Breakdown

```
Total Executor Memory (8g)
+-------------------------------------------------+
| Reserved Memory: 300MB                          |
+-------------------------------------------------+
| Spark Memory: (8g - 300MB) * 0.6 = ~4.6g        |
|   - Execution: shuffles, joins, sorts           |
|   - Storage: cached data                        |
+-------------------------------------------------+
| User Memory: ~3g                                |
|   - UDFs, data structures                       |
+-------------------------------------------------+
```

### Key Configurations

| Property | Description | Default |
|----------|-------------|---------|
| `spark.driver.memory` | Driver heap | 1g |
| `spark.executor.memory` | Executor heap | 1g |
| `spark.memory.fraction` | Spark memory fraction | 0.6 |
| `spark.memory.storageFraction` | Storage fraction | 0.5 |

### Memory Patterns

**Cache-heavy workloads:**
```python
spark.conf.set("spark.memory.storageFraction", "0.6")
```

**Shuffle-heavy workloads:**
```python
spark.conf.set("spark.memory.storageFraction", "0.2")
```

### Troubleshooting OOM

```python
# Increase executor memory
spark.conf.set("spark.executor.memory", "12g")

# Increase partitions
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Avoid large collects
df.limit(1000).collect()  # Instead of df.collect()
```

## Summary
- Driver memory for results and broadcasts
- Executor memory divided into Spark and user pools
- Configure based on workload type
- Monitor via Spark UI and adjust as needed

## Additional Resources
- [Memory Management](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)
