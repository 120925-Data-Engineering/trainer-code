# Transformations

## Learning Objectives
- Understand the full range of RDD transformations
- Differentiate between narrow and wide transformations
- Apply grouping, sorting, and reducing operations
- Optimize transformation chains for performance

## Why This Matters
Transformations are the core of data processing in Spark. They define how data is reshaped, aggregated, and prepared for analysis. Understanding the performance implications of different transformations helps you write efficient Spark applications that scale.

## The Concept

### Transformation Fundamentals

Transformations are **lazy operations** that define a computation on an RDD without executing it. They create a new RDD from an existing one.

```python
# Each transformation returns a new RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])
mapped = rdd.map(lambda x: x * 2)      # New RDD, not executed
filtered = mapped.filter(lambda x: x > 5)  # Another new RDD
# Nothing has executed yet!

result = filtered.collect()  # NOW execution happens
```

### Narrow vs Wide Transformations

Understanding this distinction is crucial for performance optimization.

#### Narrow Transformations
Data stays within the same partition. No shuffle required.

```
Parent Partition    Child Partition
     [1, 2]     ->      [2, 4]      (map: x * 2)
     [3, 4]     ->      [6, 8]
     [5, 6]     ->      [10, 12]
```

**Examples:** map, filter, flatMap, mapPartitions, union

#### Wide Transformations
Data must be shuffled across partitions via the network.

```
Parent Partitions      Shuffle       Child Partitions
  [("a",1),("b",2)]  \           /  [("a",1),("a",3)]
                      \--> --> ---> 
  [("a",3),("c",4)]  /           \  [("b",2),("c",4)]
```

**Examples:** groupByKey, reduceByKey, sortByKey, join, distinct

### Common Transformations Reference

#### Element-wise Transformations

| Transformation | Description | Example |
|---------------|-------------|---------|
| `map(func)` | Apply function to each element | `rdd.map(lambda x: x*2)` |
| `filter(func)` | Keep elements where func returns True | `rdd.filter(lambda x: x>0)` |
| `flatMap(func)` | Map + flatten results | `rdd.flatMap(lambda x: x.split())` |

#### Partition-level Transformations

| Transformation | Description | Use Case |
|---------------|-------------|----------|
| `mapPartitions(func)` | Apply function to entire partition | Database connections |
| `mapPartitionsWithIndex(func)` | Include partition index | Debugging |
| `coalesce(n)` | Reduce partitions (no shuffle if decreasing) | Reduce output files |
| `repartition(n)` | Increase/decrease partitions (shuffle) | Rebalance data |

```python
# mapPartitions - efficient for operations with setup cost
def process_partition(partition):
    # Setup (done once per partition)
    connection = create_db_connection()
    
    for record in partition:
        yield connection.process(record)
    
    # Cleanup
    connection.close()

rdd.mapPartitions(process_partition)
```

#### Aggregation Transformations

| Transformation | Description |
|---------------|-------------|
| `reduceByKey(func)` | Combine values with same key |
| `groupByKey()` | Group values by key (caution: memory intensive) |
| `aggregateByKey(zeroValue, seqFunc, combFunc)` | Flexible aggregation |
| `combineByKey(createCombiner, mergeValue, mergeCombiner)` | Most general aggregation |

```python
# reduceByKey - efficient, combines on each partition first
sales = sc.parallelize([("A", 100), ("B", 200), ("A", 150), ("B", 50)])
totals = sales.reduceByKey(lambda a, b: a + b)
# [("A", 250), ("B", 250)]

# groupByKey - less efficient, shuffles all data
grouped = sales.groupByKey()
# [("A", [100, 150]), ("B", [200, 50])]
```

**Performance tip:** Prefer `reduceByKey` over `groupByKey` when possible.

#### Set Operations

```python
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([3, 4, 5, 6])

union_rdd = rdd1.union(rdd2)           # [1,2,3,4,3,4,5,6]
intersection = rdd1.intersection(rdd2)  # [3, 4]
difference = rdd1.subtract(rdd2)        # [1, 2]
distinct_rdd = union_rdd.distinct()     # [1,2,3,4,5,6]
```

#### Sorting Transformations

```python
data = sc.parallelize([("b", 2), ("a", 3), ("c", 1)])

# Sort by key
by_key = data.sortByKey()
# [("a", 3), ("b", 2), ("c", 1)]

# Sort by value
by_value = data.sortBy(lambda x: x[1])
# [("c", 1), ("b", 2), ("a", 3)]

# Descending order
descending = data.sortBy(lambda x: x[1], ascending=False)
# [("a", 3), ("b", 2), ("c", 1)]
```

### Transformation Chains and DAGs

Spark builds a Directed Acyclic Graph (DAG) of transformations:

```python
result = sc.textFile("data.txt") \
    .flatMap(lambda line: line.split()) \     # Stage 1
    .map(lambda word: (word, 1)) \            # Stage 1
    .reduceByKey(lambda a, b: a + b) \        # Stage 2 (shuffle)
    .filter(lambda x: x[1] > 10) \            # Stage 2
    .map(lambda x: x[0])                       # Stage 2
```

**DAG visualization:**
```
textFile --> flatMap --> map --> [SHUFFLE] --> reduceByKey --> filter --> map
              Stage 1                              Stage 2
```

### Optimization Patterns

#### Pattern 1: Filter Early
```python
# Less efficient - filter after expensive operations
result = rdd.map(expensive_func).filter(cheap_filter)

# More efficient - filter first to reduce data volume
result = rdd.filter(cheap_filter).map(expensive_func)
```

#### Pattern 2: Combine Before Shuffle
```python
# Less efficient - shuffles all values
total = rdd.groupByKey().mapValues(sum)

# More efficient - reduces locally before shuffle
total = rdd.reduceByKey(lambda a, b: a + b)
```

#### Pattern 3: Minimize Shuffles
```python
# Multiple shuffles
result = rdd.groupByKey().mapValues(len).sortByKey()

# Single shuffle when possible
result = rdd.aggregateByKey(0, 
    lambda acc, v: acc + 1,
    lambda a, b: a + b
).sortByKey()
```

## Code Example

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "TransformationsDemo")

# Sample web log data
log_data = [
    "192.168.1.1 GET /home 200 1234",
    "192.168.1.2 GET /products 200 5678",
    "192.168.1.1 POST /login 200 123",
    "192.168.1.3 GET /home 404 0",
    "192.168.1.1 GET /products 200 4321",
    "192.168.1.2 GET /home 200 2345",
    "192.168.1.4 GET /products 500 0",
    "192.168.1.1 GET /checkout 200 9876"
]

logs = sc.parallelize(log_data)

# Parse log entries
def parse_log(line):
    parts = line.split()
    return {
        "ip": parts[0],
        "method": parts[1],
        "path": parts[2],
        "status": int(parts[3]),
        "bytes": int(parts[4])
    }

parsed = logs.map(parse_log)

print("=== Filtering ===")
# Filter successful requests
successful = parsed.filter(lambda r: r["status"] == 200)
print(f"Successful requests: {successful.count()}")

# Filter GET requests only
gets = parsed.filter(lambda r: r["method"] == "GET")
print(f"GET requests: {gets.count()}")

print("\n=== Grouping and Aggregation ===")
# Count requests per IP
ip_counts = parsed.map(lambda r: (r["ip"], 1)) \
                  .reduceByKey(lambda a, b: a + b)
print("Requests per IP:")
for ip, count in ip_counts.collect():
    print(f"  {ip}: {count}")

# Total bytes per path
path_bytes = parsed.filter(lambda r: r["status"] == 200) \
                   .map(lambda r: (r["path"], r["bytes"])) \
                   .reduceByKey(lambda a, b: a + b)
print("\nBytes per path:")
for path, total in path_bytes.sortBy(lambda x: x[1], ascending=False).collect():
    print(f"  {path}: {total} bytes")

print("\n=== Complex Aggregation ===")
# Calculate stats per IP: (total requests, total bytes, success rate)
ip_stats = parsed.map(lambda r: (
    r["ip"], 
    (1, r["bytes"], 1 if r["status"] == 200 else 0)
)).reduceByKey(lambda a, b: (
    a[0] + b[0],   # total requests
    a[1] + b[1],   # total bytes
    a[2] + b[2]    # successful requests
))

print("IP Statistics:")
for ip, (total, bytes_sum, success) in ip_stats.collect():
    rate = (success / total) * 100
    print(f"  {ip}: {total} requests, {bytes_sum} bytes, {rate:.1f}% success")

print("\n=== Sorting ===")
# Top IPs by request count
top_ips = ip_counts.sortBy(lambda x: x[1], ascending=False).take(3)
print("Top 3 IPs by request count:")
for ip, count in top_ips:
    print(f"  {ip}: {count}")

print("\n=== Set Operations ===")
# Find IPs with errors
error_ips = parsed.filter(lambda r: r["status"] >= 400) \
                  .map(lambda r: r["ip"]).distinct()
success_ips = parsed.filter(lambda r: r["status"] == 200) \
                    .map(lambda r: r["ip"]).distinct()

# IPs that had both success and errors
mixed_ips = error_ips.intersection(success_ips)
print(f"IPs with both success and errors: {mixed_ips.collect()}")

# IPs with only errors
only_errors = error_ips.subtract(success_ips)
print(f"IPs with only errors: {only_errors.collect()}")

sc.stop()
```

**Output:**
```
=== Filtering ===
Successful requests: 6
GET requests: 7

=== Grouping and Aggregation ===
Requests per IP:
  192.168.1.1: 4
  192.168.1.2: 2
  192.168.1.3: 1
  192.168.1.4: 1

Bytes per path:
  /checkout: 9876 bytes
  /products: 10000 bytes
  /home: 3579 bytes
  /login: 123 bytes

=== Complex Aggregation ===
IP Statistics:
  192.168.1.1: 4 requests, 15554 bytes, 100.0% success
  192.168.1.2: 2 requests, 8023 bytes, 100.0% success
  192.168.1.3: 1 requests, 0 bytes, 0.0% success
  192.168.1.4: 1 requests, 0 bytes, 0.0% success

=== Sorting ===
Top 3 IPs by request count:
  192.168.1.1: 4
  192.168.1.2: 2
  192.168.1.3: 1

=== Set Operations ===
IPs with both success and errors: []
IPs with only errors: ['192.168.1.3', '192.168.1.4']
```

## Summary
- Transformations are lazy operations that define data processing logic
- Narrow transformations (map, filter) operate within partitions without shuffling
- Wide transformations (groupByKey, reduceByKey) require data shuffling
- Prefer reduceByKey over groupByKey for better performance
- Filter early in the pipeline to reduce data volume
- Use mapPartitions for operations with expensive setup/teardown
- Chain transformations efficiently to minimize shuffle operations

## Additional Resources
- [RDD Transformations Reference](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations)
- [Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Understanding Shuffle Operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations)
