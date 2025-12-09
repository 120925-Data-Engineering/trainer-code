# Basic RDD Operations

## Learning Objectives
- Create RDDs from collections and external files
- Apply fundamental transformations: map, filter, flatMap
- Understand the difference between narrow and wide transformations
- Chain multiple operations together effectively

## Why This Matters
Mastering basic RDD operations is essential for data processing in Spark. These fundamental transformations are the building blocks for all data manipulation. Understanding how to use map, filter, and flatMap enables you to clean, transform, and reshape data at scale.

## The Concept

### Creating RDDs

Before applying operations, you need data in an RDD. There are two primary methods:

#### From Python Collections
```python
# From a list
numbers = sc.parallelize([1, 2, 3, 4, 5])

# From a range
large_range = sc.parallelize(range(1, 1000001))

# Specify partitions
partitioned = sc.parallelize([1, 2, 3, 4, 5], numSlices=4)
```

#### From External Files
```python
# Single text file
lines = sc.textFile("data/input.txt")

# Multiple files with wildcard
logs = sc.textFile("logs/*.log")

# Whole text files (filename, content) pairs
files = sc.wholeTextFiles("documents/")
```

### Fundamental Transformations

#### map(func)
Applies a function to each element, returning a new RDD with the same number of elements.

```python
numbers = sc.parallelize([1, 2, 3, 4, 5])

# Square each number
squared = numbers.map(lambda x: x ** 2)
print(squared.collect())  # [1, 4, 9, 16, 25]

# Convert to string
strings = numbers.map(lambda x: f"Number: {x}")
print(strings.collect())  
# ['Number: 1', 'Number: 2', 'Number: 3', 'Number: 4', 'Number: 5']
```

**Use map when:** You want to transform each element into exactly one output element.

#### filter(func)
Returns elements where the function returns True.

```python
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# Keep only even numbers
evens = numbers.filter(lambda x: x % 2 == 0)
print(evens.collect())  # [2, 4, 6, 8, 10]

# Keep numbers greater than 5
large = numbers.filter(lambda x: x > 5)
print(large.collect())  # [6, 7, 8, 9, 10]
```

**Use filter when:** You want to select a subset of elements based on a condition.

#### flatMap(func)
Applies a function that returns an iterable, then flattens the results.

```python
sentences = sc.parallelize([
    "Hello World",
    "Spark is powerful",
    "Data engineering rocks"
])

# Split sentences into words
words = sentences.flatMap(lambda s: s.split())
print(words.collect())
# ['Hello', 'World', 'Spark', 'is', 'powerful', 'Data', 'engineering', 'rocks']
```

**Use flatMap when:** Each input element produces zero or more output elements.

#### Comparison: map vs flatMap

```python
data = sc.parallelize(["a b c", "d e"])

# map keeps structure
mapped = data.map(lambda x: x.split())
print(mapped.collect())  # [['a', 'b', 'c'], ['d', 'e']]

# flatMap flattens result
flat = data.flatMap(lambda x: x.split())
print(flat.collect())  # ['a', 'b', 'c', 'd', 'e']
```

### Other Useful Operations

#### distinct()
Removes duplicate elements.

```python
data = sc.parallelize([1, 2, 2, 3, 3, 3, 4])
unique = data.distinct()
print(unique.collect())  # [1, 2, 3, 4]
```

#### union(other)
Combines two RDDs.

```python
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([3, 4, 5])
combined = rdd1.union(rdd2)
print(combined.collect())  # [1, 2, 3, 3, 4, 5]
```

#### intersection(other)
Returns elements present in both RDDs.

```python
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([3, 4, 5, 6])
common = rdd1.intersection(rdd2)
print(common.collect())  # [3, 4]
```

#### subtract(other)
Returns elements in the first RDD not in the second.

```python
rdd1 = sc.parallelize([1, 2, 3, 4])
rdd2 = sc.parallelize([3, 4, 5, 6])
diff = rdd1.subtract(rdd2)
print(diff.collect())  # [1, 2]
```

#### sample(withReplacement, fraction, seed)
Returns a random sample of the RDD.

```python
data = sc.parallelize(range(100))
sampled = data.sample(False, 0.1, seed=42)
print(sampled.collect())  # ~10 random elements
```

### Chaining Operations

Operations can be chained together for complex transformations:

```python
# Read log file, filter errors, extract timestamps
log_rdd = sc.textFile("application.log")

error_times = log_rdd \
    .filter(lambda line: "ERROR" in line) \
    .map(lambda line: line.split()[0]) \
    .distinct() \
    .collect()
```

### Narrow vs Wide Transformations

Understanding this distinction helps optimize performance:

| Type | Shuffle? | Examples |
|------|----------|----------|
| **Narrow** | No | map, filter, flatMap, union |
| **Wide** | Yes | distinct, intersection, subtract, groupByKey |

**Narrow transformations:** Each partition of the parent RDD contributes to at most one partition of the result.

**Wide transformations:** Data from multiple partitions must be shuffled across the network.

```
Narrow Transformation (map):          Wide Transformation (distinct):
                                      
Partition 0 -> Partition 0            Partition 0 --+
                                                     \
Partition 1 -> Partition 1            Partition 1 ---+-> Shuffle -> New Partitions
                                                     /
Partition 2 -> Partition 2            Partition 2 --+
```

## Code Example

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "BasicRDDOperations")

# Sample log data
log_data = [
    "2024-01-15 10:30:22 INFO User login successful",
    "2024-01-15 10:31:05 ERROR Database connection failed",
    "2024-01-15 10:31:10 INFO Retry connection",
    "2024-01-15 10:31:15 ERROR Database connection failed",
    "2024-01-15 10:32:00 INFO Database connected",
    "2024-01-15 10:33:45 WARN High memory usage detected",
    "2024-01-15 10:35:00 ERROR Out of memory exception",
    "2024-01-15 10:35:05 INFO System restart initiated"
]

logs = sc.parallelize(log_data)

# Example 1: Filter and map
print("=== Error Logs (timestamp only) ===")
error_times = logs \
    .filter(lambda line: "ERROR" in line) \
    .map(lambda line: line.split()[0] + " " + line.split()[1])

for time in error_times.collect():
    print(f"  {time}")

# Example 2: FlatMap to extract words
print("\n=== Unique Log Levels ===")
log_levels = logs \
    .map(lambda line: line.split()[2]) \
    .distinct()

print(f"  {log_levels.collect()}")

# Example 3: Complex chain
print("\n=== Word Frequency in Error Messages ===")
error_words = logs \
    .filter(lambda line: "ERROR" in line) \
    .flatMap(lambda line: line.split()[3:]) \
    .map(lambda word: (word.lower(), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

for word, count in error_words.collect():
    print(f"  {word}: {count}")

# Example 4: Set operations
info_lines = logs.filter(lambda l: "INFO" in l)
warn_error_lines = logs.filter(lambda l: "WARN" in l or "ERROR" in l)

print(f"\n=== Line Counts ===")
print(f"  INFO lines: {info_lines.count()}")
print(f"  WARN/ERROR lines: {warn_error_lines.count()}")
print(f"  All lines: {logs.count()}")
print(f"  Combined (union): {info_lines.union(warn_error_lines).count()}")

sc.stop()
```

**Output:**
```
=== Error Logs (timestamp only) ===
  2024-01-15 10:31:05
  2024-01-15 10:31:15
  2024-01-15 10:35:00

=== Unique Log Levels ===
  ['INFO', 'ERROR', 'WARN']

=== Word Frequency in Error Messages ===
  connection: 2
  database: 2
  failed: 2
  of: 1
  out: 1
  memory: 1
  exception: 1

=== Line Counts ===
  INFO lines: 4
  WARN/ERROR lines: 4
  All lines: 8
  Combined (union): 8
```

## Summary
- Create RDDs from collections with `parallelize()` or files with `textFile()`
- `map()` transforms each element to exactly one output element
- `filter()` selects elements matching a condition
- `flatMap()` transforms each element to zero or more output elements
- Operations can be chained for complex data pipelines
- Narrow transformations (map, filter) are more efficient than wide transformations (distinct, intersection)

## Additional Resources
- [RDD Transformations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations)
- [PySpark RDD API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
