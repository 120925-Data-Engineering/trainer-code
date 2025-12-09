# Actions

## Learning Objectives
- Understand what actions are in Spark RDDs
- Identify when computation is triggered
- Use common RDD actions effectively
- Recognize the difference between actions and transformations

## Why This Matters
Actions are the operations that trigger actual computation in Spark. While transformations define what you want to do with data, actions make it happen. Understanding actions is crucial because they determine when your Spark job executes and how results are returned to your driver program.

## The Concept

### What are Actions?

Actions are RDD operations that:
- **Trigger computation** of the RDD lineage
- **Return results** to the driver program or write to storage
- **Break the lazy evaluation** chain

When you call an action, Spark:
1. Builds an execution plan from the lineage
2. Optimizes the plan
3. Executes tasks across the cluster
4. Returns or persists results

### Common RDD Actions

#### collect()
Returns all elements to the driver as a list.

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.collect()
print(result)  # [1, 2, 3, 4, 5]
```

**Warning:** Only use on small datasets. Collecting large datasets can crash the driver.

#### count()
Returns the number of elements in the RDD.

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
print(rdd.count())  # 5
```

#### first()
Returns the first element of the RDD.

```python
rdd = sc.parallelize([10, 20, 30])
print(rdd.first())  # 10
```

#### take(n)
Returns the first n elements as a list.

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
print(rdd.take(3))  # [1, 2, 3]
```

#### takeSample(withReplacement, num, seed)
Returns a random sample of elements.

```python
rdd = sc.parallelize(range(100))
sample = rdd.takeSample(False, 5, seed=42)
print(sample)  # [73, 31, 17, 96, 8] (varies by seed)
```

#### reduce(func)
Aggregates elements using a function that takes two arguments and returns one.

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
total = rdd.reduce(lambda a, b: a + b)
print(total)  # 15
```

The function must be **commutative** and **associative** for correct parallel execution.

#### fold(zeroValue, func)
Similar to reduce, but with an initial "zero value" for each partition.

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
total = rdd.fold(0, lambda a, b: a + b)
print(total)  # 15
```

#### aggregate(zeroValue, seqOp, combOp)
More flexible aggregation with separate functions for within-partition and across-partition operations.

```python
# Calculate sum and count simultaneously
rdd = sc.parallelize([1, 2, 3, 4, 5])

# zeroValue: (sum, count)
# seqOp: accumulate within partition
# combOp: combine partitions
result = rdd.aggregate(
    (0, 0),
    lambda acc, val: (acc[0] + val, acc[1] + 1),  # seqOp
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # combOp
)
print(f"Sum: {result[0]}, Count: {result[1]}")  # Sum: 15, Count: 5
```

#### foreach(func)
Applies a function to each element without returning results. Useful for side effects.

```python
def log_element(x):
    print(f"Processing: {x}")

rdd = sc.parallelize([1, 2, 3])
rdd.foreach(log_element)  # Prints on executor nodes, not driver
```

#### saveAsTextFile(path)
Writes RDD to a text file (one file per partition).

```python
rdd = sc.parallelize(["line 1", "line 2", "line 3"])
rdd.saveAsTextFile("output/results")
```

#### countByValue()
Returns the count of each unique value as a dictionary.

```python
rdd = sc.parallelize(["a", "b", "a", "c", "a", "b"])
counts = rdd.countByValue()
print(dict(counts))  # {'a': 3, 'b': 2, 'c': 1}
```

### Actions vs Transformations

| Aspect | Transformations | Actions |
|--------|-----------------|---------|
| Return Type | New RDD | Value or storage |
| Execution | Lazy (deferred) | Eager (immediate) |
| Examples | map, filter, flatMap | collect, count, reduce |
| Purpose | Define computation | Trigger computation |

### Execution Flow

```
Transformation 1     Transformation 2     Transformation 3     Action
     map        ->      filter       ->      map         ->    collect
     
     [Lazy]              [Lazy]              [Lazy]           [Executes!]
                                                                  |
                                                                  v
                                                           Build DAG
                                                                  |
                                                                  v
                                                           Execute on Cluster
                                                                  |
                                                                  v
                                                           Return Results
```

## Code Example

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "ActionsDemo")

# Create sample data
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
words = sc.parallelize(["apple", "banana", "apple", "cherry", "banana", "apple"])

# Demonstrate various actions
print("=== Counting Operations ===")
print(f"count(): {numbers.count()}")
print(f"countByValue(): {dict(words.countByValue())}")

print("\n=== Retrieval Operations ===")
print(f"collect(): {numbers.collect()}")
print(f"first(): {numbers.first()}")
print(f"take(3): {numbers.take(3)}")
print(f"takeSample(False, 3): {numbers.takeSample(False, 3, seed=42)}")

print("\n=== Aggregation Operations ===")
print(f"reduce (sum): {numbers.reduce(lambda a, b: a + b)}")
print(f"reduce (max): {numbers.reduce(lambda a, b: a if a > b else b)}")
print(f"fold (sum with zero): {numbers.fold(0, lambda a, b: a + b)}")

# Aggregate: calculate sum and count for average
sum_count = numbers.aggregate(
    (0, 0),
    lambda acc, val: (acc[0] + val, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
)
print(f"aggregate (sum, count): {sum_count}")
print(f"Average: {sum_count[0] / sum_count[1]}")

print("\n=== Other Useful Actions ===")
print(f"top(3) - largest values: {numbers.top(3)}")
print(f"takeOrdered(3) - smallest values: {numbers.takeOrdered(3)}")
print(f"isEmpty(): {numbers.isEmpty()}")

sc.stop()
```

**Output:**
```
=== Counting Operations ===
count(): 10
countByValue(): {'apple': 3, 'banana': 2, 'cherry': 1}

=== Retrieval Operations ===
collect(): [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
first(): 1
take(3): [1, 2, 3]
takeSample(False, 3): [8, 2, 6]

=== Aggregation Operations ===
reduce (sum): 55
reduce (max): 10
fold (sum with zero): 55
aggregate (sum, count): (55, 10)
Average: 5.5

=== Other Useful Actions ===
top(3) - largest values: [10, 9, 8]
takeOrdered(3) - smallest values: [1, 2, 3]
isEmpty(): False
```

## Summary
- Actions trigger computation and break the lazy evaluation chain
- collect(), count(), first(), and take() retrieve data to the driver
- reduce(), fold(), and aggregate() perform aggregations across partitions
- foreach() applies side effects without returning data
- saveAsTextFile() persists results to storage
- Always be cautious with collect() on large datasets

## Additional Resources
- [RDD Actions Reference](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions)
- [PySpark RDD API Reference](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html)
- [Understanding Lazy Evaluation](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations)
