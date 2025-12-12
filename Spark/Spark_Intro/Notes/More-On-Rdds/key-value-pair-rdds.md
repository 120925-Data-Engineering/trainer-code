# Key-Value Pair RDDs

## Learning Objectives
- Understand PairRDD concepts and creation
- Apply key-value specific operations
- Use reduceByKey, groupByKey, sortByKey, and join operations
- Recognize when to use pair RDDs vs regular RDDs

## Why This Matters
Many data processing tasks involve grouping, aggregating, or joining data by a key. Pair RDDs provide specialized operations for these use cases that are optimized for distributed processing. From word counts to complex joins, understanding pair RDDs unlocks efficient data manipulation in Spark.

## The Concept

### What are Pair RDDs?

A Pair RDD is an RDD of key-value tuples. In Python, each element is a tuple with two elements: `(key, value)`.

```python
# Creating a Pair RDD
pairs = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])

# Converting to Pair RDD using map
lines = sc.parallelize(["hello world", "hello spark"])
word_pairs = lines.flatMap(lambda line: line.split()) \
                  .map(lambda word: (word, 1))
# [("hello", 1), ("world", 1), ("hello", 1), ("spark", 1)]
```

### Key Operations on Pair RDDs

#### reduceByKey(func)
Combines values for each key using an associative reduce function.

```python
pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4)])
sums = pairs.reduceByKey(lambda x, y: x + y)
# [("a", 4), ("b", 6)]
```

**How it works:**
1. Combines values within each partition first (local reduce)
2. Shuffles data by key
3. Combines results across partitions

This is more efficient than groupByKey followed by local aggregation.

#### groupByKey()
Groups all values for each key into an iterable.

```python
pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4)])
grouped = pairs.groupByKey()
# [("a", [1, 3]), ("b", [2, 4])]

# Access values
for key, values in grouped.collect():
    print(f"{key}: {list(values)}")
```

**Caution:** groupByKey shuffles ALL data without local aggregation. Use reduceByKey or aggregateByKey when possible.

#### aggregateByKey(zeroValue, seqFunc, combFunc)
Flexible aggregation with separate functions for within-partition and across-partition operations.

```python
# Calculate (sum, count) for average
pairs = sc.parallelize([("a", 10), ("b", 20), ("a", 30), ("b", 40)])

sum_count = pairs.aggregateByKey(
    (0, 0),  # zeroValue: (sum, count)
    lambda acc, v: (acc[0] + v, acc[1] + 1),  # seqFunc: within partition
    lambda a, b: (a[0] + b[0], a[1] + b[1])   # combFunc: across partitions
)
# [("a", (40, 2)), ("b", (60, 2))]

averages = sum_count.mapValues(lambda x: x[0] / x[1])
# [("a", 20.0), ("b", 30.0)]
```

#### sortByKey(ascending=True)
Sorts the RDD by key.

```python
pairs = sc.parallelize([("c", 3), ("a", 1), ("b", 2)])
sorted_pairs = pairs.sortByKey()
# [("a", 1), ("b", 2), ("c", 3)]

# Descending order
sorted_desc = pairs.sortByKey(ascending=False)
# [("c", 3), ("b", 2), ("a", 1)]
```

### Join Operations

#### join(other)
Inner join on key.

```python
rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
rdd2 = sc.parallelize([("a", "x"), ("b", "y"), ("d", "z")])

joined = rdd1.join(rdd2)
# [("a", (1, "x")), ("b", (2, "y"))]
```

#### leftOuterJoin(other)
Left outer join - includes all keys from left RDD.

```python
left_joined = rdd1.leftOuterJoin(rdd2)
# [("a", (1, "x")), ("b", (2, "y")), ("c", (3, None))]
```

#### rightOuterJoin(other)
Right outer join - includes all keys from right RDD.

```python
right_joined = rdd1.rightOuterJoin(rdd2)
# [("a", (1, "x")), ("b", (2, "y")), ("d", (None, "z"))]
```

#### fullOuterJoin(other)
Full outer join - includes all keys from both RDDs.

```python
full_joined = rdd1.fullOuterJoin(rdd2)
# [("a", (1, "x")), ("b", (2, "y")), ("c", (3, None)), ("d", (None, "z"))]
```

#### cogroup(other)
Groups data from multiple RDDs by key.

```python
cogrouped = rdd1.cogroup(rdd2)
for key, (vals1, vals2) in cogrouped.collect():
    print(f"{key}: left={list(vals1)}, right={list(vals2)}")
# a: left=[1], right=['x']
# b: left=[2], right=['y']
# c: left=[3], right=[]
# d: left=[], right=['z']
```

### Value Operations

#### mapValues(func)
Applies function to each value without changing keys.

```python
pairs = sc.parallelize([("a", 1), ("b", 2)])
doubled = pairs.mapValues(lambda v: v * 2)
# [("a", 2), ("b", 4)]
```

#### flatMapValues(func)
Maps values to iterables and flattens the result.

```python
pairs = sc.parallelize([("a", "1,2,3"), ("b", "4,5")])
split = pairs.flatMapValues(lambda v: v.split(","))
# [("a", "1"), ("a", "2"), ("a", "3"), ("b", "4"), ("b", "5")]
```

### Key Operations

#### keys()
Returns RDD of just the keys.

```python
pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 3)])
keys = pairs.keys()
# ["a", "b", "a"]
```

#### values()
Returns RDD of just the values.

```python
values = pairs.values()
# [1, 2, 3]
```

#### countByKey()
Returns dictionary of counts per key (action).

```python
counts = pairs.countByKey()
# {"a": 2, "b": 1}
```

### Performance Comparison

| Operation | Data Shuffled | Use When |
|-----------|--------------|----------|
| reduceByKey | Partial (combined first) | Aggregating values |
| groupByKey | All values | Need all values per key |
| aggregateByKey | Partial (combined first) | Complex aggregations |
| combineByKey | Partial (combined first) | Most flexible aggregation |

## Code Example

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "PairRDDDemo")

# Sample sales data: (product_id, (category, price, quantity))
sales = sc.parallelize([
    ("P001", ("Electronics", 999.99, 2)),
    ("P002", ("Clothing", 49.99, 5)),
    ("P001", ("Electronics", 999.99, 1)),
    ("P003", ("Food", 12.99, 10)),
    ("P002", ("Clothing", 49.99, 3)),
    ("P004", ("Electronics", 299.99, 4)),
    ("P003", ("Food", 12.99, 5))
])

# Product info: (product_id, product_name)
products = sc.parallelize([
    ("P001", "Laptop"),
    ("P002", "T-Shirt"),
    ("P003", "Snack Pack"),
    ("P004", "Headphones"),
    ("P005", "Book")  # No sales for this product
])

print("=== Basic Aggregation ===")
# Total quantity sold per product
product_quantities = sales.mapValues(lambda v: v[2]) \
                          .reduceByKey(lambda a, b: a + b)
print("Quantity per product:")
for prod, qty in product_quantities.collect():
    print(f"  {prod}: {qty} units")

print("\n=== Revenue Calculation ===")
# Calculate revenue per sale and aggregate
revenues = sales.mapValues(lambda v: v[1] * v[2])
total_revenue = revenues.reduceByKey(lambda a, b: a + b)
print("Revenue per product:")
for prod, rev in total_revenue.sortBy(lambda x: x[1], ascending=False).collect():
    print(f"  {prod}: ${rev:,.2f}")

print("\n=== Category Aggregation ===")
# Group by category and calculate total revenue
category_revenue = sales.map(lambda x: (x[1][0], x[1][1] * x[1][2])) \
                        .reduceByKey(lambda a, b: a + b)
print("Revenue by category:")
for cat, rev in category_revenue.collect():
    print(f"  {cat}: ${rev:,.2f}")

print("\n=== Joining Data ===")
# Join sales with product names
sales_with_names = sales.mapValues(lambda v: v[1] * v[2]) \
                        .reduceByKey(lambda a, b: a + b) \
                        .join(products)
print("Product sales with names:")
for prod_id, (revenue, name) in sales_with_names.collect():
    print(f"  {name} ({prod_id}): ${revenue:,.2f}")

# Left outer join to find products without sales
product_sales = products.leftOuterJoin(total_revenue)
print("\nAll products (including unsold):")
for prod_id, (name, revenue) in product_sales.collect():
    rev_str = f"${revenue:,.2f}" if revenue else "No sales"
    print(f"  {name}: {rev_str}")

print("\n=== Advanced Aggregation ===")
# Calculate average price and total quantity per product
stats = sales.mapValues(lambda v: (v[1], v[2], 1)) \
             .reduceByKey(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2]))
# (price, total_qty, num_transactions)

print("Product statistics:")
for prod, (price, total_qty, transactions) in stats.collect():
    print(f"  {prod}: price=${price}, qty={total_qty}, transactions={transactions}")

print("\n=== Cogroup Example ===")
# Cogroup sales totals with product names
cogrouped = total_revenue.cogroup(products)
print("Cogrouped data:")
for key, (revenues, names) in cogrouped.collect():
    rev_list = list(revenues)
    name_list = list(names)
    rev_str = f"${rev_list[0]:,.2f}" if rev_list else "N/A"
    name_str = name_list[0] if name_list else "Unknown"
    print(f"  {key}: {name_str} - {rev_str}")

sc.stop()
```

**Output:**
```
=== Basic Aggregation ===
Quantity per product:
  P001: 3 units
  P002: 8 units
  P003: 15 units
  P004: 4 units

=== Revenue Calculation ===
Revenue per product:
  P001: $2,999.97
  P004: $1,199.96
  P002: $399.92
  P003: $194.85

=== Category Aggregation ===
Revenue by category:
  Electronics: $4,199.93
  Clothing: $399.92
  Food: $194.85

=== Joining Data ===
Product sales with names:
  Laptop (P001): $2,999.97
  T-Shirt (P002): $399.92
  Snack Pack (P003): $194.85
  Headphones (P004): $1,199.96

All products (including unsold):
  Laptop: $2,999.97
  T-Shirt: $399.92
  Snack Pack: $194.85
  Headphones: $1,199.96
  Book: No sales

=== Advanced Aggregation ===
Product statistics:
  P001: price=999.99, qty=3, transactions=2
  P002: price=49.99, qty=8, transactions=2
  P003: price=12.99, qty=15, transactions=2
  P004: price=299.99, qty=4, transactions=1

=== Cogrouped data ===
  P001: Laptop - $2,999.97
  P002: T-Shirt - $399.92
  P003: Snack Pack - $194.85
  P004: Headphones - $1,199.96
  P005: Book - N/A
```

## Summary
- Pair RDDs contain (key, value) tuples enabling key-based operations
- reduceByKey aggregates values efficiently by combining within partitions first
- groupByKey collects all values per key but shuffles all data (use sparingly)
- aggregateByKey provides flexible aggregation with different functions
- Join operations (join, leftOuterJoin, rightOuterJoin, fullOuterJoin) combine RDDs by key
- cogroup groups data from multiple RDDs without combining values
- Use mapValues and flatMapValues to transform values while preserving keys

## Additional Resources
- [Pair RDD Operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#working-with-key-value-pairs)
- [Shuffle Operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations)
- [Performance Tuning for Joins](https://spark.apache.org/docs/latest/tuning.html)
