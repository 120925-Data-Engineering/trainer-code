# Accumulators

## Learning Objectives
- Understand what accumulators are and their purpose
- Identify appropriate use cases for accumulators
- Implement accumulators for counting and debugging
- Recognize the limitations and best practices for accumulator usage

## Why This Matters
When processing data across a cluster, you often need to aggregate metrics like count of invalid records, execution statistics, or running totals. Accumulators provide a safe way to share write-only variables across distributed tasks. They are essential tools for debugging, monitoring, and collecting side metrics during Spark job execution.

## The Concept

### What are Accumulators?

Accumulators are **shared variables** that:
- Can be **added to** from worker tasks
- Are **read-only** on workers - only the driver can read the final value
- Support **associative** and **commutative** operations (like sum)
- Are used for implementing counters and sums in parallel computations

### How Accumulators Work

```
+------------------+
|     Driver       |
|  acc = Accum(0)  |  <-- Create and read accumulator
+------------------+
         |
         v
+--------+--------+--------+
|  Task 1 |  Task 2 |  Task 3 |
| acc += 5| acc += 3| acc += 7|
+---------+---------+---------+
         |
         v
+------------------+
|     Driver       |
|  acc.value = 15  |  <-- Final aggregated value
+------------------+
```

### Creating Accumulators

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "AccumulatorDemo")

# Numeric accumulators
counter = sc.accumulator(0)      # Initialize to 0
sum_acc = sc.accumulator(0.0)    # Float accumulator

# Increment in transformations
def process_record(record):
    counter.add(1)
    return record * 2

rdd = sc.parallelize([1, 2, 3, 4, 5])
result = rdd.map(process_record).collect()

# Read value on driver (only after action completes)
print(f"Records processed: {counter.value}")
```

### Common Use Cases

#### 1. Counting Records
```python
total_records = sc.accumulator(0)
error_records = sc.accumulator(0)

def process_line(line):
    total_records.add(1)
    try:
        return parse_line(line)
    except:
        error_records.add(1)
        return None

rdd.map(process_line).filter(lambda x: x is not None).collect()

print(f"Total: {total_records.value}, Errors: {error_records.value}")
```

#### 2. Sum Calculations
```python
total_bytes = sc.accumulator(0)

def process_file(file_info):
    total_bytes.add(file_info.size)
    return file_info

files_rdd.map(process_file).collect()
print(f"Total bytes processed: {total_bytes.value}")
```

#### 3. Debugging and Profiling
```python
null_values = sc.accumulator(0)
large_values = sc.accumulator(0)

def analyze_data(value):
    if value is None:
        null_values.add(1)
    elif value > 1000:
        large_values.add(1)
    return value

rdd.map(analyze_data).collect()
print(f"Null values: {null_values.value}")
print(f"Large values: {large_values.value}")
```

### Important Rules and Limitations

#### 1. Updates Only Guaranteed with Actions
Accumulator updates in transformations are only guaranteed to happen **once** when the transformation is part of an **action**.

```python
# SAFE - inside an action
rdd.foreach(lambda x: counter.add(1))
print(counter.value)  # Correct count

# POTENTIALLY UNSAFE - transformation without action
mapped = rdd.map(lambda x: (counter.add(1), x)[1])
# If you call mapped.count() twice, counter increments twice!
```

#### 2. Tasks May Be Re-executed
If a task fails and is retried, accumulator updates may be counted multiple times for that task.

```python
# For critical counts, consider using reduce() instead
exact_count = rdd.map(lambda x: 1).reduce(lambda a, b: a + b)
```

#### 3. Lazy Evaluation Impact
```python
counter = sc.accumulator(0)

# Transformation defined (lazy - not executed)
mapped = rdd.map(lambda x: (counter.add(1), x)[1])

# Counter is still 0 here!
print(counter.value)  # 0

# Now trigger execution
mapped.count()
print(counter.value)  # Now has the count
```

### Custom Accumulators

You can create custom accumulators for complex aggregations using AccumulatorParam:

```python
from pyspark.accumulators import AccumulatorParam

class SetAccumulatorParam(AccumulatorParam):
    def zero(self, initial_value):
        return set()
    
    def addInPlace(self, v1, v2):
        return v1.union(v2)

# Create custom accumulator
unique_words = sc.accumulator(set(), SetAccumulatorParam())

def collect_word(word):
    unique_words.add({word})
    return word

words_rdd.map(collect_word).collect()
print(f"Unique words: {unique_words.value}")
```

## Code Example

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "AccumulatorExample")

# Sample data: sales records with some invalid entries
sales_data = [
    "1001,ProductA,100.50",
    "1002,ProductB,invalid",  # Invalid price
    "1003,ProductC,75.25",
    "1004,ProductD,",         # Missing price
    "1005,ProductE,200.00",
    "1006,ProductF,50.00",
    "bad_record",             # Malformed record
    "1007,ProductG,125.75"
]

# Create accumulators for tracking
total_records = sc.accumulator(0)
valid_records = sc.accumulator(0)
invalid_records = sc.accumulator(0)
total_sales = sc.accumulator(0.0)

def parse_sale(line):
    total_records.add(1)
    
    try:
        parts = line.split(",")
        if len(parts) != 3:
            invalid_records.add(1)
            return None
        
        order_id = parts[0]
        product = parts[1]
        price_str = parts[2].strip()
        
        if not price_str:
            invalid_records.add(1)
            return None
        
        price = float(price_str)
        valid_records.add(1)
        total_sales.add(price)
        
        return (order_id, product, price)
    
    except (ValueError, IndexError):
        invalid_records.add(1)
        return None

# Process the data
rdd = sc.parallelize(sales_data)
parsed = rdd.map(parse_sale).filter(lambda x: x is not None)

# Trigger computation with an action
results = parsed.collect()

# Print accumulator values
print("=== Processing Summary ===")
print(f"Total records processed: {total_records.value}")
print(f"Valid records: {valid_records.value}")
print(f"Invalid records: {invalid_records.value}")
print(f"Total sales value: ${total_sales.value:.2f}")
print(f"\n=== Valid Sales Records ===")
for order_id, product, price in results:
    print(f"  {order_id}: {product} - ${price:.2f}")

sc.stop()
```

**Output:**
```
=== Processing Summary ===
Total records processed: 8
Valid records: 5
Invalid records: 3
Total sales value: $551.50

=== Valid Sales Records ===
  1001: ProductA - $100.50
  1003: ProductC - $75.25
  1005: ProductE - $200.00
  1006: ProductF - $50.00
  1007: ProductG - $125.75
```

## Summary
- Accumulators are shared write-only variables for aggregating values across tasks
- Common use cases include counting records, tracking errors, and debugging
- Only the driver can read accumulator values after an action completes
- Accumulator updates in transformations without actions may not execute
- Tasks can be re-executed, potentially causing duplicate updates
- Use accumulators for approximate metrics; use reduce() for exact counts

## Additional Resources
- [Shared Variables - Accumulators](https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators)
- [PySpark Accumulator API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.Accumulator.html)
- [Custom Accumulators](https://spark.apache.org/docs/latest/rdd-programming-guide.html#custom-accumulators)
