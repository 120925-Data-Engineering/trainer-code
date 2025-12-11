# Exercise: Accumulators

## Overview
Implement accumulators to track metrics during distributed processing, such as counting records and tracking errors.

**Duration:** 30-45 minutes  
**Mode:** Individual

---

## Core Tasks

### Task 1: Basic Counter Accumulator

Create `accumulators.py`:

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "AccumulatorExercise")

# Create accumulator
record_counter = sc.accumulator(0)

# Sample data
data = sc.parallelize(range(1, 101))

# Count records using accumulator
def count_record(x):
    record_counter.add(1)
    return x

data.map(count_record).collect()

print(f"Records processed: {record_counter.value}")
# Expected: 100
```

### Task 2: Error Counting Pattern

Implement a data quality checker:

```python
# Sample data with some invalid records
records = sc.parallelize([
    "100,Alice,Engineering",
    "200,Bob,Sales",
    "INVALID_RECORD",
    "300,Charlie,Marketing",
    "",  # Empty record
    "400,Diana,Engineering",
    "BAD_DATA_HERE",
    "500,Eve,Sales"
])

# Accumulators for tracking
total_records = sc.accumulator(0)
valid_records = sc.accumulator(0)
invalid_records = sc.accumulator(0)

def validate_record(record):
    total_records.add(1)
    
    # YOUR CODE: Check if record has 3 comma-separated fields
    # If valid: increment valid_records, return the record
    # If invalid: increment invalid_records, return None

valid_data = records.map(validate_record).filter(lambda x: x is not None)
valid_data.collect()

print(f"Total records: {total_records.value}")
print(f"Valid records: {valid_records.value}")
print(f"Invalid records: {invalid_records.value}")
print(f"Error rate: {invalid_records.value / total_records.value * 100:.1f}%")

# Expected:
# Total records: 8
# Valid records: 5
# Invalid records: 3
# Error rate: 37.5%
```

### Task 3: Category Counter

Count records by category:

```python
# Sample data
sales = sc.parallelize([
    ("Electronics", 999),
    ("Clothing", 50),
    ("Electronics", 299),
    ("Food", 25),
    ("Clothing", 75),
    ("Electronics", 149),
    ("Food", 30)
])

# Create accumulators for each category
electronics_count = sc.accumulator(0)
clothing_count = sc.accumulator(0)
food_count = sc.accumulator(0)

def count_by_category(record):
    category, _ = record
    # YOUR CODE: Increment the appropriate accumulator
    return record

sales.foreach(count_by_category)

print(f"Electronics: {electronics_count.value}")
print(f"Clothing: {clothing_count.value}")
print(f"Food: {food_count.value}")

# Expected:
# Electronics: 3
# Clothing: 2
# Food: 2
```

### Task 4: Sum Accumulator

Calculate totals:

```python
# Calculate total sales amount
total_sales = sc.accumulator(0)

def sum_sales(record):
    _, amount = record
    total_sales.add(amount)
    return record

sales.foreach(sum_sales)

print(f"Total sales: ${total_sales.value}")
# Expected: $1627
```

---

## Important Notes

1. **Accumulators are write-only on workers** - Only the driver can read the value
2. **Use foreach for guaranteed updates** - Transformations may be retried
3. **Values are only guaranteed after an action** - Due to lazy evaluation

---

## Deliverables

1. `accumulators.py` - Complete script with all tasks
2. Output showing correct accumulator values

---

## Definition of Done

- [ ] Basic counter accumulator works correctly
- [ ] Error counting pattern tracks valid/invalid records
- [ ] Category counters work with foreach
- [ ] Sum accumulator calculates total correctly
- [ ] Understand when accumulator values are available

---

## Additional Resources
- Written Content: `accumulators.md`
- Demo: `demo_accumulators.py`
