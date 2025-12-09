# Shared Variables

## Learning Objectives
- Understand the purpose of shared variables in Spark
- Use broadcast variables for efficient data distribution
- Implement accumulators for aggregating values
- Recognize when to use each type of shared variable

## Why This Matters
In distributed computing, sharing data between the driver and executors efficiently is challenging. Shared variables solve specific problems: broadcast variables distribute read-only data efficiently, while accumulators provide a mechanism to aggregate values across tasks. Mastering these tools helps you write performant Spark applications.

## The Concept

### Why Shared Variables?

In normal Spark operations, data is shipped with each task:

```python
# Without shared variables
large_lookup = {"key1": "value1", ...}  # 100MB dictionary

# This sends 100MB to EVERY task!
rdd.map(lambda x: large_lookup.get(x))
```

This becomes problematic when:
- The same data is used across many tasks
- The data is large
- You need to aggregate values back to the driver

### Two Types of Shared Variables

| Type | Direction | Purpose |
|------|-----------|---------|
| **Broadcast** | Driver -> Executors | Share large read-only data |
| **Accumulator** | Executors -> Driver | Aggregate values from tasks |

### Broadcast Variables

Broadcast variables efficiently distribute large read-only data to all executors once, rather than with each task.

#### How Broadcast Works

```
Without Broadcast:           With Broadcast:
                             
Driver                       Driver
   |                            |
   +---> Task 1 (100MB)         +---> Broadcast (100MB once)
   +---> Task 2 (100MB)              |
   +---> Task 3 (100MB)              v
   +---> Task 4 (100MB)         +--------+--------+--------+
                                |Executor|Executor|Executor|
Total: 400MB sent               | (cache)| (cache)| (cache)|
                                +--------+--------+--------+
                                     |        |        |
                                  Task 1   Task 2   Task 3...
                                
                                Total: 100MB sent (once per executor)
```

#### Creating and Using Broadcast Variables

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "BroadcastDemo")

# Large lookup data
country_codes = {
    "US": "United States",
    "UK": "United Kingdom",
    "DE": "Germany",
    "FR": "France",
    # ... thousands more
}

# Create broadcast variable
bc_countries = sc.broadcast(country_codes)

# Use in transformations
data = sc.parallelize(["US", "UK", "DE", "US", "FR"])
names = data.map(lambda code: bc_countries.value.get(code, "Unknown"))

print(names.collect())
# ['United States', 'United Kingdom', 'Germany', 'United States', 'France']

# Clean up when done
bc_countries.unpersist()

sc.stop()
```

#### Best Practices for Broadcast

```python
# DO: Broadcast large, read-only data
bc_lookup = sc.broadcast(large_dictionary)
rdd.map(lambda x: bc_lookup.value[x])

# DON'T: Broadcast small data (not worth the overhead)
small_list = [1, 2, 3]
rdd.filter(lambda x: x in small_list)  # Just use directly

# DON'T: Modify broadcast data (it's read-only on executors)
bc_data = sc.broadcast([1, 2, 3])
# bc_data.value.append(4)  # This WON'T work as expected!

# DO: Unpersist when done to free memory
bc_data.unpersist()
```

### Accumulators (Review and Deep Dive)

Accumulators allow tasks to add values that are aggregated at the driver.

#### Built-in Accumulator Types

```python
# Numeric accumulators
int_acc = sc.accumulator(0)
float_acc = sc.accumulator(0.0)
```

#### Using Accumulators Correctly

```python
# CORRECT: Use in actions
rdd = sc.parallelize([1, 2, 3, 4, 5])

counter = sc.accumulator(0)
total = sc.accumulator(0)

def process(x):
    counter.add(1)
    total.add(x)
    return x * 2

# Transformations are lazy - accumulator updates only happen with action
result = rdd.map(process)

# At this point, counter and total are still 0!
print(f"Before action - Counter: {counter.value}, Total: {total.value}")

# Trigger action
output = result.collect()

# Now accumulators have values
print(f"After action - Counter: {counter.value}, Total: {total.value}")
```

#### Accumulator Guarantees

**Guaranteed behavior:**
- Accumulator updates in `foreach()` are guaranteed to execute once

**Not guaranteed:**
- Updates in transformations may execute more than once (if task is retried)
- Updates in transformations may not execute (if partition is not needed)

```python
# Reliable: use foreach for accumulator updates
rdd.foreach(lambda x: counter.add(1))

# Less reliable: transformation may be retried
rdd.map(lambda x: (counter.add(1), x)[1]).count()
```

### Custom Accumulators

Create custom accumulators for complex aggregations:

```python
from pyspark.accumulators import AccumulatorParam

class ListAccumulatorParam(AccumulatorParam):
    def zero(self, initial_value):
        return []
    
    def addInPlace(self, v1, v2):
        return v1 + v2

# Create custom accumulator
list_acc = sc.accumulator([], ListAccumulatorParam())

def collect_errors(record):
    if record.get("error"):
        list_acc.add([record["id"]])
    return record

rdd.map(collect_errors).count()
print(f"Error IDs: {list_acc.value}")
```

## Code Example

```python
from pyspark import SparkContext
from pyspark.accumulators import AccumulatorParam

sc = SparkContext("local[*]", "SharedVariablesDemo")

# Sample data: user events
events = sc.parallelize([
    {"user_id": "U001", "country": "US", "action": "login"},
    {"user_id": "U002", "country": "UK", "action": "purchase"},
    {"user_id": "U003", "country": "DE", "action": "login"},
    {"user_id": "U004", "country": "XX", "action": "error"},  # Invalid country
    {"user_id": "U005", "country": "US", "action": "logout"},
    {"user_id": "U006", "country": "FR", "action": "purchase"},
    {"user_id": "U007", "country": "YY", "action": "error"},  # Invalid country
    {"user_id": "U008", "country": "UK", "action": "login"},
])

# === BROADCAST EXAMPLE ===
print("=== Broadcast Variable Demo ===")

# Lookup table to broadcast
country_lookup = {
    "US": {"name": "United States", "region": "North America"},
    "UK": {"name": "United Kingdom", "region": "Europe"},
    "DE": {"name": "Germany", "region": "Europe"},
    "FR": {"name": "France", "region": "Europe"},
}

bc_countries = sc.broadcast(country_lookup)

# Enrich events with country info
def enrich_event(event):
    country_info = bc_countries.value.get(event["country"], {"name": "Unknown", "region": "Unknown"})
    return {
        **event,
        "country_name": country_info["name"],
        "region": country_info["region"]
    }

enriched = events.map(enrich_event)
print("Enriched events:")
for event in enriched.collect():
    print(f"  {event['user_id']}: {event['country_name']} ({event['region']})")

# === ACCUMULATOR EXAMPLE ===
print("\n=== Accumulator Demo ===")

# Create accumulators for metrics
total_events = sc.accumulator(0)
login_events = sc.accumulator(0)
purchase_events = sc.accumulator(0)
invalid_countries = sc.accumulator(0)

# Custom accumulator for collecting invalid records
class StringSetParam(AccumulatorParam):
    def zero(self, initial_value):
        return set()
    def addInPlace(self, v1, v2):
        return v1.union(v2) if isinstance(v2, set) else v1.union({v2})

invalid_ids = sc.accumulator(set(), StringSetParam())

def process_event(event):
    total_events.add(1)
    
    if event["action"] == "login":
        login_events.add(1)
    elif event["action"] == "purchase":
        purchase_events.add(1)
    
    if event["country"] not in bc_countries.value:
        invalid_countries.add(1)
        invalid_ids.add(event["user_id"])
    
    return event

# Process all events
events.foreach(process_event)

print("Event Statistics:")
print(f"  Total events: {total_events.value}")
print(f"  Login events: {login_events.value}")
print(f"  Purchase events: {purchase_events.value}")
print(f"  Invalid countries: {invalid_countries.value}")
print(f"  Invalid user IDs: {invalid_ids.value}")

# === COMBINED USAGE ===
print("\n=== Combined Usage: Regional Analysis ===")

region_counter = {
    "North America": sc.accumulator(0),
    "Europe": sc.accumulator(0),
    "Unknown": sc.accumulator(0),
}

def count_by_region(event):
    country_info = bc_countries.value.get(event["country"], {"region": "Unknown"})
    region = country_info["region"]
    region_counter[region].add(1)
    return event

events.foreach(count_by_region)

print("Events by Region:")
for region, acc in region_counter.items():
    print(f"  {region}: {acc.value}")

# Cleanup
bc_countries.unpersist()
sc.stop()
```

**Output:**
```
=== Broadcast Variable Demo ===
Enriched events:
  U001: United States (North America)
  U002: United Kingdom (Europe)
  U003: Germany (Europe)
  U004: Unknown (Unknown)
  U005: United States (North America)
  U006: France (Europe)
  U007: Unknown (Unknown)
  U008: United Kingdom (Europe)

=== Accumulator Demo ===
Event Statistics:
  Total events: 8
  Login events: 3
  Purchase events: 2
  Invalid countries: 2
  Invalid user IDs: {'U004', 'U007'}

=== Combined Usage: Regional Analysis ===
Events by Region:
  North America: 2
  Europe: 4
  Unknown: 2
```

## Summary
- Shared variables solve data distribution and aggregation challenges in distributed computing
- Broadcast variables efficiently share large read-only data across all executors
- Use broadcast for lookup tables, model parameters, or any large shared data
- Accumulators aggregate values from executors back to the driver
- Accumulators are guaranteed to update correctly only within actions like foreach
- Unpersist broadcast variables when no longer needed to free memory
- Custom AccumulatorParam classes enable complex aggregation patterns

## Additional Resources
- [Broadcast Variables](https://spark.apache.org/docs/latest/rdd-programming-guide.html#broadcast-variables)
- [Accumulators](https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators)
- [Shared Variables Overview](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shared-variables)
