# Spark Streaming API Deep Dive

## Learning Objectives

- Create and configure a Spark Streaming context
- Understand output modes and triggers
- Apply windowing operations to streaming data
- Manage stateful operations in streaming applications
- Configure checkpointing for fault tolerance

## Why This Matters

In the previous content, you learned what Spark Streaming is and the difference between DStreams and Structured Streaming. Now it is time to dive into the **API itself**. Understanding the Streaming API is essential for building production-grade real-time pipelines that integrate with Kafka, as defined in this week's epic.

Mastering these concepts enables you to:
- Configure how frequently Spark processes data
- Perform time-based aggregations (e.g., counts per minute)
- Track state across events (e.g., running totals, session tracking)
- Ensure your application can recover from failures

## The Concept

### Streaming Context and Session

For **Structured Streaming** (the recommended approach), you use a standard `SparkSession`. No special context is needed:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("StreamingApp") \
    .getOrCreate()
```

For legacy **DStreams**, you would create a `StreamingContext`:

```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "DStreamApp")
ssc = StreamingContext(sc, batchDuration=1)  # 1-second batches
```

The rest of this content focuses on Structured Streaming, as it is the modern standard.

### Reading Streams

Structured Streaming supports multiple input sources:

| Source | Format String | Use Case |
|--------|---------------|----------|
| Kafka | `"kafka"` | Production streaming |
| File | `"csv"`, `"json"`, `"parquet"` | File-based streaming |
| Socket | `"socket"` | Testing/development |
| Rate | `"rate"` | Load testing |

**Reading from Kafka:**

```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "earliest") \
    .load()
```

The resulting DataFrame contains columns: `key`, `value`, `topic`, `partition`, `offset`, `timestamp`, `timestampType`.

### Output Modes

When writing a streaming DataFrame, you must specify an **output mode**:

| Mode | Description | Use Case |
|------|-------------|----------|
| **append** | Only new rows are written | Simple transformations, no aggregations |
| **complete** | Entire result table is written | Aggregations where you need full state |
| **update** | Only changed rows are written | Aggregations with partial updates |

```python
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
```

**Choosing the Right Mode:**

```
                   +-------------+
                   | Aggregation |
                   |   Present?  |
                   +------+------+
                          |
            +-------------+-------------+
            |                           |
           No                          Yes
            |                           |
            v                           |
        +-------+            +----------+---------+
        | append|            | Need full table or |
        +-------+            | just changes?      |
                             +----------+---------+
                                        |
                            +-----------+-----------+
                            |                       |
                         Full                    Changes
                            |                       |
                            v                       v
                      +---------+             +---------+
                      | complete|             |  update |
                      +---------+             +---------+
```

### Triggers

Triggers control **when** Spark processes incoming data:

| Trigger Type | Behavior |
|--------------|----------|
| **Default (micro-batch)** | Process as fast as possible |
| **Fixed interval** | Process every N seconds/minutes |
| **Once** | Process all available data once, then stop |
| **Available-now** | Process all available data, then stop (Spark 3.3+) |

```python
from pyspark.sql.streaming import Trigger

# Process every 10 seconds
query = df.writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .format("console") \
    .start()

# Process once (useful for testing or batch-like behavior)
query = df.writeStream \
    .trigger(once=True) \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/output/path") \
    .start()
```

### Windowing Operations

Windowing allows you to perform aggregations over **time-based windows** of data. This is essential for questions like "How many events occurred in the last 5 minutes?"

**Types of Windows:**

```
Tumbling Windows (non-overlapping):
|----Window 1----|----Window 2----|----Window 3----|
     5 min            5 min            5 min

Sliding Windows (overlapping):
|----Window 1----|
     |----Window 2----|
          |----Window 3----|
     5 min with 1 min slide
```

**Tumbling Window Example:**

```python
from pyspark.sql.functions import window, col

# Assume df has a 'timestamp' column and 'event_type' column
windowed_counts = df \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("event_type")
    ) \
    .count()
```

**Sliding Window Example:**

```python
# 10-minute windows, sliding every 5 minutes
windowed_counts = df \
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("event_type")
    ) \
    .count()
```

### Watermarks and Late Data

In real-world streaming, data may arrive **late** (e.g., due to network delays). Watermarks tell Spark how long to wait for late data before finalizing a window.

```python
# Wait up to 10 minutes for late data
df_with_watermark = df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("event_type")
    ) \
    .count()
```

**How Watermarks Work:**

```
Event Time:   10:00    10:05    10:10    10:15    10:20
              |        |        |        |        |
              v        v        v        v        v
Watermark:  9:50     9:55    10:00    10:05    10:10
            (10 min behind current event time)

Data arriving before watermark: Processed
Data arriving after watermark: Dropped (too late)
```

### Stateful Operations

Stateful operations maintain information across micro-batches:

**Aggregations:** Automatically stateful (Spark tracks running counts, sums, etc.)

```python
# Running count by event type (state is maintained automatically)
running_counts = df.groupBy("event_type").count()
```

**MapGroupsWithState:** For custom state logic (advanced)

```python
# Custom stateful processing (requires defining update functions)
from pyspark.sql.streaming import GroupState

def update_state(key, values, state):
    # Custom logic to update state
    pass
```

### Checkpointing

Checkpointing is **critical** for production streaming applications. It saves:
- The current state of aggregations
- Kafka offsets (which messages have been processed)
- Query progress metadata

```python
query = df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()
```

**Why Checkpointing Matters:**

1. **Fault Tolerance**: If the application crashes, it restarts from the last checkpoint
2. **Exactly-Once Processing**: Prevents duplicate processing of messages
3. **State Recovery**: Restores aggregation state (running counts, etc.)

**Checkpoint Directory Contents:**

```
/checkpoint/
  /commits/       # Completed batch metadata
  /offsets/       # Kafka offsets for each batch
  /state/         # Aggregation state data
  /metadata       # Query metadata
```

## Code Example: Complete Streaming Application

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, from_json
from pyspark.sql.types import StructType, StringType, TimestampType

# Initialize Spark
spark = SparkSession.builder \
    .appName("StreamingAPIDemo") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Define schema for JSON messages
schema = StructType() \
    .add("event_type", StringType()) \
    .add("user_id", StringType()) \
    .add("timestamp", TimestampType())

# Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load()

# Parse JSON and apply watermark
parsed_df = raw_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "5 minutes")

# Windowed aggregation
windowed_counts = parsed_df \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("event_type")
    ) \
    .count()

# Write with checkpointing
query = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "/tmp/checkpoint/events") \
    .start()

query.awaitTermination()
```

## Summary

- **SparkSession** is the entry point for Structured Streaming (no special context needed)
- **Output modes** (append, complete, update) control what data is written to sinks
- **Triggers** control processing frequency (default, fixed interval, once)
- **Windowing** enables time-based aggregations (tumbling and sliding windows)
- **Watermarks** handle late-arriving data by defining a lateness threshold
- **Checkpointing** is essential for fault tolerance and exactly-once processing

## Additional Resources

- [Structured Streaming Programming Guide - Operations](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#operations-on-streaming-dataframesdatasets)
- [Window Operations on Event Time](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time)
- [Handling Late Data and Watermarking](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)
