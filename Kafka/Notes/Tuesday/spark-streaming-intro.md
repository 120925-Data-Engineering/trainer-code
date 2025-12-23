# Introduction to Spark Streaming

## Learning Objectives

- Define what Spark Streaming is and its role in real-time data processing
- Differentiate between DStreams (legacy) and Structured Streaming (modern)
- Understand the micro-batch processing model
- Explain how Spark Streaming differs from traditional batch processing

## Why This Matters

As part of this week's epic on **Event Streaming and Real-Time Data Processing**, you have seen how Kafka captures and delivers events as they happen. But capturing data is only half the equation. To derive value from streaming data, you need a processing engine that can handle continuous data flows.

**Spark Streaming** extends Apache Spark's batch processing capabilities to handle real-time data. This enables you to use the same APIs and mental models you learned in Weeks 1 and 2 (RDDs, DataFrames, Spark SQL) on live data streams.

## The Concept

### What is Spark Streaming?

Spark Streaming is an extension of core Spark that enables **scalable, high-throughput, fault-tolerant** stream processing of live data streams. Data can be ingested from many sources (Kafka, Flume, sockets, etc.) and processed using complex algorithms.

```
        +-------------+      +-----------------+      +-------------+
Live    |   Input     |      |     Spark       |      |   Output    |
Data -> |   Sources   | ---> |    Streaming    | ---> |   Sinks     |
        | (Kafka,etc) |      |   Processing    |      | (DB, Files) |
        +-------------+      +-----------------+      +-------------+
```

### Two Approaches to Spark Streaming

Apache Spark provides two APIs for stream processing:

| API | Status | Processing Model | Recommended |
|-----|--------|------------------|-------------|
| **DStreams** | Legacy | Micro-batch on RDDs | No (deprecated) |
| **Structured Streaming** | Modern | Micro-batch on DataFrames | Yes |

#### DStreams (Discretized Streams)

DStreams was the original Spark Streaming API, introduced in Spark 0.7. It represents a continuous stream as a series of RDDs, each containing data from a small time interval (micro-batch).

```
Time:      t0        t1        t2        t3
           |         |         |         |
Stream: ---+---------+---------+---------+---->
           |         |         |         |
DStream:  RDD0      RDD1      RDD2      RDD3
```

**Characteristics:**
- Built on RDDs (low-level API)
- Requires manual state management
- Limited support for event-time processing
- Still available but **not recommended** for new projects

#### Structured Streaming

Structured Streaming is the modern, recommended approach. It treats a live data stream as an **unbounded table** that is continuously appended with new data.

```
                    Unbounded Input Table
                    +---------------------+
                    |  id  |  value  | ts |
                    +------+---------+----+
Existing rows  -->  |  1   |   A     | .. |
                    |  2   |   B     | .. |
                    |  3   |   C     | .. |
New data      -->   |  4   |   D     | .. |  <-- New row added
                    +------+---------+----+
```

**Characteristics:**
- Built on DataFrames and Spark SQL
- Same API as batch processing (use `df.filter()`, `df.groupBy()`, etc.)
- Native support for event-time and watermarks
- Exactly-once processing guarantees
- Recommended for all new streaming applications

### Micro-Batch Processing

Both DStreams and Structured Streaming use a **micro-batch** processing model by default. Instead of processing each event individually, Spark collects events over a small time interval (the trigger interval) and processes them as a batch.

```
Events:  e1  e2  e3  e4  e5  e6  e7  e8  e9  ...
         |----------|  |----------|  |----------|
         Micro-batch1  Micro-batch2  Micro-batch3
         
Trigger Interval: e.g., 1 second
```

**Advantages of Micro-Batching:**

1. **Efficiency**: Batch operations are more efficient than per-record processing
2. **Fault Tolerance**: If a batch fails, only that batch needs to be reprocessed
3. **Consistency**: Easier to guarantee exactly-once semantics
4. **Familiar APIs**: Reuse batch processing logic

**Trade-offs:**

| Aspect | Micro-Batch | True Streaming |
|--------|-------------|----------------|
| Latency | Higher (seconds) | Lower (milliseconds) |
| Throughput | Higher | Lower |
| Complexity | Lower | Higher |
| Spark Support | Default | Continuous mode (experimental) |

For most use cases, micro-batch latency (sub-second to a few seconds) is acceptable.

### Streaming vs. Batch Processing

| Aspect | Batch Processing | Stream Processing |
|--------|------------------|-------------------|
| **Data** | Bounded, complete dataset | Unbounded, continuous flow |
| **Timing** | Process after all data arrives | Process as data arrives |
| **Latency** | Minutes to hours | Seconds to milliseconds |
| **Use Case** | Historical analysis, ETL | Real-time alerts, dashboards |
| **Spark API** | `spark.read()` | `spark.readStream()` |

Spark Streaming allows you to use nearly identical code for both paradigms, making it easy to transition from batch to streaming.

## Code Example: Structured Streaming Basics

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("StructuredStreamingIntro") \
    .getOrCreate()

# Read a stream (example: from a socket)
# In production, you would read from Kafka
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Process the stream (same as batch DataFrame operations)
word_counts = lines.selectExpr("explode(split(value, ' ')) as word") \
    .groupBy("word") \
    .count()

# Write the stream to console
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
```

Notice how the code looks almost identical to batch processing. The key differences:
- `readStream` instead of `read`
- `writeStream` instead of `write`
- An output mode (`append`, `complete`, or `update`)

## Summary

- **Spark Streaming** enables real-time data processing using Spark's familiar APIs
- **DStreams** is the legacy API (RDD-based); **Structured Streaming** is the modern API (DataFrame-based)
- Both use **micro-batch** processing: collecting events into small batches for efficient processing
- Structured Streaming treats streams as **unbounded tables**, making it intuitive for SQL users
- The same DataFrame operations work for both batch and streaming, reducing learning curve

## Additional Resources

- [Apache Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Databricks: Introduction to Structured Streaming](https://docs.databricks.com/structured-streaming/index.html)
- [Spark Streaming vs. Structured Streaming Comparison](https://www.databricks.com/blog/2016/07/28/structured-streaming-in-apache-spark.html)
