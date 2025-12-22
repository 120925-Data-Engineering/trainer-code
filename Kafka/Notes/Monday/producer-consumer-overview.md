# Producer-Consumer Overview

## Learning Objectives
- Understand the producer-consumer model in Kafka
- Explain message delivery semantics: at-least-once, at-most-once, exactly-once
- Describe how consumers track their position using offsets

## Why This Matters

The producer-consumer model is the **core interaction pattern** in Kafka. Understanding how producers send messages and consumers receive them---along with the guarantees Kafka provides---is essential for building reliable data pipelines.

This knowledge is foundational for our Weekly Epic goal of implementing a functional producer-consumer pipeline.

## The Concept

### The Producer-Consumer Model

In Kafka, data flows in a unidirectional pipeline:

```
+----------+      +-------+      +----------+
| Producer | ---> | Topic | ---> | Consumer |
+----------+      +-------+      +----------+
   (write)        (store)          (read)
```

**Key characteristics:**
- **Producers** push data to topics
- **Consumers** pull data from topics
- The **topic** acts as a persistent buffer between them
- Producers and consumers operate independently

### The Complete Flow

```
1. Producer creates a message
        |
        v
2. Producer serializes the message (key + value)
        |
        v
3. Producer determines the target partition
        |
        v
4. Message is sent to the broker
        |
        v
5. Broker appends message to partition log
        |
        v
6. Broker acknowledges receipt (based on acks setting)
        |
        v
7. Consumer polls for new messages
        |
        v
8. Consumer deserializes and processes the message
        |
        v
9. Consumer commits the offset
```

### Message Delivery Semantics

A critical concept in distributed messaging is **delivery semantics**---the guarantees about how many times a message will be delivered.

| Semantic | Description | Use Case |
|----------|-------------|----------|
| **At-most-once** | Messages may be lost, never duplicated | Metrics where some loss is acceptable |
| **At-least-once** | Messages never lost, may be duplicated | Most common; requires idempotent processing |
| **Exactly-once** | Messages delivered exactly one time | Financial transactions, critical operations |

### At-Most-Once Delivery

**Flow:**
1. Consumer receives message
2. Consumer commits offset immediately
3. Consumer processes message
4. If processing fails, message is lost

```
Consumer commits BEFORE processing

Receive --> Commit --> Process
                         |
                         v
                   (If crash here, message lost)
```

**Configuration:**
```python
consumer = KafkaConsumer(
    'events',
    enable_auto_commit=True,  # Commits immediately
    auto_commit_interval_ms=100  # Very frequent commits
)
```

### At-Least-Once Delivery

**Flow:**
1. Consumer receives message
2. Consumer processes message
3. Consumer commits offset after successful processing
4. If crash before commit, message is re-delivered

```
Consumer commits AFTER processing

Receive --> Process --> Commit
               |
               v
          (If crash here, message re-delivered)
```

**Configuration:**
```python
consumer = KafkaConsumer(
    'events',
    enable_auto_commit=False  # Manual commit after processing
)

for message in consumer:
    process(message)  # Process first
    consumer.commit()  # Then commit
```

**Handling duplicates:**
Your processing logic must be **idempotent**---applying the same message twice should have no additional effect.

### Exactly-Once Semantics (EOS)

Kafka supports exactly-once semantics through:
1. **Idempotent producers**: Prevent duplicate writes
2. **Transactions**: Atomic writes across topics
3. **Consumer isolation**: Read only committed messages

```python
# Producer with exactly-once setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    enable_idempotence=True,
    acks='all',
    transactional_id='my-transactional-producer'
)

producer.init_transactions()
producer.begin_transaction()
try:
    producer.send('topic1', value=b'message1')
    producer.send('topic2', value=b'message2')
    producer.commit_transaction()
except Exception:
    producer.abort_transaction()
```

### Consumer Offsets

An **offset** is a sequential ID that uniquely identifies each message within a partition. Consumers track their position using offsets.

```
Partition 0:
+------+------+------+------+------+------+
|  0   |  1   |  2   |  3   |  4   |  5   |
+------+------+------+------+------+------+
                      ^
                      |
              Consumer offset = 3
              (will read message 3 next)
```

**Offset management:**
- Offsets are stored in a special topic: `__consumer_offsets`
- Each consumer group maintains its own offset per partition
- Consumers can commit offsets automatically or manually

### Consumer Groups

Multiple consumers can form a **consumer group** to parallelize processing.

```
Topic with 3 partitions:

    Partition 0 ----+
                    |
    Partition 1 ----+--> Consumer Group "processors"
                    |        |
    Partition 2 ----+        +--> Consumer A (P0)
                             +--> Consumer B (P1)
                             +--> Consumer C (P2)
```

**Key rules:**
- Each partition is assigned to exactly one consumer in a group
- If consumers < partitions: some consumers handle multiple partitions
- If consumers > partitions: some consumers sit idle
- Different consumer groups read the topic independently

### Rebalancing

When consumers join or leave a group, Kafka **rebalances** partition assignments.

```
Before: 2 consumers, 4 partitions
  Consumer A: P0, P1
  Consumer B: P2, P3

Consumer C joins...

After: 3 consumers, 4 partitions
  Consumer A: P0
  Consumer B: P2
  Consumer C: P1, P3
```

### Producer Acknowledgments

Producers can configure how many acknowledgments they require:

| acks | Behavior | Durability | Latency |
|------|----------|------------|---------|
| `0` | Fire and forget | Low | Lowest |
| `1` | Leader acknowledges | Medium | Low |
| `all` (-1) | All ISRs acknowledge | High | Higher |

```python
# High durability configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',  # Wait for all replicas
    retries=3    # Retry on failure
)
```

### Semantic Summary

| Aspect | At-Most-Once | At-Least-Once | Exactly-Once |
|--------|--------------|---------------|--------------|
| Data Loss | Possible | No | No |
| Duplicates | No | Possible | No |
| Complexity | Low | Medium | High |
| Performance | Highest | High | Lower |

## Code Example

Producer-consumer with at-least-once semantics:

```python
# PRODUCER
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',  # Wait for all replicas
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send messages
for i in range(10):
    message = {'event_id': i, 'data': f'Event {i}'}
    future = producer.send('events', value=message)
    result = future.get(timeout=10)  # Block until sent
    print(f"Sent: {message} to partition {result.partition}")

producer.close()

# -----------------------------------------------------------

# CONSUMER (at-least-once)
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'events',
    bootstrap_servers='localhost:9092',
    group_id='my-consumer-group',
    enable_auto_commit=False,  # Manual commit
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for message in consumer:
    try:
        event = message.value
        print(f"Processing: {event}")
        
        # Your processing logic here
        process_event(event)
        
        # Commit only after successful processing
        consumer.commit()
        
    except Exception as e:
        print(f"Error processing: {e}")
        # Message will be re-delivered on next poll
```

## Summary

- Producers write messages; consumers read them; topics store them
- **At-most-once**: Fast but may lose messages
- **At-least-once**: Safe but requires idempotent processing
- **Exactly-once**: Guaranteed single delivery with higher complexity
- **Offsets** track consumer position in each partition
- **Consumer groups** enable parallel processing with automatic partition assignment
- Producer **acks** setting controls durability vs. latency trade-off

## Additional Resources

- [Kafka Consumer Documentation](https://kafka.apache.org/documentation/#consumerapi)
- [Exactly-Once Semantics in Kafka](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- [Consumer Group Protocol](https://developer.confluent.io/learn-kafka/apache-kafka/consumer-group-protocol/)
