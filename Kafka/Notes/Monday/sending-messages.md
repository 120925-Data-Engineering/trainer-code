# Sending Messages with Kafka Producers

## Learning Objectives
- Understand the structure of a Kafka message
- Learn about key, value, headers, and timestamps
- Implement message serialization strategies
- Send messages to Kafka topics using Python

## Why This Matters

Every piece of data flowing through Kafka is a **message**. Understanding message structure and serialization is critical for:

- Designing effective data schemas
- Ensuring messages are routed to the correct partitions
- Enabling downstream consumers to properly interpret data
- Debugging message flow issues

This knowledge is essential for our Weekly Epic of building producer-consumer pipelines.

## The Concept

### Anatomy of a Kafka Message

Every Kafka message consists of four components:

```
+------------------+
|       KEY        |  Optional - affects partitioning
+------------------+
|      VALUE       |  Required - the actual payload
+------------------+
|    TIMESTAMP     |  Automatic - when message was created
+------------------+
|     HEADERS      |  Optional - metadata key-value pairs
+------------------+
```

### The Key

The **key** determines which partition receives the message.

**Key behavior:**
- Messages with the same key always go to the same partition
- This guarantees ordering for related messages
- Keys are optional (null key = round-robin distribution)

**Use cases for keys:**
- User ID (all events for a user stay ordered)
- Order ID (all updates for an order stay ordered)
- Sensor ID (all readings from a sensor stay ordered)

```python
# Messages with same key go to same partition
producer.send('user-events', key=b'user-123', value=b'logged in')
producer.send('user-events', key=b'user-123', value=b'clicked button')
producer.send('user-events', key=b'user-123', value=b'logged out')
# All three messages are in the same partition, in order
```

### The Value

The **value** is the actual data payload. This is where your business data lives.

**Common formats:**
- JSON (human-readable, flexible)
- Avro (compact, schema-enforced)
- Protobuf (compact, schema-enforced)
- Plain bytes (binary data)

```python
# JSON value
value = json.dumps({
    'order_id': 'ORD-12345',
    'customer': 'John Doe',
    'items': ['laptop', 'mouse'],
    'total': 1299.99
}).encode('utf-8')

producer.send('orders', value=value)
```

### The Timestamp

Every message has a **timestamp** indicating when it was created.

**Timestamp types:**
- **CreateTime**: When the producer created the message (default)
- **LogAppendTime**: When the broker received the message

```python
# Kafka automatically adds timestamp, but you can override
from datetime import datetime

producer.send(
    'events',
    value=b'event data',
    timestamp_ms=int(datetime.now().timestamp() * 1000)
)
```

### Headers

**Headers** store optional metadata as key-value pairs.

**Use cases:**
- Tracing IDs for distributed systems
- Message version information
- Source system identification
- Content type hints

```python
headers = [
    ('trace-id', b'abc-123-xyz'),
    ('source', b'order-service'),
    ('content-type', b'application/json'),
    ('version', b'1.0')
]

producer.send('orders', value=value, headers=headers)
```

### Serialization

Kafka stores bytes. Your data must be **serialized** to bytes before sending.

**Built-in serializers:**
```python
from kafka import KafkaProducer

# String serializer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=str.encode,
    value_serializer=str.encode
)

# JSON serializer
import json
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
```

**Custom serializer example:**
```python
import json
from datetime import datetime

def serialize_event(event):
    """Custom serializer that handles datetime objects."""
    def json_serial(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    return json.dumps(event, default=json_serial).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=serialize_event
)
```

### Sending Messages: Synchronous vs Asynchronous

**Asynchronous (default, faster):**
```python
# Non-blocking - returns immediately
future = producer.send('topic', value=b'message')

# Optionally wait for result later
try:
    record_metadata = future.get(timeout=10)
    print(f"Sent to partition {record_metadata.partition}")
except Exception as e:
    print(f"Send failed: {e}")
```

**Synchronous (slower but immediate feedback):**
```python
# Blocking - waits for broker acknowledgment
try:
    future = producer.send('topic', value=b'message')
    record_metadata = future.get(timeout=10)  # Block here
    print(f"Message sent successfully to {record_metadata.topic}")
except Exception as e:
    print(f"Send failed: {e}")
```

### Using Callbacks

For asynchronous sending with error handling:

```python
def on_send_success(record_metadata):
    print(f"Message sent to {record_metadata.topic}:"
          f"{record_metadata.partition}:{record_metadata.offset}")

def on_send_error(exception):
    print(f"Failed to send message: {exception}")

# Send with callbacks
producer.send('orders', value=b'order data').add_callback(
    on_send_success
).add_errback(
    on_send_error
)
```

### Batching for Performance

Kafka producers batch messages for efficiency:

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    linger_ms=10,        # Wait up to 10ms to batch messages
    batch_size=16384     # Maximum batch size in bytes (16 KB)
)
```

**Batching trade-off:**
- Higher `linger_ms` = more batching = higher throughput
- Lower `linger_ms` = less batching = lower latency

### Flushing the Producer

Ensure all messages are sent before closing:

```python
# Send multiple messages
for i in range(100):
    producer.send('events', value=f'event-{i}'.encode())

# Flush ensures all buffered messages are sent
producer.flush()

# Always close when done
producer.close()
```

## Code Example

Complete producer with all concepts:

```python
from kafka import KafkaProducer
import json
from datetime import datetime

# Create producer with JSON serialization
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    linger_ms=5,
    batch_size=16384
)

def send_order_event(order_id, customer, items, total):
    """Send an order event to Kafka."""
    
    # Construct the message
    key = f"order-{order_id}"
    
    value = {
        'order_id': order_id,
        'customer': customer,
        'items': items,
        'total': total,
        'timestamp': datetime.now().isoformat()
    }
    
    headers = [
        ('source', b'order-service'),
        ('version', b'1.0')
    ]
    
    # Send with callback
    future = producer.send(
        topic='orders',
        key=key,
        value=value,
        headers=headers
    )
    
    # Get result (makes it synchronous for demo)
    try:
        metadata = future.get(timeout=10)
        print(f"Order {order_id} sent to partition {metadata.partition}, "
              f"offset {metadata.offset}")
        return True
    except Exception as e:
        print(f"Failed to send order {order_id}: {e}")
        return False

# Send some orders
send_order_event('ORD-001', 'Alice', ['laptop'], 999.99)
send_order_event('ORD-002', 'Bob', ['keyboard', 'mouse'], 79.99)
send_order_event('ORD-003', 'Charlie', ['monitor', 'webcam'], 349.99)

# Cleanup
producer.flush()
producer.close()
```

## Summary

- Kafka messages consist of **key**, **value**, **timestamp**, and **headers**
- The **key** determines partitioning and enables ordering guarantees
- The **value** contains your actual data payload
- **Headers** store metadata (tracing, versioning, content hints)
- Messages must be **serialized** to bytes (JSON, Avro, Protobuf)
- Producers can send **synchronously** (blocking) or **asynchronously** (non-blocking)
- **Batching** improves throughput at the cost of slight latency increase
- Always **flush** and **close** producers when finished

## Additional Resources

- [Kafka Producer API - Official Docs](https://kafka.apache.org/documentation/#producerapi)
- [Message Serialization Strategies](https://developer.confluent.io/learn-kafka/apache-kafka/serialization/)
- [Producer Configuration Reference](https://kafka.apache.org/documentation/#producerconfigs)
