# Creating Kafka Topics

## Learning Objectives
- Create Kafka topics using command-line tools
- Create topics programmatically using Python
- Configure partitions, replication factor, and retention policies

## Why This Matters

Topics are the **fundamental organizing structure** in Kafka. Before you can send or receive any messages, you need a topic. Understanding how to create and configure topics properly is essential for:

- Ensuring appropriate parallelism (partition count)
- Guaranteeing fault tolerance (replication factor)
- Managing storage costs (retention policies)

This hands-on skill directly supports our Weekly Epic goal of implementing a functional producer-consumer pipeline.

## The Concept

### Topic Configuration Basics

When creating a topic, you must consider three key settings:

| Setting | Description | Typical Values |
|---------|-------------|----------------|
| **Partitions** | Number of partitions for parallelism | 3-12 for moderate load |
| **Replication Factor** | Number of copies for fault tolerance | 2-3 in production |
| **Retention** | How long messages are kept | 7 days (default), or size-based |

### Creating Topics via CLI

Kafka provides the `kafka-topics.sh` (or `kafka-topics.bat` on Windows) command for topic management.

**Basic Syntax:**
```bash
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic <topic-name> \
  --partitions <number> \
  --replication-factor <number>
```

**Example: Create a topic for user events**
```bash
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --partitions 3 \
  --replication-factor 1
```

**Output:**
```
Created topic user-events.
```

### Topic Naming Conventions

Good topic names are:
- **Descriptive**: Indicate the type of data they contain
- **Consistent**: Follow a naming pattern across your organization
- **Lowercase with hyphens**: Avoid spaces and special characters

**Examples:**
```
Good:
  order-events
  user-clicks
  payment-transactions
  sensor-readings-warehouse-a

Avoid:
  Topic1
  myTopic
  Order Events (spaces)
  user.clicks (dots can cause issues)
```

### Configuration Options

You can set additional configurations when creating topics:

```bash
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --partitions 6 \
  --replication-factor 2 \
  --config retention.ms=604800000 \
  --config max.message.bytes=1048576
```

**Common Configuration Properties:**

| Property | Description | Default |
|----------|-------------|---------|
| `retention.ms` | Time to retain messages (ms) | 604800000 (7 days) |
| `retention.bytes` | Max bytes to retain per partition | -1 (unlimited) |
| `max.message.bytes` | Max size of a single message | 1048576 (1 MB) |
| `cleanup.policy` | `delete` or `compact` | delete |

### Creating Topics Programmatically

Using the `kafka-python-ng` library, you can create topics from your Python applications.

**Installation:**
```bash
pip install kafka-python-ng
```

**Creating a topic with Python:**
```python
from kafka.admin import KafkaAdminClient, NewTopic

# Connect to Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id='topic-creator'
)

# Define the new topic
new_topic = NewTopic(
    name='order-events',
    num_partitions=3,
    replication_factor=1
)

# Create the topic
admin_client.create_topics(new_topics=[new_topic], validate_only=False)
print("Topic 'order-events' created successfully!")

# Close the connection
admin_client.close()
```

**Creating multiple topics at once:**
```python
from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')

topics = [
    NewTopic(name='user-events', num_partitions=3, replication_factor=1),
    NewTopic(name='payment-events', num_partitions=6, replication_factor=1),
    NewTopic(name='inventory-updates', num_partitions=3, replication_factor=1)
]

admin_client.create_topics(new_topics=topics)
print("All topics created successfully!")
admin_client.close()
```

### Using confluent-kafka Library

An alternative library is `confluent-kafka`, which is the official Confluent client:

```bash
pip install confluent-kafka
```

```python
from confluent_kafka.admin import AdminClient, NewTopic

# Connect to Kafka
admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

# Define new topics
new_topics = [
    NewTopic('sensor-data', num_partitions=4, replication_factor=1),
    NewTopic('alerts', num_partitions=2, replication_factor=1)
]

# Create topics
futures = admin.create_topics(new_topics)

# Wait for completion
for topic, future in futures.items():
    try:
        future.result()  # Block until complete
        print(f"Topic '{topic}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")
```

### Partition Count Guidelines

Choosing the right partition count depends on your use case:

| Scenario | Recommended Partitions |
|----------|------------------------|
| Low throughput, single consumer | 1-3 |
| Moderate throughput, small team | 3-6 |
| High throughput, large consumer group | 12+ |
| Maximum parallelism needed | Match consumer count |

**Rules of thumb:**
- You can increase partitions later, but you cannot decrease them
- More partitions = more parallelism but more overhead
- Aim for partitions >= number of consumers in a group

### Replication Factor Guidelines

| Environment | Recommended Replication Factor |
|-------------|-------------------------------|
| Development | 1 (single broker is fine) |
| Staging | 2 |
| Production | 3 (standard), 5 (critical data) |

**Important:** Replication factor cannot exceed the number of brokers in your cluster.

## Code Example

Complete example of creating a topic with error handling:

```python
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_topic(topic_name, partitions=3, replication_factor=1):
    """Create a Kafka topic with error handling."""
    
    admin_client = KafkaAdminClient(
        bootstrap_servers='localhost:9092',
        client_id='topic-manager'
    )
    
    topic = NewTopic(
        name=topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor
    )
    
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created with {partitions} partitions")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()

# Usage
create_topic('my-events', partitions=4)
create_topic('my-logs', partitions=2)
```

## Summary

- Topics are created using CLI (`kafka-topics.sh`) or programmatically (Python libraries)
- Key configurations include **partitions**, **replication factor**, and **retention**
- Use meaningful, lowercase-hyphenated names for topics
- Partition count affects parallelism and cannot be decreased after creation
- Replication factor should be 3 for production workloads
- Both `kafka-python-ng` and `confluent-kafka` libraries support topic administration

## Additional Resources

- [Kafka CLI Tools - Official Docs](https://kafka.apache.org/documentation/#topicconfigs)
- [kafka-python AdminClient Documentation](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaAdminClient.html)
- [Confluent Admin Client Guide](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#admin-api)
