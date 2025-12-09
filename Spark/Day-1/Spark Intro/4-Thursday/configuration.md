# Configuration

## Learning Objectives
- Understand SparkConf and configuration properties
- Configure memory settings for optimal performance
- Apply performance tuning options
- Set configuration through different methods

## Why This Matters
Proper configuration is essential for Spark application performance. Understanding how to tune memory, parallelism, and other settings allows you to optimize job execution, prevent failures, and make efficient use of cluster resources.

## The Concept

### Configuration Hierarchy

Spark configuration can be set at multiple levels, with later levels overriding earlier ones:

```
1. spark-defaults.conf (lowest priority)
2. SparkConf in code
3. spark-submit command line (highest priority)
```

### SparkConf Object

The programmatic way to configure Spark applications:

```python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Create configuration
conf = SparkConf() \
    .setAppName("MyApplication") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "2g")

# Create SparkContext with configuration
sc = SparkContext(conf=conf)

# Or with SparkSession
spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()
```

### Configuration via SparkSession Builder

More modern approach using builder pattern:

```python
spark = SparkSession.builder \
    .appName("ModernConfig") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

### Key Configuration Categories

#### 1. Application Properties

| Property | Description | Default |
|----------|-------------|---------|
| `spark.app.name` | Application name | None |
| `spark.master` | Cluster manager URL | None |
| `spark.driver.cores` | Driver cores (cluster mode) | 1 |
| `spark.driver.memory` | Driver memory | 1g |

#### 2. Executor Properties

| Property | Description | Default |
|----------|-------------|---------|
| `spark.executor.instances` | Number of executors | Dynamic |
| `spark.executor.memory` | Memory per executor | 1g |
| `spark.executor.cores` | Cores per executor | All available |

#### 3. Memory Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `spark.memory.fraction` | Fraction for execution/storage | 0.6 |
| `spark.memory.storageFraction` | Fraction of memory.fraction for storage | 0.5 |
| `spark.driver.maxResultSize` | Max size of action results | 1g |

#### 4. Shuffle Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `spark.sql.shuffle.partitions` | Shuffle partitions for SQL | 200 |
| `spark.default.parallelism` | Default RDD parallelism | Depends on cluster |
| `spark.shuffle.compress` | Compress shuffle output | true |

### Memory Architecture

Understanding Spark memory helps with configuration:

```
+----------------------------------------+
|           Executor Memory              |
|  +----------------------------------+  |
|  |         User Memory              |  |
|  |  (1 - spark.memory.fraction)     |  |
|  +----------------------------------+  |
|  +----------------------------------+  |
|  |        Spark Memory              |  |
|  |    (spark.memory.fraction)       |  |
|  |  +-----------+  +-----------+    |  |
|  |  | Execution |  |  Storage  |    |  |
|  |  |  Memory   |  |  Memory   |    |  |
|  |  +-----------+  +-----------+    |  |
|  +----------------------------------+  |
|  +----------------------------------+  |
|  |      Reserved Memory (300MB)     |  |
|  +----------------------------------+  |
+----------------------------------------+
```

**Execution Memory:** Used for shuffles, joins, sorts, aggregations
**Storage Memory:** Used for cached data and broadcast variables

### Common Configuration Patterns

#### Pattern 1: Memory-Intensive Jobs
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .getOrCreate()
```

#### Pattern 2: Shuffle-Heavy Jobs
```python
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.shuffle.spill.compress", "true") \
    .getOrCreate()
```

#### Pattern 3: Large Data Collection
```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()
```

### Viewing Current Configuration

```python
# View all configurations
for item in spark.sparkContext.getConf().getAll():
    print(f"{item[0]} = {item[1]}")

# Get specific configuration
memory = spark.conf.get("spark.executor.memory")
print(f"Executor memory: {memory}")

# Check if configuration exists
try:
    value = spark.conf.get("spark.some.setting")
except Exception:
    value = "Not set"
```

### Configuration File (spark-defaults.conf)

Located in `$SPARK_HOME/conf/spark-defaults.conf`:

```properties
# Application defaults
spark.master                     yarn
spark.driver.memory              2g
spark.executor.memory            4g
spark.executor.instances         4

# Performance tuning
spark.sql.shuffle.partitions     200
spark.default.parallelism        100

# Serialization
spark.serializer                 org.apache.spark.serializer.KryoSerializer
```

### Command Line Configuration

Override any configuration via spark-submit:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 4 \
    --num-executors 10 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    my_app.py
```

### Dynamic Configuration

Some configurations can be changed at runtime:

```python
# Runtime SQL configurations
spark.conf.set("spark.sql.shuffle.partitions", "100")

# Check current value
print(spark.conf.get("spark.sql.shuffle.partitions"))
```

**Note:** Not all configurations are dynamic. Core settings like executor memory must be set at startup.

## Code Example

```python
from pyspark import SparkConf
from pyspark.sql import SparkSession

def demonstrate_configuration():
    """Demonstrate various configuration approaches."""
    
    # Method 1: SparkConf object
    conf = SparkConf()
    conf.set("spark.app.name", "ConfigurationDemo")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.sql.shuffle.partitions", "100")
    
    # Method 2: SparkSession builder (preferred)
    spark = SparkSession.builder \
        .appName("ConfigurationDemo") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.ui.showConsoleProgress", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
    # Display all current configurations
    print("=" * 60)
    print("CURRENT SPARK CONFIGURATION")
    print("=" * 60)
    
    # Key configurations to display
    key_configs = [
        "spark.app.name",
        "spark.master",
        "spark.driver.memory",
        "spark.executor.memory",
        "spark.sql.shuffle.partitions",
        "spark.default.parallelism",
    ]
    
    all_conf = dict(sc.getConf().getAll())
    
    print("\nKey Configurations:")
    for key in key_configs:
        value = all_conf.get(key, spark.conf.get(key, "Not set"))
        print(f"  {key}: {value}")
    
    # Dynamic configuration example
    print("\n" + "=" * 60)
    print("DYNAMIC CONFIGURATION CHANGES")
    print("=" * 60)
    
    # Get current shuffle partitions
    current = spark.conf.get("spark.sql.shuffle.partitions")
    print(f"Current shuffle partitions: {current}")
    
    # Change dynamically
    spark.conf.set("spark.sql.shuffle.partitions", "50")
    new_value = spark.conf.get("spark.sql.shuffle.partitions")
    print(f"New shuffle partitions: {new_value}")
    
    # Reset to original
    spark.conf.set("spark.sql.shuffle.partitions", current)
    
    # Demonstrate effect of parallelism
    print("\n" + "=" * 60)
    print("PARALLELISM DEMONSTRATION")
    print("=" * 60)
    
    data = list(range(10000))
    
    # Default parallelism
    rdd1 = sc.parallelize(data)
    print(f"Default parallelism: {sc.defaultParallelism}")
    print(f"RDD partitions (default): {rdd1.getNumPartitions()}")
    
    # Specified parallelism
    rdd2 = sc.parallelize(data, numSlices=20)
    print(f"RDD partitions (specified 20): {rdd2.getNumPartitions()}")
    
    # Show memory configuration
    print("\n" + "=" * 60)
    print("MEMORY CONFIGURATION")
    print("=" * 60)
    
    memory_configs = [
        ("spark.driver.memory", "Driver memory"),
        ("spark.executor.memory", "Executor memory"),
        ("spark.memory.fraction", "Memory fraction for Spark"),
        ("spark.memory.storageFraction", "Storage fraction"),
        ("spark.driver.maxResultSize", "Max result size"),
    ]
    
    for conf_key, description in memory_configs:
        try:
            value = spark.conf.get(conf_key)
        except Exception:
            value = "Default"
        print(f"  {description}: {value}")
    
    # Performance test with different shuffle partitions
    print("\n" + "=" * 60)
    print("SHUFFLE PARTITIONS IMPACT")
    print("=" * 60)
    
    import time
    
    df = spark.range(100000)
    
    for partitions in [10, 50, 200]:
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        
        start = time.time()
        result = df.groupBy((df.id % 100).alias("group")).count()
        result.collect()
        elapsed = time.time() - start
        
        print(f"  {partitions} partitions: {elapsed:.3f}s")
    
    # Cleanup
    spark.stop()
    print("\nConfiguration demo completed.")

if __name__ == "__main__":
    demonstrate_configuration()
```

**Output:**
```
============================================================
CURRENT SPARK CONFIGURATION
============================================================

Key Configurations:
  spark.app.name: ConfigurationDemo
  spark.master: local[*]
  spark.driver.memory: 2g
  spark.executor.memory: Not set
  spark.sql.shuffle.partitions: 100
  spark.default.parallelism: 8

============================================================
DYNAMIC CONFIGURATION CHANGES
============================================================
Current shuffle partitions: 100
New shuffle partitions: 50

============================================================
PARALLELISM DEMONSTRATION
============================================================
Default parallelism: 8
RDD partitions (default): 8
RDD partitions (specified 20): 20

============================================================
MEMORY CONFIGURATION
============================================================
  Driver memory: 2g
  Executor memory: Default
  Memory fraction for Spark: Default
  Storage fraction: Default
  Max result size: Default

============================================================
SHUFFLE PARTITIONS IMPACT
============================================================
  10 partitions: 0.234s
  50 partitions: 0.312s
  200 partitions: 0.567s

Configuration demo completed.
```

## Summary
- SparkConf and SparkSession.builder provide programmatic configuration
- Configuration can be set via code, spark-defaults.conf, or spark-submit
- Command-line options override code and file configurations
- Memory configuration affects execution and storage capacity
- Shuffle partitions significantly impact job performance
- Some configurations can be changed dynamically at runtime
- View current configuration using getConf().getAll() or spark.conf.get()

## Additional Resources
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Memory Management](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)
- [Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
