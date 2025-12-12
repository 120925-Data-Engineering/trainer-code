# SparkSession Deep Dive

## Learning Objectives
- Master the creation and configuration of SparkSession objects
- Understand advanced configuration options and when to use them
- Learn best practices for SparkSession management in production applications

## Why This Matters

While the previous topic introduced SparkSession as the entry point, this topic takes you deeper into its practical usage. In real-world data engineering, you will need to:

- Configure SparkSession for different environments (development, testing, production)
- Tune memory restrictions, execution settings, and SQL-specific options
- Manage session lifecycle properly to avoid resource leaks
- Understand how configurations affect performance

Mastering SparkSession configuration is foundational to our Weekly Epic of *Mastering Spark SQL and DataFrames*. Every optimization, every DataFrame operation, and every SQL query depends on how well your SparkSession is configured.

## The Concept

### SparkSession Configuration Categories

SparkSession configurations fall into several categories:

| Category | Examples | Purpose |
|----------|----------|---------|
| Application | `spark.app.name`, `spark.master` | Identify and locate your application |
| Execution | `spark.executor.*`, `spark.driver.*` | Control resource allocation |
| SQL | `spark.sql.*` | Tune DataFrame and SQL behavior |
| Shuffle | `spark.sql.shuffle.partitions` | Optimize data redistribution |
| Memory | `spark.memory.*` | Configure memory management |
| I/O | `spark.sql.parquet.*`, `spark.sql.csv.*` | Control file reading/writing |

### Configuration Methods

There are multiple ways to set configurations:

**1. At Build Time (Recommended for static configs)**
```python
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()
```

**2. At Runtime (For dynamic adjustments)**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

**3. Environment Variables**
```bash
export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3
```

**4. Configuration Files (spark-defaults.conf)**
```properties
spark.sql.shuffle.partitions 200
spark.driver.memory 4g
```

### Common Configuration Options

#### SQL and DataFrame Settings

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.sql.shuffle.partitions` | 200 | Number of partitions for shuffles (joins, aggregations) |
| `spark.sql.adaptive.enabled` | true (3.0+) | Enable adaptive query execution |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Maximum size for broadcast joins |
| `spark.sql.parquet.compression.codec` | snappy | Compression for Parquet files |

#### Driver and Executor Settings

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.driver.memory` | 1g | Memory allocated to the driver |
| `spark.executor.memory` | 1g | Memory allocated to each executor |
| `spark.executor.cores` | 1 | CPU cores per executor |
| `spark.dynamicAllocation.enabled` | false | Enable dynamic executor scaling |

### Session Lifecycle Management

```
       +---------------+
       |    Create     |
       |  SparkSession |
       +---------------+
              |
              v
       +---------------+
       |   Configure   |
       |   (builder)   |
       +---------------+
              |
              v
       +---------------+
       |    Active     |
       |   (running)   |
       +---------------+
              |
              v
       +---------------+
       |     Stop      |
       | spark.stop()  |
       +---------------+
```

**Important**: Always call `spark.stop()` when your application finishes to properly release resources.

### Multiple Sessions

While you typically use one SparkSession per application, you can create multiple sessions:

```python
# First session
spark1 = SparkSession.builder \
    .appName("Session1") \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

# Second session with different config
spark2 = spark1.newSession()
spark2.conf.set("spark.sql.shuffle.partitions", "50")

# They share the same SparkContext but have independent SQL configs
```

## Code Example

### Production-Ready SparkSession Setup

```python
from pyspark.sql import SparkSession
import os

def create_spark_session(app_name: str, environment: str = "development") -> SparkSession:
    """
    Create a SparkSession with environment-appropriate configuration.
    
    Args:
        app_name: Name of the Spark application
        environment: One of 'development', 'testing', 'production'
    
    Returns:
        Configured SparkSession
    """
    
    builder = SparkSession.builder.appName(app_name)
    
    # Base configurations for all environments
    builder = builder \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    if environment == "development":
        # Local development settings
        builder = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.ui.showConsoleProgress", "true")
    
    elif environment == "testing":
        # Testing settings (smaller resources)
        builder = builder \
            .master("local[2]") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "4")
    
    elif environment == "production":
        # Production settings (cluster mode)
        builder = builder \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    return builder.getOrCreate()


# Usage
if __name__ == "__main__":
    env = os.getenv("SPARK_ENV", "development")
    spark = create_spark_session("MyDataPipeline", env)
    
    try:
        # Your application logic here
        df = spark.range(1000)
        print(f"Created DataFrame with {df.count()} rows")
        
        # Check configuration
        print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
    
    finally:
        # Always stop the session
        spark.stop()
```

### Inspecting Session Configuration

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ConfigInspector") \
    .getOrCreate()

# Get all SQL configurations
all_configs = spark.sparkContext.getConf().getAll()
print("All Spark Configurations:")
for key, value in sorted(all_configs):
    print(f"  {key}: {value}")

# Get specific configurations
print("\nKey Configurations:")
print(f"  App Name: {spark.sparkContext.appName}")
print(f"  Master: {spark.sparkContext.master}")
print(f"  Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  Adaptive Enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
```

### Runtime Configuration Changes

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RuntimeConfig").getOrCreate()

# Some configurations can be changed at runtime
spark.conf.set("spark.sql.shuffle.partitions", "50")

# Verify the change
print(spark.conf.get("spark.sql.shuffle.partitions"))  # Output: 50

# Check if a configuration is modifiable
try:
    spark.conf.isModifiable("spark.sql.shuffle.partitions")
except Exception as e:
    print(f"Configuration check: {e}")
```

## Summary

- SparkSession configuration is divided into categories: application, execution, SQL, shuffle, memory, and I/O
- Configurations can be set at build time (recommended for static settings) or at runtime (for dynamic adjustments)
- Key configurations include `spark.sql.shuffle.partitions`, `spark.sql.adaptive.enabled`, and memory settings
- Always follow proper lifecycle management: create, use, and stop the session
- Use environment-aware configuration patterns for development, testing, and production deployments

## Additional Resources

- [Spark Configuration Documentation](https://spark.apache.org/docs/latest/configuration.html)
- [Spark SQL Configuration Options](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Tuning Spark Applications (Databricks)](https://docs.databricks.com/en/spark/latest/spark-sql/tuning.html)
