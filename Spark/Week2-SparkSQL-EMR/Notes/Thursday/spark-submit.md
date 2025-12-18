# Spark Submit

## Learning Objectives

- Master the spark-submit command and its options
- Understand deployment modes (client vs cluster)
- Package and deploy Spark applications
- Pass arguments and configurations effectively

## Why This Matters

spark-submit is the gateway to running Spark applications in production. Whether you are deploying to YARN, Kubernetes, or a standalone cluster, understanding spark-submit options ensures your applications run with the right resources and configurations.

## The Concept

### What is spark-submit?

spark-submit is the command-line tool for submitting Spark applications to a cluster. It handles:

- Packaging your application
- Setting up the classpath
- Configuring resources
- Launching the application on the cluster

### Basic Syntax

```bash
spark-submit [options] <app> [app arguments]
```

Example:

```bash
spark-submit --master yarn --deploy-mode cluster my_app.py --input data.csv
```

### Key Options

#### Cluster Options

| Option | Description |
|--------|-------------|
| `--master` | Cluster manager URL |
| `--deploy-mode` | client or cluster |
| `--name` | Application name |
| `--class` | Main class (for JVM apps) |

#### Resource Options

| Option | Description |
|--------|-------------|
| `--driver-memory` | Memory for driver |
| `--driver-cores` | Cores for driver (cluster mode) |
| `--executor-memory` | Memory per executor |
| `--executor-cores` | Cores per executor |
| `--num-executors` | Number of executors |
| `--total-executor-cores` | Total cores (standalone/Mesos) |

#### Dependency Options

| Option | Description |
|--------|-------------|
| `--py-files` | Python files to add (.py, .zip, .egg) |
| `--files` | Files to distribute to executors |
| `--jars` | JAR files to include |
| `--packages` | Maven coordinates for packages |

### Master URL Formats

```bash
# Local mode
--master local           # 1 thread
--master local[4]        # 4 threads
--master local[*]        # All cores

# Standalone cluster
--master spark://host:7077

# YARN
--master yarn

# Kubernetes
--master k8s://https://host:port

# Mesos
--master mesos://host:5050
```

### Deployment Modes

#### Client Mode

- Driver runs on the machine where spark-submit is executed
- Good for interactive and debugging
- Results returned to local terminal

```bash
spark-submit --master yarn --deploy-mode client app.py
```

```
+----------------+
|  Your Machine  |  <-- Driver runs here
|   (Client)     |
+----------------+
        |
        v
+----------------+
|    Cluster     |
|   (Executors)  |
+----------------+
```

#### Cluster Mode

- Driver runs on a cluster node
- Better for production
- Application continues if client disconnects

```bash
spark-submit --master yarn --deploy-mode cluster app.py
```

```
+----------------+
|  Your Machine  |  <-- Just submits job
|   (Client)     |
+----------------+
        |
        v
+----------------+
|    Cluster     |
| Driver +       |  <-- Driver runs in cluster
| Executors      |
+----------------+
```

### Common Submission Patterns

#### Basic Python Application

```bash
spark-submit \
    --master local[*] \
    my_app.py
```

#### YARN Cluster Submission

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 4 \
    --num-executors 10 \
    my_app.py
```

#### With Dependencies

```bash
spark-submit \
    --master yarn \
    --py-files dependencies.zip,utils.py \
    --files config.json \
    my_app.py
```

#### With Maven Packages

```bash
spark-submit \
    --master yarn \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    kafka_consumer.py
```

#### With Configuration

```bash
spark-submit \
    --master yarn \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.dynamicAllocation.enabled=true \
    my_app.py
```

### Packaging Python Applications

#### Single File

```bash
spark-submit app.py
```

#### Multiple Files

```bash
# Create zip of modules
zip -r mypackage.zip mypackage/

# Submit with dependencies
spark-submit --py-files mypackage.zip main.py
```

#### Using requirements

```bash
# Create environment with dependencies
pip install -r requirements.txt -t ./deps

# Zip dependencies
cd deps && zip -r ../deps.zip . && cd ..

# Submit
spark-submit --py-files deps.zip main.py
```

### Passing Arguments

```python
# my_app.py
import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--partitions", type=int, default=100)
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("MyApp").getOrCreate()
    df = spark.read.parquet(args.input)
    df.repartition(args.partitions).write.parquet(args.output)
    spark.stop()

if __name__ == "__main__":
    main()
```

Submit with arguments:

```bash
spark-submit my_app.py \
    --input hdfs://data/input \
    --output hdfs://data/output \
    --partitions 200
```

### Environment Variables

```bash
# Set before spark-submit
export PYSPARK_PYTHON=/usr/bin/python3
export SPARK_HOME=/opt/spark

# Or inline
PYSPARK_PYTHON=/usr/bin/python3 spark-submit app.py
```

Common environment variables:

- `PYSPARK_PYTHON`: Python interpreter on workers
- `PYSPARK_DRIVER_PYTHON`: Python interpreter on driver
- `SPARK_HOME`: Spark installation directory
- `HADOOP_CONF_DIR`: Hadoop configuration directory

## Code Example

Here is a complete example application with proper spark-submit usage:

**my_etl_job.py:**

```python
#!/usr/bin/env python3
"""
ETL Job for processing sales data.

Usage:
    spark-submit --master yarn --deploy-mode cluster \
        --driver-memory 2g --executor-memory 4g \
        --num-executors 4 --executor-cores 2 \
        my_etl_job.py \
        --input s3://bucket/sales \
        --output s3://bucket/processed \
        --date 2024-01-15
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name):
    """Create and configure SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Sales ETL Job")
    parser.add_argument("--input", required=True, help="Input data path")
    parser.add_argument("--output", required=True, help="Output data path")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    parser.add_argument("--partitions", type=int, default=100, 
                        help="Number of output partitions")
    return parser.parse_args()

def process_sales(spark, input_path, output_path, date_str, partitions):
    """Main ETL processing logic."""
    
    logger.info(f"Processing sales data for {date_str}")
    logger.info(f"Input: {input_path}")
    logger.info(f"Output: {output_path}")
    
    # For demo, create sample data
    data = [
        ("P001", "Electronics", 999.99, 2, date_str),
        ("P002", "Clothing", 49.99, 5, date_str),
        ("P003", "Food", 12.99, 10, date_str),
    ]
    
    df = spark.createDataFrame(
        data, 
        ["product_id", "category", "price", "quantity", "sale_date"]
    )
    
    # Transformations
    result = df \
        .withColumn("total", F.col("price") * F.col("quantity")) \
        .withColumn("processed_at", F.current_timestamp())
    
    # Display sample
    logger.info("Sample output:")
    result.show()
    
    # Calculate metrics
    total_revenue = result.agg(F.sum("total")).collect()[0][0]
    record_count = result.count()
    
    logger.info(f"Total records: {record_count}")
    logger.info(f"Total revenue: ${total_revenue:.2f}")
    
    return result

def main():
    """Main entry point."""
    args = parse_arguments()
    
    logger.info("=" * 60)
    logger.info("Starting Sales ETL Job")
    logger.info("=" * 60)
    
    spark = None
    try:
        spark = create_spark_session("SalesETL")
        
        # Log Spark configuration
        sc = spark.sparkContext
        logger.info(f"Application ID: {sc.applicationId}")
        logger.info(f"Master: {sc.master}")
        
        # Process data
        result = process_sales(
            spark,
            args.input,
            args.output,
            args.date,
            args.partitions
        )
        
        logger.info("=" * 60)
        logger.info("ETL Job completed successfully")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
```

**Submission scripts:**

```bash
# submit_local.sh - For local testing
#!/bin/bash
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    my_etl_job.py \
    --input ./data/sales \
    --output ./output/processed \
    --date 2024-01-15
```

```bash
# submit_yarn.sh - For YARN cluster
#!/bin/bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 4 \
    --num-executors 10 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=20 \
    my_etl_job.py \
    --input hdfs://cluster/data/sales \
    --output hdfs://cluster/output/processed \
    --date 2024-01-15 \
    --partitions 200
```

## Summary

- spark-submit is the standard tool for deploying Spark applications
- Use --master to specify the cluster manager (local, yarn, k8s, spark://)
- Choose --deploy-mode client for development, cluster for production
- Configure resources with --driver-memory, --executor-memory, --num-executors
- Include dependencies with --py-files, --files, --jars, --packages
- Pass application arguments after the script name
- Use --conf for additional Spark configuration properties
- Environment variables control Python interpreter and paths

## Additional Resources

- [Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html)
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Running on YARN](https://spark.apache.org/docs/latest/running-on-yarn.html)
