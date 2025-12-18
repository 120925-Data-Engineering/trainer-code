# Running Spark Job on EMR

## Learning Objectives
- Submit Spark jobs to EMR clusters
- Use EMR steps for job execution
- Integrate S3 for input and output data
- Monitor job execution and troubleshoot issues

## Why This Matters
Once your EMR cluster is running, you need to know how to submit and monitor Spark jobs effectively. Understanding EMR steps, S3 integration, and monitoring tools enables you to run production data pipelines reliably in the cloud.

## The Concept

### Job Submission Methods

There are several ways to run Spark jobs on EMR:

| Method | Best For | Automation |
|--------|----------|------------|
| EMR Steps | Batch processing | High |
| SSH + spark-submit | Development/debugging | Low |
| EMR Notebooks | Interactive exploration | Low |
| Step Functions | Complex workflows | High |

### EMR Steps

Steps are the primary way to submit jobs to EMR. Each step is a unit of work that the cluster processes.

```
Cluster
   |
   +-- Step 1: Data Ingestion (Spark)
   |
   +-- Step 2: Transformation (Spark)
   |
   +-- Step 3: Export (Custom JAR)
```

#### Adding a Step via CLI

```bash
aws emr add-steps \
    --cluster-id j-XXXXXXXXXXXXX \
    --steps Type=Spark,Name="MySparkJob",\
ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,\
--master,yarn,\
s3://my-bucket/scripts/my_job.py,\
--input,s3://my-bucket/data/input,\
--output,s3://my-bucket/data/output]
```

#### Step Configuration Options

```json
{
    "Type": "Spark",
    "Name": "Process Sales Data",
    "ActionOnFailure": "CONTINUE",
    "Args": [
        "--deploy-mode", "cluster",
        "--master", "yarn",
        "--conf", "spark.executor.memory=4g",
        "--conf", "spark.executor.cores=2",
        "--conf", "spark.sql.shuffle.partitions=200",
        "s3://bucket/scripts/process_sales.py",
        "--date", "2024-01-15"
    ]
}
```

**ActionOnFailure options:**
- `CONTINUE`: Process next step
- `CANCEL_AND_WAIT`: Stop subsequent steps, keep cluster
- `TERMINATE_CLUSTER`: Terminate on failure

### S3 Integration

EMR integrates seamlessly with S3 for data storage:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("S3Job").getOrCreate()

# Read from S3
df = spark.read.parquet("s3://my-bucket/data/input/")

# Process
result = df.filter(df.status == "active")

# Write to S3
result.write.mode("overwrite").parquet("s3://my-bucket/data/output/")
```

**S3 URI formats:**
- `s3://bucket/path` - Standard (recommended)
- `s3a://bucket/path` - Hadoop 2.x+ connector
- `s3n://bucket/path` - Legacy (deprecated)

### Creating a Complete EMR Job

**Step 1: Upload script to S3**
```bash
aws s3 cp my_job.py s3://my-bucket/scripts/
```

**Step 2: Submit as step**
```bash
aws emr add-steps \
    --cluster-id j-XXXXXXXXXXXXX \
    --steps '[
        {
            "Type": "Spark",
            "Name": "Daily ETL",
            "ActionOnFailure": "CONTINUE",
            "Args": [
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--driver-memory", "4g",
                "--executor-memory", "8g",
                "--executor-cores", "4",
                "--num-executors", "10",
                "s3://my-bucket/scripts/my_job.py",
                "--input", "s3://my-bucket/raw/",
                "--output", "s3://my-bucket/processed/"
            ]
        }
    ]'
```

**Step 3: Monitor progress**
```bash
aws emr describe-step \
    --cluster-id j-XXXXXXXXXXXXX \
    --step-id s-XXXXXXXXXXXXX
```

### Monitoring Jobs

#### Via AWS CLI
```bash
# List all steps
aws emr list-steps --cluster-id j-XXXXXXXXXXXXX

# Get step details
aws emr describe-step --cluster-id j-XXXXXXXXXXXXX --step-id s-XXXXX
```

#### Via Spark UI
Access through SSH tunnel:
```bash
# Create tunnel
ssh -i key.pem -ND 8157 hadoop@primary-node-dns

# Browser with SOCKS proxy
# Spark History: http://primary-node:18080
```

#### Via CloudWatch
EMR publishes metrics to CloudWatch:
- `AppsRunning`: Number of running applications
- `AppsPending`: Queued applications
- `YARNMemoryAvailablePercentage`: Available memory
- `ContainerAllocated`: Active containers

### Transient Clusters

For batch jobs, use transient clusters that terminate after steps complete:

```bash
aws emr create-cluster \
    --name "Transient Spark Job" \
    --release-label emr-7.0.0 \
    --applications Name=Spark \
    --instance-groups \
        InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
        InstanceGroupType=CORE,InstanceCount=4,InstanceType=r5.xlarge \
    --use-default-roles \
    --auto-terminate \
    --steps Type=Spark,Name="MyJob",ActionOnFailure=TERMINATE_CLUSTER,\
Args=[--deploy-mode,cluster,s3://bucket/scripts/job.py]
```

### Troubleshooting

#### Log Locations
```
s3://my-bucket/emr-logs/j-XXXXX/
    |-- node/
    |   |-- i-xxxxx/
    |       |-- applications/
    |       |   |-- spark/
    |       |       |-- spark-xxx.log
    |       |-- daemons/
    |-- steps/
        |-- s-xxxxx/
            |-- stdout.gz
            |-- stderr.gz
            |-- controller.gz
```

#### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Step fails immediately | Script not found | Check S3 path |
| Out of memory | Insufficient executor memory | Increase executor memory |
| Job stuck | Resource contention | Check YARN queue |
| S3 access denied | IAM permissions | Update instance profile |

## Code Example

**Complete ETL job for EMR:**

```python
#!/usr/bin/env python3
"""
EMR Spark ETL Job
Upload to S3 and submit as EMR step.
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create SparkSession configured for EMR."""
    return SparkSession.builder \
        .appName("EMR-ETL-Job") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def process_data(spark, input_path, output_path):
    """Main ETL logic."""
    
    logger.info(f"Reading from: {input_path}")
    
    # Define schema
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_date", StringType(), True)
    ])
    
    # Read input data
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(input_path)
    
    logger.info(f"Input records: {df.count()}")
    
    # Transformations
    result = df \
        .withColumn("transaction_date", F.to_date("transaction_date")) \
        .withColumn("year", F.year("transaction_date")) \
        .withColumn("month", F.month("transaction_date")) \
        .filter(F.col("amount") > 0) \
        .dropDuplicates(["transaction_id"])
    
    # Add processing metadata
    result = result \
        .withColumn("processed_at", F.current_timestamp()) \
        .withColumn("etl_version", F.lit("1.0"))
    
    logger.info(f"Output records: {result.count()}")
    
    # Write output partitioned by year/month
    logger.info(f"Writing to: {output_path}")
    
    result.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)
    
    logger.info("Write complete")
    
    return result.count()

def generate_metrics(spark, output_path):
    """Generate summary metrics."""
    
    df = spark.read.parquet(output_path)
    
    metrics = {
        "total_records": df.count(),
        "total_amount": df.agg(F.sum("amount")).collect()[0][0],
        "unique_customers": df.select("customer_id").distinct().count(),
        "date_range": {
            "min": df.agg(F.min("transaction_date")).collect()[0][0],
            "max": df.agg(F.max("transaction_date")).collect()[0][0]
        }
    }
    
    logger.info("=== Job Metrics ===")
    for key, value in metrics.items():
        logger.info(f"  {key}: {value}")
    
    return metrics

def main():
    parser = argparse.ArgumentParser(description="EMR ETL Job")
    parser.add_argument("--input", required=True, help="S3 input path")
    parser.add_argument("--output", required=True, help="S3 output path")
    args = parser.parse_args()
    
    spark = None
    try:
        spark = create_spark_session()
        
        logger.info("=" * 60)
        logger.info("Starting EMR ETL Job")
        logger.info(f"Input: {args.input}")
        logger.info(f"Output: {args.output}")
        logger.info("=" * 60)
        
        record_count = process_data(spark, args.input, args.output)
        metrics = generate_metrics(spark, args.output)
        
        logger.info("=" * 60)
        logger.info(f"Job completed successfully. Processed {record_count} records.")
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

**Submission script:**

```bash
#!/bin/bash
# submit_emr_job.sh

CLUSTER_ID="j-XXXXXXXXXXXXX"
SCRIPT_PATH="s3://my-bucket/scripts/emr_etl_job.py"
INPUT_PATH="s3://my-bucket/data/raw/transactions/"
OUTPUT_PATH="s3://my-bucket/data/processed/transactions/"

# Upload script
aws s3 cp emr_etl_job.py $SCRIPT_PATH

# Submit step
STEP_ID=$(aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps "[
        {
            \"Type\": \"Spark\",
            \"Name\": \"Daily Transaction ETL\",
            \"ActionOnFailure\": \"CONTINUE\",
            \"Args\": [
                \"--deploy-mode\", \"cluster\",
                \"--master\", \"yarn\",
                \"--driver-memory\", \"4g\",
                \"--executor-memory\", \"8g\",
                \"--executor-cores\", \"4\",
                \"--num-executors\", \"10\",
                \"--conf\", \"spark.sql.shuffle.partitions=200\",
                \"$SCRIPT_PATH\",
                \"--input\", \"$INPUT_PATH\",
                \"--output\", \"$OUTPUT_PATH\"
            ]
        }
    ]" \
    --query 'StepIds[0]' \
    --output text)

echo "Submitted step: $STEP_ID"

# Monitor step status
while true; do
    STATUS=$(aws emr describe-step \
        --cluster-id $CLUSTER_ID \
        --step-id $STEP_ID \
        --query 'Step.Status.State' \
        --output text)
    
    echo "Step status: $STATUS"
    
    case $STATUS in
        COMPLETED)
            echo "Job completed successfully!"
            exit 0
            ;;
        FAILED|CANCELLED)
            echo "Job failed or was cancelled"
            exit 1
            ;;
        *)
            sleep 30
            ;;
    esac
done
```

## Summary
- Use EMR Steps for submitting Spark jobs to clusters
- Jobs read/write data from S3 using standard Spark APIs
- Configure step parameters for resources and Spark settings
- Set ActionOnFailure to control cluster behavior on errors
- Use transient clusters with --auto-terminate for batch jobs
- Monitor via CLI, Spark UI (SSH tunnel), or CloudWatch
- Check S3 logs for detailed debugging information

## Additional Resources
- [Adding Steps to EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-work-with-steps.html)
- [EMR with S3](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-file-systems.html)
- [View Log Files](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-debugging.html)
