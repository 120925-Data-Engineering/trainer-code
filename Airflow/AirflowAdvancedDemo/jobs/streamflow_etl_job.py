"""
StreamFlow ETL Job
==================
PySpark job that processes user_events and transaction_events from the landing zone,
joins them by user_id, and writes enriched data to the gold zone.

This job is triggered by the StreamFlow DAG via spark-submit.

Usage:
    spark-submit --master spark://spark-master:7077 \
        streamflow_etl_job.py \
        --input /opt/spark-data/landing/2024-01-15 \
        --output /opt/spark-data/gold/2024-01-15
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, count, sum as spark_sum,
    to_timestamp, when
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def create_spark_session(app_name="StreamFlowETL"):
    """Create and configure SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()


def read_json_lines(spark, file_path, schema=None):
    """
    Read JSON Lines file (one JSON object per line).
    This is the format produced by kafka_batch_consumer.py.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None
    
    print(f"Reading: {file_path}")
    
    if schema:
        df = spark.read.schema(schema).json(file_path)
    else:
        df = spark.read.json(file_path)
    
    record_count = df.count()
    print(f"  Loaded {record_count} records")
    
    return df


def extract(spark, input_path):
    """Extract user_events and transaction_events from landing zone."""
    print("\n" + "=" * 50)
    print("EXTRACT PHASE")
    print("=" * 50)
    
    user_events_path = os.path.join(input_path, "user_events.json")
    transaction_events_path = os.path.join(input_path, "transaction_events.json")
    
    user_events = read_json_lines(spark, user_events_path)
    transaction_events = read_json_lines(spark, transaction_events_path)
    
    return user_events, transaction_events


def transform(user_events, transaction_events):
    """
    Transform and join the data.
    Creates user activity summary with transaction aggregates.
    """
    print("\n" + "=" * 50)
    print("TRANSFORM PHASE")
    print("=" * 50)
    
    # User activity aggregation
    if user_events is not None:
        print("Aggregating user activity...")
        user_activity = user_events.groupBy("user_id").agg(
            count("*").alias("event_count"),
            count(when(col("action") == "checkout", 1)).alias("checkout_count"),
            count(when(col("action") == "view_product", 1)).alias("view_count")
        )
        print(f"  User activity records: {user_activity.count()}")
    else:
        user_activity = None
    
    # Transaction aggregation
    if transaction_events is not None:
        print("Aggregating transactions...")
        transaction_summary = transaction_events.groupBy("user_id").agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            count(when(col("status") == "completed", 1)).alias("completed_count")
        )
        print(f"  Transaction summary records: {transaction_summary.count()}")
    else:
        transaction_summary = None
    
    # Join user activity with transactions
    print("Joining datasets...")
    
    if user_activity is not None and transaction_summary is not None:
        # Full outer join to capture all users and transactions
        enriched = user_activity.join(
            transaction_summary,
            on="user_id",
            how="full_outer"
        )
    elif user_activity is not None:
        enriched = user_activity
    elif transaction_summary is not None:
        enriched = transaction_summary
    else:
        print("WARNING: No data to transform!")
        return None
    
    # Add processing metadata
    enriched = enriched \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("pipeline", lit("streamflow"))
    
    # Fill nulls with 0 for numeric aggregates
    for col_name in ["event_count", "checkout_count", "view_count", 
                     "transaction_count", "total_amount", "completed_count"]:
        if col_name in enriched.columns:
            enriched = enriched.fillna({col_name: 0})
    
    print(f"Enriched dataset: {enriched.count()} records")
    enriched.printSchema()
    
    return enriched


def load(df, output_path):
    """Write transformed data to gold zone as Parquet."""
    print("\n" + "=" * 50)
    print("LOAD PHASE")
    print("=" * 50)
    
    if df is None:
        print("No data to write!")
        return False
    
    print(f"Writing to: {output_path}")
    
    # Clean up output directory if it exists (for reruns)
    # In production, you might use append mode or partitioning instead
    
    df.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"Successfully wrote {df.count()} records to gold zone")
    
    # Show sample of output
    print("\nSample output:")
    df.show(5, truncate=False)
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="StreamFlow ETL - Process Kafka events to analytics"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input path (landing zone directory)"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path (gold zone directory)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("StreamFlow ETL Job")
    print("=" * 60)
    print(f"Input Path: {args.input}")
    print(f"Output Path: {args.output}")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # ETL Pipeline
        user_events, transaction_events = extract(spark, args.input)
        
        if user_events is None and transaction_events is None:
            print("\nERROR: No input data found!")
            print("Ensure the landing zone contains user_events.json and/or transaction_events.json")
            return 1
        
        enriched = transform(user_events, transaction_events)
        success = load(enriched, args.output)
        
        if success:
            print("\n" + "=" * 60)
            print("ETL JOB COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            return 0
        else:
            print("\nETL job failed during load phase")
            return 1
        
    except Exception as e:
        print(f"\nETL job failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    exit(main())
