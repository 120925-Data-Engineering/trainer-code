"""
Sample PySpark ETL Job

This job demonstrates a simple ETL pattern:
1. Read JSON from landing zone
2. Transform data (add processing timestamp)
3. Write CSV to gold zone

Usage:
    spark-submit sample_etl_job.py [input_path] [output_path]
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import sys


def create_spark_session(app_name="SampleETL"):
    """Create and configure SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()


def extract(spark, input_path):
    """Read JSON data from landing zone."""
    print(f"Reading from: {input_path}")
    df = spark.read.json(input_path)
    print(f"Extracted {df.count()} records")
    return df


def transform(df):
    """Apply transformations to the data."""
    # Add processing metadata
    df_transformed = df \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("pipeline", lit("airflow_spark_demo"))
    
    print("Transformation complete")
    return df_transformed


def load(df, output_path):
    """Write transformed data to gold zone as CSV."""
    print(f"Writing to: {output_path}")
    df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"Loaded {df.count()} records to gold zone")


def main(input_path, output_path):
    """Main ETL pipeline."""
    spark = create_spark_session()
    
    try:
        # ETL Pipeline
        df_raw = extract(spark, input_path)
        df_transformed = transform(df_raw)
        load(df_transformed, output_path)
        
        print("ETL job completed successfully!")
        
    except Exception as e:
        print(f"ETL job failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    # Parse command line arguments
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark-data/landing"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/spark-data/gold"
    
    main(input_path, output_path)
