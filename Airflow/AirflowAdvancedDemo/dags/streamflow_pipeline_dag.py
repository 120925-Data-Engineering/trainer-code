"""
StreamFlow Pipeline DAG
=======================
Complete Kafka-to-Spark orchestration pipeline demonstrating production patterns:
- Batch Kafka consumer triggered by Airflow
- FileSensor waiting for landing zone data
- Spark ETL job submission
- Output validation
- Success notifications and cleanup

This DAG implements the StreamFlow architecture pattern for bridging
streaming data (Kafka) with batch processing (Spark).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os


# ============================================================
# Configuration
# ============================================================

# Paths within the Docker environment
SCRIPTS_PATH = "/opt/airflow/scripts"          # Mounted from code/scripts
SPARK_JOBS_PATH = "/opt/spark-jobs"            # Mounted from code/jobs
LANDING_ZONE = "/opt/spark-data/landing"
GOLD_ZONE = "/opt/spark-data/gold"

# Kafka configuration
KAFKA_TOPICS = "user_events,transaction_events"
CONSUME_DURATION = 30  # seconds to consume from Kafka

# Spark configuration
SPARK_MASTER = "spark://spark-master:7077"


# ============================================================
# Callbacks
# ============================================================

def alert_on_failure(context):
    """Send alert on pipeline failure."""
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    
    print("=" * 60)
    print("ALERT: StreamFlow Pipeline Failure!")
    print("=" * 60)
    print(f"DAG: {task_instance.dag_id}")
    print(f"Task: {task_instance.task_id}")
    print(f"Execution Date: {context.get('ds')}")
    print(f"Error: {exception}")
    print("=" * 60)
    
    # In production: integrate with Slack, PagerDuty, email, etc.
    # Example: send_slack_alert(task_instance.task_id, str(exception))


# ============================================================
# Task Functions
# ============================================================

def validate_spark_output(**context):
    """
    Verify that the Spark job produced the expected output.
    Raises an exception if validation fails.
    """
    ds = context["ds"]
    output_path = f"{GOLD_ZONE}/{ds}"
    
    print(f"Validating output at: {output_path}")
    
    # Check if output directory exists
    if not os.path.exists(output_path):
        raise ValueError(f"Output directory missing: {output_path}")
    
    # Check for Parquet files
    files = os.listdir(output_path)
    parquet_files = [f for f in files if f.endswith(".parquet")]
    
    if not parquet_files:
        # Also check for _SUCCESS file (Spark completion marker)
        if "_SUCCESS" not in files:
            raise ValueError(f"No parquet files or _SUCCESS marker in {output_path}")
        print("Found _SUCCESS marker but no .parquet files - checking subdirectories...")
    
    # Count total parquet files (including partitioned output)
    total_parquet = 0
    for root, dirs, filenames in os.walk(output_path):
        for filename in filenames:
            if filename.endswith(".parquet"):
                total_parquet += 1
    
    if total_parquet == 0:
        raise ValueError(f"No parquet files found in {output_path} or subdirectories")
    
    print(f"Validation PASSED: Found {total_parquet} parquet file(s)")
    
    return {
        "output_path": output_path,
        "parquet_files": total_parquet,
        "status": "validated"
    }


# ============================================================
# DAG Definition
# ============================================================

default_args = {
    "owner": "data_platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="streamflow_pipeline",
    description="Kafka-Spark orchestration pipeline with sensors and validation",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger for demo
    catchup=False,
    tags=["demo", "streamflow", "kafka", "spark"],
) as dag:
    
    # --------------------------------------------------------
    # Task 1: Trigger Kafka Batch Consumer
    # --------------------------------------------------------
    # Consumes from Kafka topics and writes JSON to landing zone
    trigger_kafka_consumer = BashOperator(
        task_id="trigger_kafka_consumer",
        bash_command=f"""
            echo "Starting Kafka batch consumer..."
            python {SCRIPTS_PATH}/kafka_batch_consumer.py \\
                --topics {KAFKA_TOPICS} \\
                --duration {CONSUME_DURATION} \\
                --output {LANDING_ZONE}/{{{{ ds }}}} \\
                --bootstrap-servers kafka:9092
        """,
    )
    
    # --------------------------------------------------------
    # Task 2: Wait for Landing Zone Files
    # --------------------------------------------------------
    # Sensor waits until consumer has written the user_events file
    wait_for_landing = FileSensor(
        task_id="wait_for_landing_zone",
        filepath=f"{LANDING_ZONE}/{{{{ ds }}}}/user_events.json",
        poke_interval=10,        # Check every 10 seconds
        timeout=300000,             # 5 minute timeout
        mode="reschedule",       # Free worker slot while waiting
        soft_fail=False,         # Fail task if timeout
    )
    
    # --------------------------------------------------------
    # Task 3: Run Spark ETL Job
    # --------------------------------------------------------
    # Submits the ETL job to the Spark cluster
    spark_etl_job = BashOperator(
        task_id="spark_etl_job",
        bash_command=f"""
            echo "Submitting Spark ETL job..."
            spark-submit \\
                --master {SPARK_MASTER} \\
                --deploy-mode client \\
                --name "StreamFlowETL_{{{{ ds }}}}" \\
                {SPARK_JOBS_PATH}/streamflow_etl_job.py \\
                --input {LANDING_ZONE}/{{{{ ds }}}} \\
                --output {GOLD_ZONE}/{{{{ ds }}}}
        """,
        retries=2,
        retry_delay=timedelta(minutes=3),
    )
    
    # --------------------------------------------------------
    # Task 4: Validate Output
    # --------------------------------------------------------
    validate_output = PythonOperator(
        task_id="validate_output",
        python_callable=validate_spark_output,
    )
    
    # --------------------------------------------------------
    # Task 5: Success Notification
    # --------------------------------------------------------
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="""
            echo "========================================"
            echo "StreamFlow Pipeline COMPLETED!"
            echo "========================================"
            echo "Execution Date: {{ ds }}"
            echo "Timestamp: $(date)"
            echo "========================================"
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # --------------------------------------------------------
    # Task 6: Cleanup Landing Zone
    # --------------------------------------------------------
    # Always runs to clean up intermediate files
    cleanup_landing = BashOperator(
        task_id="cleanup_landing",
        bash_command=f"""
            echo "Cleaning up landing zone..."
            rm -rf {LANDING_ZONE}/{{{{ ds }}}}
            echo "Cleanup complete for {{{{ ds }}}}"
        """,
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream failed
    )
    
    # --------------------------------------------------------
    # Task 7: Pipeline End
    # --------------------------------------------------------
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    
    # --------------------------------------------------------
    # Dependencies
    # --------------------------------------------------------
    # Main pipeline flow
    trigger_kafka_consumer >> wait_for_landing >> spark_etl_job >> validate_output
    
    # Notification branch (only on success)
    validate_output >> notify_success
    
    # Cleanup always runs after core pipeline
    [validate_output, notify_success] >> cleanup_landing >> end


# ============================================================
# DAG Documentation
# ============================================================

dag.doc_md = """
## StreamFlow Pipeline

A complete Kafka-to-Spark orchestration pipeline demonstrating production patterns.

### Architecture

```
+--------------------+     +------------------+     +----------------+
| trigger_kafka_     |     | wait_for_        |     | spark_etl_job  |
| consumer           | --> | landing_zone     | --> |                |
| (BashOperator)     |     | (FileSensor)     |     | (spark-submit) |
+--------------------+     +------------------+     +----------------+
                                                           |
                                                           v
                           +-------------------------------------------+
                           | validate_output --> notify_success        |
                           |                           |               |
                           |                           v               |
                           |                    cleanup_landing --> end|
                           +-------------------------------------------+
```

### Pipeline Stages

1. **trigger_kafka_consumer** - Runs batch consumer script to pull from Kafka topics
2. **wait_for_landing_zone** - FileSensor waits for `user_events.json`
3. **spark_etl_job** - Submits PySpark job to process and transform data
4. **validate_output** - Confirms Parquet files were created
5. **notify_success** - Logs completion (integrate with Slack/email in production)
6. **cleanup_landing** - Removes intermediate files (runs even on failure)

### Demo Instructions

**Prerequisites:**
1. Start the pipeline stack: `docker-compose up -d`
2. Wait for all services to be healthy

**Demo Steps:**
1. Run the producer to populate Kafka (from local or container):
   ```bash
   # Local
   python scripts/kafka_data_producer.py --bootstrap-servers localhost:9094 --duration 30
   
   # Container
   docker exec pipeline-airflow-scheduler python /opt/airflow/dags/../scripts/kafka_data_producer.py --duration 30
   ```

2. Trigger this DAG from the Airflow UI

3. Watch the pipeline:
   - Consumer pulls from Kafka
   - Sensor waits for file
   - Spark processes data
   - Validation confirms output

4. Check results in `/opt/spark-data/gold/<date>/`

### Error Handling

- **Retries**: Kafka consumer and Spark job retry 2x with 2-3 minute delays
- **Failure Callback**: `alert_on_failure` logs details (extend for Slack/PagerDuty)
- **Cleanup**: Always runs with `ALL_DONE` trigger rule
- **Timeouts**: Sensor times out after 5 minutes if no data

### Key Patterns

| Pattern | Implementation |
|---------|----------------|
| File Buffer | Kafka consumer writes JSON; Spark reads as batch |
| Sensor Wait | `mode='reschedule'` frees worker while waiting |
| Date Partitioning | `{{ ds }}` template creates daily directories |
| Guaranteed Cleanup | `ALL_DONE` trigger rule ensures cleanup runs |
"""
