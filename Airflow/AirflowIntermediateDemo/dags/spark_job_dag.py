"""
Spark Job DAG - Demonstrates triggering Spark via spark-submit

This DAG shows how to:
1. Use BashOperator to call spark-submit
2. Pass Airflow's execution date for date-partitioned output
3. Handle errors and retries
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def validate_output(**context):
    """Validate that Spark job produced expected CSV output."""
    import os
    
    # Get the execution date from Airflow context
    run_date = context["ds"]  # Format: YYYY-MM-DD
    output_path = f"/opt/spark-data/gold/date={run_date}"
    
    if os.path.exists(output_path):
        csv_files = [f for f in os.listdir(output_path) if f.endswith('.csv')]
        if csv_files:
            print(f"Output validated: {output_path} contains {len(csv_files)} CSV files")
            return True
    
    raise ValueError(f"Output validation failed: {output_path} has no CSV files")


with DAG(
    dag_id="spark_etl_pipeline",
    description="Triggers a PySpark ETL job on the Spark cluster",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    default_args=default_args,
    tags=["spark", "etl", "demo"],
) as dag:
    
    # Task 1: Run Spark ETL Job
    # Airflow container has Spark client installed (via Dockerfile.airflow)
    # This allows direct spark-submit to the Spark Master
    # 
    # Key: {{ ds }} passes Airflow's execution date (YYYY-MM-DD) to the job
    # This enables date-partitioned output: /gold/date=2025-12-30/
    run_spark_job = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
            spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --name "AirflowTriggeredETL" \
                /opt/spark-jobs/sample_etl_job.py \
                /opt/spark-data/landing \
                /opt/spark-data/gold \
                {{ ds }}
        """,
    )
    
    # Task 2: Validate the output (uses {{ ds }} via context)
    validate = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )
    
    # Task 3: Notify success
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "Spark ETL completed for {{ ds }} at $(date)"',
    )
    
    # Dependencies
    run_spark_job >> validate >> notify_success
