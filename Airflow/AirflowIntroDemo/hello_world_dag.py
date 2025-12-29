"""
Hello World DAG
================
A simple introductory DAG demonstrating basic Airflow concepts.

This DAG shows:
- DAG definition with context manager
- BashOperator for shell commands
- PythonOperator for Python functions
- Task dependencies with >> operator
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


# Python functions for PythonOperator
def greet():
    """
    A simple greeting function.
    The return value is automatically pushed to XCom.
    """
    print("Hello from Airflow!")
    print("This message appears in the task logs.")
    return "greeting_complete"


def process_data():
    """
    Simulates data processing.
    In real DAGs, this would contain actual business logic.
    """
    import time
    
    print("Processing data...")
    
    # Simulate some work
    for i in range(3):
        print(f"  Processing batch {i + 1} of 3...")
        time.sleep(1)
    
    print("Processing complete!")
    return {"batches_processed": 3, "status": "success"}


# DAG Definition
with DAG(
    dag_id="hello_world",
    description="A simple Hello World DAG for learning Airflow basics",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only (no automatic scheduling)
    catchup=False,  # Don't run for past dates
    tags=["demo", "beginner"],
    default_args={
        "owner": "airflow_demo",
        "retries": 1,
    }
) as dag:
    
    # Task 1: Start marker (BashOperator)
    start = BashOperator(
        task_id="start",
        bash_command="echo 'Pipeline starting at $(date)'"
    )
    
    # Task 2: Greeting (PythonOperator)
    greet_task = PythonOperator(
        task_id="greet",
        python_callable=greet
    )
    
    # Task 3: Processing (PythonOperator)
    process_task = PythonOperator(
        task_id="process",
        python_callable=process_data
    )
    
    # Task 4: End marker (BashOperator)
    end = BashOperator(
        task_id="end",
        bash_command="echo 'Pipeline completed successfully at $(date)'"
    )
    
    # Define dependencies (execution order)
    # Read as: start THEN greet THEN process THEN end
    start >> greet_task >> process_task >> end


# This docstring helps with DAG documentation in the UI
dag.doc_md = """
## Hello World DAG

This is a demonstration DAG that shows the basic structure of an Airflow pipeline.

### Tasks:
1. **start** - Prints the start time
2. **greet** - Prints a greeting message
3. **process** - Simulates data processing
4. **end** - Prints the completion time

### Usage:
Trigger this DAG manually from the Airflow UI to see it in action.
"""
