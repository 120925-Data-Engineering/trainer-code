"""
Failing DAG for Debugging Practice
===================================
This DAG intentionally fails to give trainees
practice with debugging and troubleshooting.

Errors included:
1. Missing module import
2. Database connection failure
3. Data validation error
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


# ============================================================
# Task Functions with Intentional Errors
# ============================================================

def task_with_import_error(**context):
    """
    This task fails due to a missing module.
    Demonstrates: ModuleNotFoundError
    """
    print("Attempting to import a module that doesn't exist...")
    
    # This will fail with ModuleNotFoundError
    import nonexistent_module
    
    return {"status": "success"}


def task_with_connection_error(**context):
    """
    This task fails due to connection issues.
    Demonstrates: ConnectionRefusedError
    """
    print("Attempting to connect to a database...")
    
    import socket
    
    # Try to connect to a non-existent service
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    
    # This will fail with connection refused
    sock.connect(("localhost", 99999))
    
    return {"status": "success"}


def task_with_validation_error(**context):
    """
    This task fails due to data validation.
    Demonstrates: ValueError
    """
    print("Processing data...")
    
    data = {"records": []}
    
    print(f"Found {len(data['records'])} records")
    
    # Validation check
    if len(data["records"]) == 0:
        raise ValueError(
            "Data validation failed: No records found in input. "
            "Expected at least 1 record. "
            "Check if the source system is available and data was extracted."
        )
    
    return {"status": "success"}


def task_with_type_error(**context):
    """
    This task fails due to a type error.
    Demonstrates: TypeError
    """
    print("Performing calculation...")
    
    value = "not_a_number"
    
    # This will fail with TypeError
    result = value + 10
    
    return {"result": result}


def working_task(**context):
    """
    This task works correctly.
    Included to show contrast with failing tasks.
    """
    print("This task works correctly!")
    return {"status": "success"}


# ============================================================
# DAG Definition
# ============================================================

default_args = {
    "owner": "training",
    "retries": 0,  # No retries so we see failures immediately
    "email_on_failure": False
}

with DAG(
    dag_id="failing_dag",
    description="Intentionally failing DAG for debugging practice",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "debugging", "training"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # This task will succeed
    good_task = PythonOperator(
        task_id="working_task",
        python_callable=working_task
    )
    
    # These tasks will fail with different errors
    # Trainees can uncomment one at a time to practice debugging
    
    fail_import = PythonOperator(
        task_id="fail_import_error",
        python_callable=task_with_import_error
    )
    
    fail_validation = PythonOperator(
        task_id="fail_validation_error",
        python_callable=task_with_validation_error
    )
    
    fail_type = PythonOperator(
        task_id="fail_type_error",
        python_callable=task_with_type_error
    )
    
    end = EmptyOperator(task_id="end")
    
    # Fan-out to show multiple failure types
    start >> good_task >> [fail_import, fail_validation, fail_type] >> end


dag.doc_md = """
## Failing DAG for Practice

This DAG intentionally fails to give you practice with debugging.

### Failure Types:
1. **fail_import_error** - Missing Python module
2. **fail_validation_error** - Data validation failure
3. **fail_type_error** - Type mismatch in calculation

### Debugging Exercise:
1. Trigger this DAG
2. Wait for tasks to fail
3. Click on a failed task
4. Read the logs to identify the error
5. Determine what you would do to fix it

### Learning Goals:
- Navigate to task logs quickly
- Read Python tracebacks
- Identify error types and root causes
"""
