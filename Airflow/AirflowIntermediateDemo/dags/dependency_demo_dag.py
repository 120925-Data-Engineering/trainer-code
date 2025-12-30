"""
Task Dependencies Demo DAG
===========================
Demonstrates complex dependency patterns:
- Fan-out (parallel execution)
- Fan-in (aggregation)
- Conditional branching
- Trigger rules

Weekly Epic: Mastering Workflow Orchestration with Apache Airflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time


# ============================================================
# Python Functions for Tasks
# ============================================================

def extract_region(region: str, **context):
    """
    Simulate extracting data from a region.
    Each region runs in parallel.
    """
    print(f"Extracting data from {region}...")
    time.sleep(random.uniform(1, 3))  # Simulate varying extraction times
    
    record_count = random.randint(1000, 5000)
    print(f"  Extracted {record_count} records from {region}")
    
    # Push to XCom for downstream use
    return {"region": region, "records": record_count}


def validate_all_extracts(**context):
    """
    Validate that all regional extracts completed successfully.
    This task demonstrates fan-in: it waits for all upstream tasks.
    """
    ti = context["ti"]
    
    # Pull results from all extract tasks
    us_result = ti.xcom_pull(task_ids="extract_us")
    eu_result = ti.xcom_pull(task_ids="extract_eu")
    apac_result = ti.xcom_pull(task_ids="extract_apac")
    
    total_records = (
        us_result["records"] + 
        eu_result["records"] + 
        apac_result["records"]
    )
    
    print(f"Validating all extracts...")
    print(f"  US: {us_result['records']} records")
    print(f"  EU: {eu_result['records']} records")
    print(f"  APAC: {apac_result['records']} records")
    print(f"  Total: {total_records} records")
    
    return {"total_records": total_records, "status": "validated"}


def choose_load_strategy(**context):
    """
    Branch logic: choose loading strategy based on data size.
    Returns the task_id of the branch to follow.
    """
    ti = context["ti"]
    validation_result = ti.xcom_pull(task_ids="validate_all")
    
    total_records = validation_result["total_records"]
    
    print(f"Choosing load strategy for {total_records} records...")
    
    # Decision logic
    if total_records > 10000:
        print("  -> Taking PARTITIONED load path (large dataset)")
        return "load_partitioned"
    else:
        print("  -> Taking SIMPLE load path (small dataset)")
        return "load_simple"


def load_partitioned(**context):
    """Load using partitioning strategy for large datasets."""
    print("Executing partitioned load...")
    print("  - Creating partitions by date")
    print("  - Loading in parallel batches")
    time.sleep(2)
    print("Partitioned load complete!")
    return {"strategy": "partitioned", "status": "success"}


def load_simple(**context):
    """Load using simple strategy for small datasets."""
    print("Executing simple load...")
    print("  - Direct insert to destination")
    time.sleep(1)
    print("Simple load complete!")
    return {"strategy": "simple", "status": "success"}


def cleanup_temp_files(**context):
    """
    Cleanup task that runs regardless of success or failure.
    Demonstrates trigger_rule=ALL_DONE
    """
    print("Cleaning up temporary files...")
    print("  - Removing staging tables")
    print("  - Deleting temp files")
    print("Cleanup complete!")


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id="dependency_demo",
    description="Demonstrates task dependencies, branching, and trigger rules",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "dependencies", "branching"],
    default_args={
        "owner": "airflow_demo",
        "retries": 1,
    }
) as dag:
    
    # ---------- Start ----------
    start = EmptyOperator(task_id="start")
    
    # ---------- Fan-Out: Parallel Extraction ----------
    # These three tasks run in parallel after 'start' completes
    
    extract_us = PythonOperator(
        task_id="extract_us",
        python_callable=extract_region,
        op_kwargs={"region": "US"}
    )
    
    extract_eu = PythonOperator(
        task_id="extract_eu",
        python_callable=extract_region,
        op_kwargs={"region": "EU"}
    )
    
    extract_apac = PythonOperator(
        task_id="extract_apac",
        python_callable=extract_region,
        op_kwargs={"region": "APAC"}
    )
    
    # ---------- Fan-In: Validation ----------
    # This task waits for ALL extract tasks to complete
    
    validate_all = PythonOperator(
        task_id="validate_all",
        python_callable=validate_all_extracts
    )
    
    # ---------- Branching: Choose Strategy ----------
    # Returns task_id of the branch to follow
    
    branch = BranchPythonOperator(
        task_id="choose_strategy",
        python_callable=choose_load_strategy
    )
    
    # ---------- Branch Paths ----------
    
    load_partitioned_task = PythonOperator(
        task_id="load_partitioned",
        python_callable=load_partitioned
    )
    
    load_simple_task = PythonOperator(
        task_id="load_simple",
        python_callable=load_simple
    )
    
    # ---------- Join After Branch ----------
    # NONE_FAILED_MIN_ONE_SUCCESS: runs if at least one upstream succeeded
    # and none failed (skipped is OK)
    
    join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    # ---------- Cleanup (Always Runs) ----------
    # ALL_DONE: runs regardless of upstream success or failure
    
    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup_temp_files,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # ---------- End ----------
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    # ============================================================
    # Dependencies
    # ============================================================
    
    # Fan-out: start triggers all extracts in parallel
    start >> [extract_us, extract_eu, extract_apac]
    
    # Fan-in: validation waits for all extracts
    [extract_us, extract_eu, extract_apac] >> validate_all
    
    # Branching
    validate_all >> branch >> [load_partitioned_task, load_simple_task]
    
    # Join after branch
    [load_partitioned_task, load_simple_task] >> join
    
    # Cleanup and end
    join >> cleanup >> end


# Documentation
dag.doc_md = """
## Dependency Demo DAG

This DAG demonstrates advanced dependency patterns in Airflow.

### Patterns Shown:
1. **Fan-Out**: `start` triggers 3 parallel extract tasks
2. **Fan-In**: `validate_all` waits for all extracts
3. **Branching**: `choose_strategy` picks one of two load paths
4. **Trigger Rules**: `cleanup` runs regardless of success/failure

### Trigger and Observe:
1. Trigger the DAG manually
2. Watch the Graph view to see parallel execution
3. Note which branch is taken and which is skipped
"""
