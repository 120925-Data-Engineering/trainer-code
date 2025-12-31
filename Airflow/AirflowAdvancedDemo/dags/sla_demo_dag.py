"""
SLA Demo DAG
============
Demonstrates SLA configuration and miss handling.

IMPORTANT: Airflow SLAs are measured from the DAG's EXECUTION DATE (scheduled time),
NOT from when the task actually starts. This means:
- SLA = time from execution_date to when task should COMPLETE
- For a task in a chain, SLA must account for ALL upstream tasks
- SLAs are checked by the scheduler periodically (not instantly)

Features:
- Task-level SLA definition
- DAG-level SLA miss callback
- Tasks designed to exceed SLA
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import time
import random


# ============================================================
# SLA Miss Callback
# ============================================================

def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Called when any task in this DAG misses its SLA.
    
    Args:
        dag: The DAG object
        task_list: Tasks that missed SLA
        blocking_task_list: Tasks blocking the missed tasks
        slas: SLA objects
        blocking_tis: Blocking task instances
    """
    print("=" * 60)
    print("SLA MISS ALERT!")
    print(f"DAG: {dag.dag_id}")
    print(f"Tasks that missed SLA: {[t.task_id for t in task_list]}")
    print(f"SLAs: {slas}")
    print("=" * 60)
    
    # In production, this would:
    # - Send Slack notification
    # - Send email
    # - Create PagerDuty incident
    # - Log to monitoring system


# ============================================================
# Task Functions
# ============================================================

def fast_task(**context):
    """A task that completes quickly."""
    print("Fast task executing...")
    time.sleep(2)
    print("Fast task complete!")
    return {"status": "success", "duration": 2}


def variable_task(**context):
    """
    A task that takes longer than its SLA allows.
    Since this task has a 10s SLA but runs after fast_task (2s),
    and we sleep for 15-25s, it will ALWAYS exceed the cumulative SLA.
    """
    print("Variable task starting...")
    
    # Always take 15-25 seconds - guaranteed to miss 10s SLA
    sleep_time = random.randint(15, 25)
    print(f"This run will take {sleep_time} seconds...")
    print(f"SLA is 10 seconds from execution_date, so this WILL miss!")
    
    time.sleep(sleep_time)
    
    print(f"Variable task complete after {sleep_time} seconds")
    return {"status": "success", "duration": sleep_time}


def critical_task(**context):
    """
    A critical task with a strict SLA.
    """
    print("Critical task executing...")
    print("This task has a 30-second SLA")
    
    # Usually fast, occasionally slow
    if random.random() < 0.3:  # 30% chance of being slow
        print("Encountered slow path!")
        time.sleep(45)  # Will miss the 30-second SLA
    else:
        time.sleep(5)
    
    print("Critical task complete!")
    return {"status": "success"}


# ============================================================
# DAG Definition
# ============================================================

default_args = {
    "owner": "data_team",
    "email": ["alerts@example.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}

with DAG(
    dag_id="sla_demo",
    description="Demonstrates SLA configuration and monitoring",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    # IMPORTANT: SLAs work best with scheduled DAGs
    # For manual testing, you can still trigger manually
    schedule="*/5 * * * *",  # Every 5 minutes (for demo)
    catchup=False,
    sla_miss_callback=sla_miss_alert,
    tags=["demo", "monitoring", "sla"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # Task with relaxed SLA (60 seconds)
    fast = PythonOperator(
        task_id="fast_task",
        sla=timedelta(seconds=60),
        python_callable=fast_task
    )
    
    # Task with tight SLA (10 seconds from execution_date)
    # Since fast_task takes ~2s, this leaves only ~8s for variable_task
    # But variable_task sleeps 15-25s, so it will ALWAYS miss
    variable = PythonOperator(
        task_id="variable_task",
        sla=timedelta(seconds=10),  # Will miss since task takes 15-25s
        python_callable=variable_task
    )
    
    # Task with strict SLA (30 seconds)
    critical = PythonOperator(
        task_id="critical_task",
        sla=timedelta(seconds=30),  # Strict deadline
        python_callable=critical_task
    )
    
    end = EmptyOperator(task_id="end")
    
    # Dependencies
    start >> fast >> variable >> critical >> end


dag.doc_md = """
## SLA Demo

This DAG demonstrates Service Level Agreement (SLA) configuration.

### How Airflow SLAs Work:

> **IMPORTANT:** SLAs are measured from the DAG's **execution_date** (scheduled time),
> NOT from when the task starts. The SLA is the deadline by which a task must COMPLETE,
> measured from the beginning of the DAG run.

Example: If a DAG is scheduled for 10:00:00 and a task has `sla=timedelta(seconds=30)`,
the task must complete by 10:00:30 - regardless of when upstream tasks finish.

### SLA Configuration:
- `fast_task`: 60 second SLA (should never miss)
- `variable_task`: 10 second SLA (will ALWAYS miss - task takes 15-25s)
- `critical_task`: 60 second SLA (may miss due to cumulative time)

### Testing:
1. Enable the DAG (runs every 5 minutes)
2. Wait for a scheduled run, OR trigger manually
3. Check **Browse -> SLA Misses** for violations
4. Check scheduler logs for `sla_miss_callback` output

### Notes:
- SLA misses are checked by the scheduler periodically
- The callback may not fire instantly - wait 1-2 minutes
- Manual triggers also respect SLAs (measured from trigger time)
"""
