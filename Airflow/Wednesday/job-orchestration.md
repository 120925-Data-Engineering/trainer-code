# Job Orchestration with Airflow

## Learning Objectives
- Design end-to-end pipeline orchestration strategies
- Configure scheduling with cron expressions and data intervals
- Implement catchup and backfill for historical data processing
- Coordinate multiple DAGs for complex workflows

## Why This Matters

Individual DAGs are building blocks; **orchestration** is how you construct the complete data platform. Understanding scheduling strategies, backfills, and cross-DAG coordination enables you to build systems that handle both regular processing and recovery scenarios gracefully.

## The Concept

### Scheduling Fundamentals

#### Cron Expressions

Airflow uses standard cron syntax for scheduling:

```
* * * * *
| | | | |
| | | | +-- Day of week (0-7, Sunday=0 or 7)
| | | +---- Month (1-12)
| | +------ Day of month (1-31)
| +-------- Hour (0-23)
+---------- Minute (0-59)
```

Examples:

| Expression | Meaning |
|------------|---------|
| `0 0 * * *` | Daily at midnight |
| `0 6 * * 1-5` | Weekdays at 6 AM |
| `*/15 * * * *` | Every 15 minutes |
| `0 0 1 * *` | First day of each month |
| `0 12 * * 0` | Sundays at noon |

#### Preset Schedules

Airflow provides convenience presets:

| Preset | Equivalent | Description |
|--------|------------|-------------|
| `@once` | - | Run once on start_date |
| `@hourly` | `0 * * * *` | Every hour |
| `@daily` | `0 0 * * *` | Daily at midnight |
| `@weekly` | `0 0 * * 0` | Sundays at midnight |
| `@monthly` | `0 0 1 * *` | First of month |
| `@yearly` | `0 0 1 1 *` | January 1st |
| `None` | - | Manual trigger only |

```python
with DAG(
    dag_id="daily_etl",
    schedule="@daily",  # or "0 0 * * *"
    ...
) as dag:
    ...
```

### Data Intervals

Airflow 2.x uses **data intervals** to represent the time period a DAG run covers:

```
|-------- Data Interval --------|
|                               |
start -----------------------> end
                               |
                               +-- Run starts here (logical_date)
```

Example for a daily DAG:

| Data Interval Start | Data Interval End | Logical Date | Actual Run Time |
|---------------------|-------------------|--------------|-----------------|
| 2024-01-01 00:00 | 2024-01-02 00:00 | 2024-01-02 00:00 | 2024-01-02 00:00 |

The DAG runs **after** the interval ends to process data from that interval.

```python
def process_data(**context):
    # Access interval information
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]
    logical_date = context["logical_date"]
    
    print(f"Processing data from {data_interval_start} to {data_interval_end}")
```

### Catchup and Backfill

#### Catchup

When `catchup=True`, Airflow creates runs for all intervals between `start_date` and now:

```python
with DAG(
    dag_id="catchup_example",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=True  # Default is True
) as dag:
    ...
```

If today is January 10th, Airflow creates 9 DAG runs (Jan 1-9).

**When to use catchup:**
- Processing historical data is required
- Each run is idempotent
- Data is available for past intervals

**When to disable:**
```python
catchup=False  # Only run from now onward
```

#### Manual Backfill

For controlled historical processing:

```bash
# Backfill specific date range
airflow dags backfill \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    my_dag_id

# With specific parallelism
airflow dags backfill \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --max-active-runs 3 \
    my_dag_id
```

### Timetables (Advanced Scheduling)

For complex schedules, use custom timetables:

```python
from airflow.timetables.trigger import CronTriggerTimetable

# Run on business days only
with DAG(
    dag_id="business_days_only",
    timetable=CronTriggerTimetable(
        cron="0 9 * * 1-5",  # 9 AM, Mon-Fri
        timezone="America/New_York"
    ),
    ...
) as dag:
    ...
```

### Cross-DAG Orchestration

#### ExternalTaskSensor

Wait for another DAG's task to complete:

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_extraction",
    external_dag_id="upstream_dag",
    external_task_id="final_task",
    execution_delta=timedelta(hours=0),  # Same execution time
    timeout=3600,
    poke_interval=60
)
```

#### TriggerDagRunOperator

Trigger another DAG:

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_downstream = TriggerDagRunOperator(
    task_id="trigger_reporting",
    trigger_dag_id="reporting_dag",
    conf={"source": "upstream_complete"},
    wait_for_completion=False
)
```

#### Dataset-Driven Scheduling (Airflow 2.4+)

DAGs can trigger based on dataset updates:

```python
from airflow import Dataset

# Producer DAG
orders_dataset = Dataset("s3://bucket/orders/")

with DAG(dag_id="producer", schedule="@daily") as producer_dag:
    produce = PythonOperator(
        task_id="produce_orders",
        outlets=[orders_dataset],  # Marks dataset as updated
        python_callable=produce_data
    )

# Consumer DAG - runs when dataset is updated
with DAG(dag_id="consumer", schedule=[orders_dataset]) as consumer_dag:
    consume = PythonOperator(
        task_id="consume_orders",
        python_callable=consume_data
    )
```

### Orchestration Patterns

#### Pattern 1: Sequential DAG Chain

```
DAG_A (Extract) --> DAG_B (Transform) --> DAG_C (Load)
```

```python
# In DAG_B
wait_for_a = ExternalTaskSensor(
    task_id="wait_for_dag_a",
    external_dag_id="DAG_A",
    external_task_id="final_task"
)

wait_for_a >> transform_tasks
```

#### Pattern 2: Fan-Out / Broadcast

```
         +--> DAG_B1
DAG_A -->+--> DAG_B2
         +--> DAG_B3
```

```python
# In DAG_A
trigger_b1 = TriggerDagRunOperator(task_id="trigger_b1", trigger_dag_id="DAG_B1")
trigger_b2 = TriggerDagRunOperator(task_id="trigger_b2", trigger_dag_id="DAG_B2")
trigger_b3 = TriggerDagRunOperator(task_id="trigger_b3", trigger_dag_id="DAG_B3")

final_task >> [trigger_b1, trigger_b2, trigger_b3]
```

#### Pattern 3: Event-Driven Pipeline

```
File Arrives --> Sensor Detects --> Processing DAG
```

```python
with DAG(dag_id="file_processor", schedule=None) as dag:  # Manual/triggered only
    
    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name="incoming",
        bucket_key="data/{{ ds }}/input.csv",
        poke_interval=300,
        timeout=3600
    )
    
    process = PythonOperator(...)
    
    wait_for_file >> process
```

### SLAs (Service Level Agreements)

Define expected completion times:

```python
with DAG(
    dag_id="sla_example",
    sla_miss_callback=alert_sla_miss,  # Custom callback
    ...
) as dag:
    
    critical_task = PythonOperator(
        task_id="critical_task",
        sla=timedelta(hours=2),  # Must complete within 2 hours
        python_callable=critical_function
    )
```

SLA misses are tracked and can trigger alerts.

### Complete Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

def extract(**context):
    print(f"Extracting for {context['data_interval_start']}")
    return {"records": 1000}

def transform(**context):
    print("Transforming data")

def load(**context):
    print("Loading to warehouse")

with DAG(
    dag_id="orchestrated_etl",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # 6 AM daily
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "sla": timedelta(hours=3)
    }
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )
    
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )
    
    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )
    
    trigger_reporting = TriggerDagRunOperator(
        task_id="trigger_reporting",
        trigger_dag_id="reporting_pipeline",
        wait_for_completion=False
    )
    
    extract_task >> transform_task >> load_task >> trigger_reporting
```

## Summary

- Use cron expressions or presets for scheduling
- Data intervals represent the time period a run processes
- Catchup enables historical processing; backfill provides manual control
- ExternalTaskSensor and TriggerDagRunOperator coordinate multiple DAGs
- Datasets (2.4+) enable event-driven scheduling
- SLAs track and alert on completion expectations

## Additional Resources

- [DAG Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/index.html)
- [Data-aware Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
- [Cross-DAG Dependencies](https://docs.astronomer.io/learn/cross-dag-dependencies)
