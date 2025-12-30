# Sensors and Triggers

## Learning Objectives
- Understand how Sensors wait for external conditions
- Implement common sensor patterns (file, time, external task)
- Configure sensor modes (poke vs. reschedule)
- Use triggers for event-driven workflows

## Why This Matters

Data pipelines often depend on external events: files arriving, APIs becoming available, upstream jobs completing. **Sensors** allow your DAGs to wait for these conditions before proceeding, ensuring data is ready when processing begins. Without sensors, you risk processing incomplete data or failing unnecessarily.

## The Concept

### What is a Sensor?

A **Sensor** is a special type of operator that waits for a specific condition to be true before succeeding. Sensors repeatedly check (poke) until the condition is met or a timeout occurs.

```
+--------+     +--------+     +--------+
| Sensor |---->| Sensor |---->| Task   |
| poke 1 |     | poke N |     | runs   |
| (wait) |     |(success)|    |        |
+--------+     +--------+     +--------+
```

### Sensor Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `poke_interval` | Seconds between checks | 60 |
| `timeout` | Seconds before failing | 604800 (7 days) |
| `mode` | poke or reschedule | poke |
| `soft_fail` | Mark skipped instead of failed | False |

### Common Sensors

#### FileSensor

Wait for a file to exist:

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_input",
    filepath="/data/incoming/daily_export.csv",
    poke_interval=60,      # Check every minute
    timeout=3600,          # Fail after 1 hour
    mode="poke"
)
```

#### S3KeySensor

Wait for an S3 object:

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_s3 = S3KeySensor(
    task_id="wait_for_s3_file",
    bucket_name="data-lake",
    bucket_key="raw/{{ ds }}/orders.parquet",
    aws_conn_id="aws_default",
    poke_interval=300,     # Check every 5 minutes
    timeout=7200           # Fail after 2 hours
)
```

#### ExternalTaskSensor

Wait for another DAG's task:

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_extraction",
    external_dag_id="extraction_dag",
    external_task_id="final_task",
    execution_delta=timedelta(hours=0),  # Same schedule
    poke_interval=60,
    timeout=3600
)
```

#### DateTimeSensor

Wait until a specific time:

```python
from airflow.sensors.date_time import DateTimeSensor

wait_until_6am = DateTimeSensor(
    task_id="wait_until_6am",
    target_time="{{ execution_date.replace(hour=6, minute=0) }}"
)
```

#### SqlSensor

Wait for a database condition:

```python
from airflow.providers.common.sql.sensors.sql import SqlSensor

wait_for_data = SqlSensor(
    task_id="wait_for_records",
    conn_id="postgres_default",
    sql="SELECT COUNT(*) FROM staging WHERE date = '{{ ds }}'",
    success=lambda result: result[0][0] > 0,  # True if records exist
    poke_interval=120,
    timeout=3600
)
```

#### HttpSensor

Wait for an API endpoint:

```python
from airflow.providers.http.sensors.http import HttpSensor

wait_for_api = HttpSensor(
    task_id="wait_for_api",
    http_conn_id="api_connection",
    endpoint="/health",
    response_check=lambda response: response.status_code == 200,
    poke_interval=30,
    timeout=600
)
```

### Sensor Modes

#### Poke Mode (Default)

The sensor occupies a worker slot while waiting:

```python
sensor = FileSensor(
    task_id="wait_for_file",
    mode="poke",           # Keeps worker slot
    poke_interval=60,
    ...
)
```

**Use when:**
- Short expected wait times
- Frequent poke intervals
- High worker availability

#### Reschedule Mode

The sensor releases the worker slot between pokes:

```python
sensor = FileSensor(
    task_id="wait_for_file",
    mode="reschedule",     # Releases worker between pokes
    poke_interval=300,     # Check every 5 minutes
    ...
)
```

**Use when:**
- Long expected wait times
- Less frequent poke intervals acceptable
- Worker slots are limited

### Smart Sensors (Deprecated)

Airflow 2.2-2.3 had Smart Sensors for batching. They are deprecated in favor of Deferrable Operators.

### Deferrable Operators (Airflow 2.2+)

Deferrable operators suspend execution completely, freeing all resources:

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Deferrable sensor
wait_for_s3 = S3KeySensor(
    task_id="wait_for_file",
    bucket_name="data-lake",
    bucket_key="input/{{ ds }}/data.csv",
    deferrable=True,       # Uses Triggerer component
    poke_interval=300
)
```

**Requirements:**
- Airflow 2.2+
- Triggerer component running
- Provider supports deferrable mode

### Custom Sensors

Create sensors for specific conditions:

```python
from airflow.sensors.base import BaseSensorOperator

class RecordCountSensor(BaseSensorOperator):
    """Wait for minimum record count in a table."""
    
    def __init__(self, conn_id, table, min_count, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table = table
        self.min_count = min_count
    
    def poke(self, context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        result = hook.get_first(f"SELECT COUNT(*) FROM {self.table}")
        count = result[0]
        
        self.log.info(f"Found {count} records (minimum: {self.min_count})")
        return count >= self.min_count

# Usage
wait_for_records = RecordCountSensor(
    task_id="wait_for_records",
    conn_id="postgres_default",
    table="staging_orders",
    min_count=1000,
    poke_interval=120,
    timeout=3600
)
```

### Trigger Rules with Sensors

Handle sensor outcomes:

```python
from airflow.utils.trigger_rule import TriggerRule

sensor = FileSensor(
    task_id="wait_for_file",
    soft_fail=True,  # Skip instead of fail on timeout
    ...
)

process = PythonOperator(
    task_id="process",
    trigger_rule=TriggerRule.NONE_FAILED,  # Runs even if sensor skipped
    ...
)

notify_missing = EmailOperator(
    task_id="notify_missing",
    trigger_rule=TriggerRule.ALL_SKIPPED,  # Only if sensor skipped
    ...
)

sensor >> [process, notify_missing]
```

### Complete Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

def process_data(**context):
    print("Processing data...")

def load_data(**context):
    print("Loading to warehouse...")

with DAG(
    dag_id="sensor_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 8 * * *",  # 8 AM daily
    catchup=False
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # Wait for upstream DAG to complete
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_extraction",
        external_dag_id="extraction_dag",
        external_task_id="complete",
        execution_delta=timedelta(hours=2),  # Upstream runs 2 hours earlier
        mode="reschedule",
        poke_interval=300,
        timeout=7200
    )
    
    # Wait for file to appear in S3
    wait_for_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name="data-lake",
        bucket_key="processed/{{ ds }}/orders.parquet",
        aws_conn_id="aws_default",
        mode="reschedule",
        poke_interval=300,
        timeout=7200
    )
    
    # Process and load
    process = PythonOperator(
        task_id="process",
        python_callable=process_data
    )
    
    load = PythonOperator(
        task_id="load",
        python_callable=load_data
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> wait_for_upstream >> wait_for_file >> process >> load >> end
```

## Summary

- **Sensors** wait for external conditions before allowing downstream tasks to run
- Common sensors: FileSensor, S3KeySensor, ExternalTaskSensor, SqlSensor
- **Poke mode** holds a worker slot; **reschedule mode** releases it between checks
- **Deferrable operators** (2.2+) free all resources during waits
- Use `soft_fail=True` to skip instead of fail on timeout
- Create custom sensors for specific conditions

## Additional Resources

- [Sensors - Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html)
- [Deferrable Operators](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
- [Astronomer Sensors Guide](https://docs.astronomer.io/learn/what-is-a-sensor)
