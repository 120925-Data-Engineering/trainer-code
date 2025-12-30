# DAG Design Patterns

## Learning Objectives
- Apply best practices for DAG file organization
- Recognize common DAG design patterns (ETL, branching, fan-out/fan-in)
- Structure DAGs for maintainability and testability
- Avoid common anti-patterns that lead to issues

## Why This Matters

Well-designed DAGs are easy to understand, debug, and maintain. Poorly designed DAGs become technical debt---they break unexpectedly, are difficult to modify, and frustrate everyone who works with them. Learning design patterns upfront saves countless hours of refactoring later.

## The Concept

### DAG File Organization

A well-organized Airflow project separates concerns:

```
airflow/
├── dags/
│   ├── __init__.py
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── daily_sales.py
│   │   └── weekly_inventory.py
│   ├── ml/
│   │   ├── __init__.py
│   │   └── model_training.py
│   └── utils/
│       ├── __init__.py
│       └── common_tasks.py
├── plugins/
│   └── custom_operators.py
├── tests/
│   └── dags/
│       └── test_daily_sales.py
└── requirements.txt
```

### Best Practices

#### 1. One DAG Per File

Keep each DAG in its own file for clarity:

```python
# Good: dags/etl/daily_sales.py
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(dag_id="daily_sales", ...) as dag:
    ...
```

#### 2. Use DAG Context Manager

The `with` statement ensures proper DAG registration:

```python
# Good
with DAG(dag_id="my_dag", ...) as dag:
    task1 = PythonOperator(...)
    task2 = PythonOperator(...)

# Avoid
dag = DAG(dag_id="my_dag", ...)
task1 = PythonOperator(..., dag=dag)
task2 = PythonOperator(..., dag=dag)
```

#### 3. Externalize Configuration

Keep configuration outside DAG files:

```python
# config/etl_config.py
ETL_CONFIG = {
    "source_table": "raw_sales",
    "target_table": "clean_sales",
    "batch_size": 10000
}

# dags/etl/daily_sales.py
from config.etl_config import ETL_CONFIG

def extract():
    table = ETL_CONFIG["source_table"]
    ...
```

#### 4. Keep Top-Level Code Minimal

DAG files are parsed frequently. Heavy imports slow everything:

```python
# Bad: Heavy import at top level
import pandas as pd
import numpy as np
from heavy_ml_library import model

# Good: Import inside callable
def process_data():
    import pandas as pd
    # ... use pandas
```

### Common Design Patterns

#### Pattern 1: Linear ETL

The simplest pattern---sequential tasks:

```python
extract >> transform >> load >> validate >> notify
```

```
+--------+     +----------+     +------+     +--------+     +--------+
| Extract| --> | Transform| --> | Load | --> |Validate| --> | Notify |
+--------+     +----------+     +------+     +--------+     +--------+
```

**Use when:** Simple pipelines with no parallelism needed.

#### Pattern 2: Fan-Out / Fan-In

Parallel processing with aggregation:

```python
start >> [process_a, process_b, process_c] >> aggregate >> end
```

```
              +--------+
          +-->|Process A|--+
          |   +--------+   |
+-----+   |   +--------+   |   +---------+   +---+
|Start|-->+-->|Process B|--+-->|Aggregate|-->|End|
+-----+   |   +--------+   |   +---------+   +---+
          |   +--------+   |
          +-->|Process C|--+
              +--------+
```

**Use when:** Processing independent data partitions in parallel.

```python
with DAG(dag_id="fan_out_fan_in", ...) as dag:
    start = EmptyOperator(task_id="start")
    
    # Fan-out: parallel processing
    process_tasks = [
        PythonOperator(
            task_id=f"process_{region}",
            python_callable=process_region,
            op_kwargs={"region": region}
        )
        for region in ["us", "eu", "apac"]
    ]
    
    # Fan-in: aggregate results
    aggregate = PythonOperator(
        task_id="aggregate",
        python_callable=aggregate_results
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> process_tasks >> aggregate >> end
```

#### Pattern 3: Conditional Branching

Execute different paths based on conditions:

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    if condition:
        return "path_a"
    else:
        return "path_b"

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=choose_branch
)

start >> branch >> [path_a, path_b]
path_a >> join
path_b >> join
join >> end
```

```
              +------+
          +-->|Path A|--+
          |   +------+   |
+------+  |              |   +----+   +---+
|Branch|--+              +-->|Join|-->|End|
+------+  |              |   +----+   +---+
          |   +------+   |
          +-->|Path B|--+
              +------+
```

**Use when:** Different logic based on data or time conditions.

#### Pattern 4: Dynamic Task Generation

Create tasks based on runtime data:

```python
# Read configuration to determine tasks
regions = ["us", "eu", "apac", "latam"]

with DAG(dag_id="dynamic_tasks", ...) as dag:
    for region in regions:
        extract = PythonOperator(
            task_id=f"extract_{region}",
            python_callable=extract_data,
            op_kwargs={"region": region}
        )
        
        load = PythonOperator(
            task_id=f"load_{region}",
            python_callable=load_data,
            op_kwargs={"region": region}
        )
        
        extract >> load
```

**Use when:** Similar tasks for multiple entities (regions, tables, clients).

#### Pattern 5: Sensor-Triggered Pipeline

Wait for external conditions before processing:

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath="/data/incoming/daily_export.csv",
    poke_interval=60,  # Check every 60 seconds
    timeout=3600       # Timeout after 1 hour
)

wait_for_file >> extract >> transform >> load
```

**Use when:** Pipelines depend on external events (file arrival, API availability).

### Anti-Patterns to Avoid

#### 1. Monolithic DAGs

**Problem:** One DAG with hundreds of tasks

```python
# Bad: 200 tasks in one DAG
with DAG(dag_id="everything", ...) as dag:
    for table in all_200_tables:
        ...
```

**Solution:** Split into smaller, focused DAGs

#### 2. Heavy Processing in DAG Parsing

**Problem:** Slow parsing blocks the scheduler

```python
# Bad: Database query during parsing
data = database.query("SELECT * FROM config")
for row in data:
    PythonOperator(...)
```

**Solution:** Use Variables or move logic into tasks

#### 3. Missing Retry Configuration

**Problem:** Transient failures cause unnecessary alerts

```python
# Bad: No retries
task = PythonOperator(task_id="api_call", ...)

# Good: Retry with backoff
task = PythonOperator(
    task_id="api_call",
    retries=3,
    retry_delay=timedelta(minutes=5),
    ...
)
```

#### 4. Hardcoded Secrets

**Problem:** Credentials in code

```python
# Bad: Never do this
connection = connect(password="secret123")

# Good: Use Airflow connections
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("my_db")
connection = connect(password=conn.password)
```

## Summary

- Organize DAG files logically with one DAG per file
- Use the context manager pattern for DAG definition
- Common patterns include linear ETL, fan-out/fan-in, branching, and sensor-triggered
- Avoid anti-patterns: monolithic DAGs, heavy parsing, missing retries, hardcoded secrets
- Design for maintainability from the start

## Additional Resources

- [DAG Best Practices - Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Astronomer DAG Writing Guide](https://docs.astronomer.io/learn/dag-best-practices)
- [Data Pipelines with Apache Airflow - Book](https://www.manning.com/books/data-pipelines-with-apache-airflow)
