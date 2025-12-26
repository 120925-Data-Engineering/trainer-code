# DAGs, Operators, and Tasks

## Learning Objectives
- Define what a DAG is and why the "acyclic" property matters
- Understand the relationship between Operators and Tasks
- Identify common Operator types and when to use each
- Read and write basic DAG definitions

## Why This Matters

DAGs, Operators, and Tasks are the **building blocks** of every Airflow workflow. Without a solid understanding of these core concepts, you cannot design effective pipelines. Think of them as the vocabulary you need before writing sentences---mastering these fundamentals enables you to express any workflow logic.

## The Concept

### What is a DAG?

**DAG** stands for **Directed Acyclic Graph**.

- **Directed**: Each edge (connection) between nodes has a direction (A -> B means A runs before B)
- **Acyclic**: There are no cycles---you cannot return to a node you have already visited
- **Graph**: A collection of nodes (tasks) connected by edges (dependencies)

```
     +---+
     | A |
     +---+
      / \
     v   v
  +---+ +---+
  | B | | C |
  +---+ +---+
     \   /
      v v
     +---+
     | D |
     +---+
```

#### Why Acyclic?

If a workflow had cycles, it would never complete:
- A triggers B
- B triggers C
- C triggers A (cycle!)
- A triggers B... (infinite loop)

Airflow enforces the acyclic property to guarantee workflows terminate.

### DAG Definition

A DAG in Airflow is a Python object that defines:
- Workflow metadata (ID, description, schedule)
- Start date and scheduling interval
- Default arguments for tasks
- The tasks themselves and their dependencies

```python
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["team@company.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="my_first_dag",
    default_args=default_args,
    description="A simple example DAG",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "etl"]
) as dag:
    # Tasks go here
    pass
```

### Key DAG Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `dag_id` | Unique identifier | `"etl_pipeline"` |
| `start_date` | First execution date | `datetime(2024, 1, 1)` |
| `schedule` | How often to run | `"@daily"`, `"0 5 * * *"` |
| `catchup` | Run missed intervals | `True` or `False` |
| `default_args` | Default task settings | `{"retries": 3}` |
| `tags` | Categorization in UI | `["etl", "prod"]` |

### What is an Operator?

An **Operator** is a template for a task---it defines **what** action to perform.

Think of Operators as classes and Tasks as instances:
- `PythonOperator` is the class
- `extract_data_task` is an instance

Operators encapsulate:
- The logic to execute
- How to handle success/failure
- What parameters are needed

### Common Operators

#### PythonOperator
Executes a Python function:

```python
from airflow.operators.python import PythonOperator

def my_function():
    print("Hello from Python!")
    return "result"

python_task = PythonOperator(
    task_id="run_python",
    python_callable=my_function
)
```

#### BashOperator
Executes a bash command:

```python
from airflow.operators.bash import BashOperator

bash_task = BashOperator(
    task_id="run_bash",
    bash_command="echo 'Hello from Bash!'"
)
```

#### EmailOperator
Sends an email:

```python
from airflow.operators.email import EmailOperator

email_task = EmailOperator(
    task_id="send_email",
    to="team@company.com",
    subject="Pipeline Complete",
    html_content="<p>The ETL pipeline finished successfully.</p>"
)
```

#### DummyOperator (EmptyOperator in 2.x)
A no-op task for grouping or structuring:

```python
from airflow.operators.empty import EmptyOperator

start = EmptyOperator(task_id="start")
end = EmptyOperator(task_id="end")
```

#### Provider Operators
Airflow has hundreds of operators for external systems:

| Category | Examples |
|----------|----------|
| Cloud | S3, GCS, BigQuery, Redshift |
| Databases | Postgres, MySQL, Snowflake |
| Big Data | Spark, Hive, Hadoop |
| Messaging | Kafka, SQS, Pub/Sub |

### What is a Task?

A **Task** is a specific instance of an Operator within a DAG.

```python
# This is a Task (instance of PythonOperator)
extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_function,
    dag=dag
)
```

Each task has:
- A unique `task_id` within the DAG
- An operator that defines its behavior
- Dependencies on other tasks

### Task Lifecycle

```
+--------+     +--------+     +---------+     +--------+
|  none  | --> | queued | --> | running | --> | success|
+--------+     +--------+     +---------+     +--------+
                                   |
                                   v
                              +---------+
                              | failed  |
                              +---------+
                                   |
                                   v
                              +----------+
                              | up_for   |
                              | retry    |
                              +----------+
```

### Defining Dependencies

Dependencies are defined using bitshift operators or methods:

```python
# Bitshift operators (recommended)
task_a >> task_b  # A runs before B
task_a << task_b  # A runs after B

# Chaining
task_a >> task_b >> task_c

# Multiple dependencies
task_a >> [task_b, task_c]  # A before B and C
[task_a, task_b] >> task_c  # A and B before C

# Method syntax (alternative)
task_b.set_upstream(task_a)
task_a.set_downstream(task_b)
```

### Complete DAG Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def extract():
    print("Extracting data from API...")
    return {"records": 100}

def transform(**context):
    # Access data from previous task via XCom
    print("Transforming data...")

def load():
    print("Loading to database...")

with DAG(
    dag_id="etl_example",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    
    start = EmptyOperator(task_id="start")
    
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
    
    notify = BashOperator(
        task_id="notify",
        bash_command='echo "Pipeline complete!"'
    )
    
    end = EmptyOperator(task_id="end")
    
    # Define the workflow
    start >> extract_task >> transform_task >> load_task >> notify >> end
```

## Summary

- A **DAG** is a Directed Acyclic Graph that defines workflow structure
- **Operators** are templates that define what action to perform
- **Tasks** are instances of operators within a specific DAG
- Dependencies are defined using `>>` and `<<` operators
- Common operators include PythonOperator, BashOperator, and provider-specific operators

## Additional Resources

- [Airflow Concepts - DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Airflow Operators Guide](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html)
- [Astronomer DAG Writing Best Practices](https://docs.astronomer.io/learn/dag-best-practices)
