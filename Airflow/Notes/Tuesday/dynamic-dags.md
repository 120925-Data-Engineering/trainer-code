# Dynamic DAGs

## Learning Objectives
- Generate DAGs dynamically using Python loops and logic
- Create tasks based on external configuration (files, databases, APIs)
- Understand the tradeoffs of dynamic DAG generation
- Implement common patterns for dynamic task creation

## Why This Matters

Static DAGs work for simple, fixed workflows. But real-world scenarios often require flexibility: processing varying numbers of tables, handling different clients, or adapting to changing configurations. **Dynamic DAGs** allow your pipelines to evolve without code changes, making them more maintainable and scalable.

## The Concept

### Static vs. Dynamic DAGs

#### Static DAG
Tasks are hardcoded:

```python
extract_users = PythonOperator(task_id="extract_users", ...)
extract_orders = PythonOperator(task_id="extract_orders", ...)
extract_products = PythonOperator(task_id="extract_products", ...)
```

Adding a new table requires code changes.

#### Dynamic DAG
Tasks are generated from data:

```python
tables = ["users", "orders", "products", "inventory", "shipments"]

for table in tables:
    PythonOperator(task_id=f"extract_{table}", ...)
```

Adding a new table requires only a configuration change.

### Pattern 1: Loop-Based Generation

The simplest dynamic pattern:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

TABLES = ["users", "orders", "products", "inventory"]

def extract_table(table_name, **context):
    print(f"Extracting {table_name}...")
    # Actual extraction logic here

def load_table(table_name, **context):
    print(f"Loading {table_name}...")
    # Actual load logic here

with DAG(
    dag_id="dynamic_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
) as dag:
    
    for table in TABLES:
        extract = PythonOperator(
            task_id=f"extract_{table}",
            python_callable=extract_table,
            op_kwargs={"table_name": table}
        )
        
        load = PythonOperator(
            task_id=f"load_{table}",
            python_callable=load_table,
            op_kwargs={"table_name": table}
        )
        
        extract >> load
```

Result: 4 extract tasks and 4 load tasks, each processing a different table.

### Pattern 2: Configuration File

Read configuration from a file:

```yaml
# config/tables.yaml
tables:
  - name: users
    source: postgres
    priority: high
  - name: orders
    source: postgres
    priority: high
  - name: logs
    source: s3
    priority: low
```

```python
import yaml
from pathlib import Path

# Load configuration (at DAG parse time)
config_path = Path(__file__).parent / "config" / "tables.yaml"
with open(config_path) as f:
    config = yaml.safe_load(f)

with DAG(dag_id="config_driven_etl", ...) as dag:
    for table_config in config["tables"]:
        table_name = table_config["name"]
        source = table_config["source"]
        
        extract = PythonOperator(
            task_id=f"extract_{table_name}",
            python_callable=extract_from_source,
            op_kwargs={"table": table_name, "source": source}
        )
```

### Pattern 3: Multiple DAGs from Configuration

Generate entirely separate DAGs:

```python
# config/clients.yaml
clients:
  - name: acme_corp
    schedule: "@daily"
    tables: [orders, users]
  - name: globex_inc
    schedule: "@hourly"
    tables: [transactions]
```

```python
import yaml

with open("config/clients.yaml") as f:
    clients = yaml.safe_load(f)["clients"]

def create_client_dag(client_config):
    dag_id = f"etl_{client_config['name']}"
    
    with DAG(
        dag_id=dag_id,
        schedule=client_config["schedule"],
        start_date=datetime(2024, 1, 1)
    ) as dag:
        for table in client_config["tables"]:
            PythonOperator(
                task_id=f"process_{table}",
                python_callable=process_table,
                op_kwargs={"client": client_config["name"], "table": table}
            )
    
    return dag

# Generate DAGs for each client
for client in clients:
    globals()[f"dag_{client['name']}"] = create_client_dag(client)
```

### Pattern 4: Database-Driven Configuration

Read task definitions from a database:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import json

def get_pipeline_config():
    """Fetch configuration from database."""
    # This runs at DAG parse time
    conn = BaseHook.get_connection("config_db")
    # Simplified: in reality, use a proper DB client
    return [
        {"name": "users", "enabled": True},
        {"name": "orders", "enabled": True},
        {"name": "legacy", "enabled": False}
    ]

# WARNING: This query runs every time DAGs are parsed
config = get_pipeline_config()

with DAG(dag_id="db_driven_dag", ...) as dag:
    for item in config:
        if item["enabled"]:
            PythonOperator(
                task_id=f"process_{item['name']}",
                python_callable=process_item,
                op_kwargs={"item_name": item["name"]}
            )
```

**Caution:** Database queries during parsing can slow down the scheduler.

### Pattern 5: TaskGroup for Organization

Group related dynamic tasks:

```python
from airflow.utils.task_group import TaskGroup

REGIONS = ["us", "eu", "apac"]

with DAG(dag_id="regional_etl", ...) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    for region in REGIONS:
        with TaskGroup(group_id=f"region_{region}") as region_group:
            extract = PythonOperator(
                task_id="extract",
                python_callable=extract_data,
                op_kwargs={"region": region}
            )
            
            transform = PythonOperator(
                task_id="transform",
                python_callable=transform_data,
                op_kwargs={"region": region}
            )
            
            load = PythonOperator(
                task_id="load",
                python_callable=load_data,
                op_kwargs={"region": region}
            )
            
            extract >> transform >> load
        
        start >> region_group >> end
```

In the UI, tasks are grouped visually:

```
start -> [region_us] -> end
         [region_eu]
         [region_apac]
```

### Airflow 2.3+ Dynamic Task Mapping

For true runtime-dynamic tasks:

```python
from airflow.decorators import dag, task

@dag(start_date=datetime(2024, 1, 1), schedule="@daily")
def dynamic_mapping_example():
    
    @task
    def get_files():
        """Returns a list at runtime."""
        return ["file1.csv", "file2.csv", "file3.csv"]
    
    @task
    def process_file(filename):
        print(f"Processing {filename}")
        return filename
    
    files = get_files()
    processed = process_file.expand(filename=files)

dag_instance = dynamic_mapping_example()
```

The number of `process_file` tasks is determined at **runtime**, not parse time.

### Tradeoffs and Considerations

#### Parse-Time vs. Runtime

| Aspect | Parse-Time Dynamic | Runtime Dynamic (expand) |
|--------|-------------------|-------------------------|
| When determined | DAG file parsing | Task execution |
| Visibility | Tasks visible before run | Tasks appear during run |
| Performance | Can slow parsing | More efficient |
| Complexity | Simpler to implement | Requires Airflow 2.3+ |

#### Best Practices

1. **Cache configuration**: Avoid repeated database queries
2. **Limit dynamic scope**: Generate 10s of tasks, not 1000s
3. **Use Variables**: For simple key-value configuration
4. **Version control configs**: Keep YAML/JSON files in git
5. **Document patterns**: Make dynamic logic discoverable

### Complete Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import yaml
from pathlib import Path

# Load configuration
CONFIG_PATH = Path(__file__).parent / "config" / "pipelines.yaml"
with open(CONFIG_PATH) as f:
    PIPELINES = yaml.safe_load(f)["pipelines"]

def extract(pipeline_name, source, **context):
    print(f"Extracting {pipeline_name} from {source}")

def transform(pipeline_name, **context):
    print(f"Transforming {pipeline_name}")

def load(pipeline_name, destination, **context):
    print(f"Loading {pipeline_name} to {destination}")

with DAG(
    dag_id="dynamic_pipeline_factory",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    for pipeline in PIPELINES:
        name = pipeline["name"]
        source = pipeline["source"]
        destination = pipeline["destination"]
        
        with TaskGroup(group_id=name) as pipeline_group:
            extract_task = PythonOperator(
                task_id="extract",
                python_callable=extract,
                op_kwargs={"pipeline_name": name, "source": source}
            )
            
            transform_task = PythonOperator(
                task_id="transform",
                python_callable=transform,
                op_kwargs={"pipeline_name": name}
            )
            
            load_task = PythonOperator(
                task_id="load",
                python_callable=load,
                op_kwargs={"pipeline_name": name, "destination": destination}
            )
            
            extract_task >> transform_task >> load_task
        
        start >> pipeline_group >> end
```

## Summary

- Dynamic DAGs generate tasks from data rather than hardcoding
- Common sources: Python lists, YAML/JSON files, databases
- Use TaskGroups to organize related dynamic tasks
- Airflow 2.3+ supports true runtime dynamic mapping
- Balance flexibility with parsing performance
- Document dynamic patterns for team understanding

## Additional Resources

- [Dynamic DAG Generation - Astronomer](https://docs.astronomer.io/learn/dynamically-generating-dags)
- [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- [TaskGroups](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#taskgroups)
