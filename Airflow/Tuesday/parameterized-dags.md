# Parameterized DAGs

## Learning Objectives
- Use Airflow Variables for runtime configuration
- Pass data between tasks with XComs
- Leverage Jinja templating for dynamic values
- Implement parameterized DAGs that adapt to different contexts

## Why This Matters

Hardcoded values create rigid pipelines that break when requirements change. **Parameterized DAGs** separate configuration from logic, enabling the same DAG to run with different settings, dates, or inputs. This pattern is essential for reusable, maintainable workflows.

## The Concept

### Airflow Variables

**Variables** are key-value pairs stored in Airflow's metadata database, accessible at runtime.

#### Creating Variables

**Via UI:**
1. Go to Admin -> Variables
2. Click "+"
3. Enter Key and Value

**Via CLI:**
```bash
airflow variables set environment production
airflow variables set batch_size 1000
airflow variables set config_json '{"retries": 3, "timeout": 300}'
```

**Via API:**
```python
from airflow.models import Variable

Variable.set("my_key", "my_value")
Variable.set("my_json", {"nested": "data"}, serialize_json=True)
```

#### Accessing Variables

```python
from airflow.models import Variable

# Simple value
environment = Variable.get("environment")

# With default
batch_size = Variable.get("batch_size", default_var=500)

# JSON value
config = Variable.get("config_json", deserialize_json=True)
retries = config["retries"]
```

#### Variables in Templates

```python
task = BashOperator(
    task_id="print_env",
    bash_command="echo Environment: {{ var.value.environment }}"
)

# JSON access in templates
task = BashOperator(
    task_id="print_config",
    bash_command="echo Retries: {{ var.json.config_json.retries }}"
)
```

### XComs (Cross-Communications)

**XComs** pass data between tasks within a DAG run.

#### Pushing XComs

```python
def push_data(**context):
    # Method 1: Return value (auto-pushed as 'return_value')
    return {"records": 100, "status": "complete"}

def push_explicit(**context):
    # Method 2: Explicit push
    context["ti"].xcom_push(key="custom_key", value="custom_value")

push_task = PythonOperator(
    task_id="push_data",
    python_callable=push_data
)
```

#### Pulling XComs

```python
def pull_data(**context):
    # Pull return value from specific task
    data = context["ti"].xcom_pull(task_ids="push_data")
    print(f"Received: {data}")
    
    # Pull specific key
    value = context["ti"].xcom_pull(task_ids="push_explicit", key="custom_key")

pull_task = PythonOperator(
    task_id="pull_data",
    python_callable=pull_data
)
```

#### XComs in Templates

```python
task = BashOperator(
    task_id="use_xcom",
    bash_command="echo Records: {{ ti.xcom_pull(task_ids='push_data')['records'] }}"
)
```

#### XCom Limitations

- Default size limit: ~48KB (stored in database)
- For large data: use external storage (S3, GCS) and pass references
- XComs are cleared after DAG run retention period

### Jinja Templating

Airflow uses Jinja2 for dynamic value injection at runtime.

#### Built-in Template Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `{{ ds }}` | Execution date (YYYY-MM-DD) | 2024-01-15 |
| `{{ ds_nodash }}` | Execution date (YYYYMMDD) | 20240115 |
| `{{ ts }}` | Execution timestamp | 2024-01-15T00:00:00+00:00 |
| `{{ execution_date }}` | DateTime object | datetime(2024, 1, 15) |
| `{{ prev_ds }}` | Previous execution date | 2024-01-14 |
| `{{ next_ds }}` | Next execution date | 2024-01-16 |
| `{{ dag }}` | Current DAG object | DAG object |
| `{{ task }}` | Current task object | Task object |
| `{{ params }}` | User-defined params | {"key": "value"} |

#### Template Examples

```python
# SQL with templated date
sql_task = PostgresOperator(
    task_id="query_by_date",
    sql="""
        SELECT * FROM orders 
        WHERE order_date = '{{ ds }}'
        AND region = '{{ params.region }}'
    """,
    params={"region": "US"}
)

# Bash with templated path
bash_task = BashOperator(
    task_id="process_file",
    bash_command="python process.py --date {{ ds }} --env {{ var.value.environment }}"
)

# S3 key with execution date
s3_task = S3CopyObjectOperator(
    task_id="copy_to_processed",
    source_bucket="raw-data",
    source_key="input/{{ ds_nodash }}/data.csv",
    dest_bucket="processed-data",
    dest_key="output/{{ ds_nodash }}/data.csv"
)
```

#### Custom Macros

Define reusable template functions:

```python
def days_ago_formatted(days, format="%Y-%m-%d"):
    from datetime import datetime, timedelta
    return (datetime.now() - timedelta(days=days)).strftime(format)

with DAG(
    dag_id="custom_macros",
    user_defined_macros={
        "days_ago": days_ago_formatted
    },
    ...
) as dag:
    task = BashOperator(
        task_id="use_macro",
        bash_command="echo {{ days_ago(7) }}"  # Outputs date 7 days ago
    )
```

### DAG Parameters (params)

Pass parameters at the DAG or task level:

```python
with DAG(
    dag_id="parameterized_dag",
    params={
        "source_table": "users",
        "batch_size": 1000,
        "enable_validation": True
    },
    ...
) as dag:
    
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data,
        # Access params in Python
        op_kwargs={
            "table": "{{ params.source_table }}",
            "batch": "{{ params.batch_size }}"
        }
    )
```

#### Overriding at Trigger Time

When triggering manually:
1. Click "Trigger DAG w/ config"
2. Provide JSON:
```json
{
    "source_table": "orders",
    "batch_size": 5000
}
```

In code:
```python
def my_task(**context):
    # Runtime params override defaults
    params = context["params"]
    table = params.get("source_table", "default_table")
```

### Params with Validation (Airflow 2.2+)

Define parameter schemas:

```python
from airflow import DAG
from airflow.models.param import Param

with DAG(
    dag_id="validated_params",
    params={
        "environment": Param(
            default="dev",
            type="string",
            enum=["dev", "staging", "prod"]
        ),
        "batch_size": Param(
            default=1000,
            type="integer",
            minimum=1,
            maximum=10000
        ),
        "enable_notifications": Param(
            default=True,
            type="boolean"
        )
    },
    ...
) as dag:
    ...
```

Invalid parameters are rejected at trigger time.

### Complete Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.models.param import Param
from datetime import datetime

def extract_data(**context):
    """Extract data based on parameters."""
    params = context["params"]
    ds = context["ds"]
    
    source = params["source_table"]
    batch_size = params["batch_size"]
    
    print(f"Extracting {source} for {ds} with batch size {batch_size}")
    
    # Simulate extraction result
    record_count = 1500
    
    # Push result for downstream tasks
    return {"records": record_count, "source": source}

def validate_data(**context):
    """Validate extracted data."""
    ti = context["ti"]
    
    # Pull result from extract task
    extract_result = ti.xcom_pull(task_ids="extract")
    record_count = extract_result["records"]
    
    # Get threshold from Variables
    min_records = int(Variable.get("min_record_threshold", default_var=100))
    
    if record_count < min_records:
        raise ValueError(f"Only {record_count} records (minimum: {min_records})")
    
    print(f"Validation passed: {record_count} records")
    return True

with DAG(
    dag_id="fully_parameterized_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    params={
        "source_table": Param(
            default="orders",
            type="string",
            description="Source table to extract"
        ),
        "batch_size": Param(
            default=1000,
            type="integer",
            minimum=100,
            maximum=10000
        ),
        "enable_validation": Param(
            default=True,
            type="boolean"
        )
    },
    user_defined_macros={
        "env": lambda: Variable.get("environment", "dev")
    }
) as dag:
    
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data
    )
    
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_data
    )
    
    load = BashOperator(
        task_id="load",
        bash_command="""
            echo "Loading {{ ti.xcom_pull(task_ids='extract')['source'] }}"
            echo "Date: {{ ds }}"
            echo "Environment: {{ env() }}"
        """
    )
    
    extract >> validate >> load
```

## Summary

- **Variables** store global configuration accessible across DAGs
- **XComs** pass data between tasks within a DAG run
- **Jinja templates** inject dynamic values at runtime
- **Params** allow DAG-level configuration with optional validation
- Combine these features for flexible, reusable pipelines
- Avoid passing large data via XComs; use external storage

## Additional Resources

- [Airflow Variables](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html)
- [XComs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [Templates Reference](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
- [DAG Params](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html)
