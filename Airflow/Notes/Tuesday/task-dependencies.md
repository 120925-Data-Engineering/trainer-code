# Task Dependencies

## Learning Objectives
- Define task dependencies using bitshift operators
- Implement complex dependency patterns (fan-out, fan-in, cross-dependencies)
- Use trigger rules to control task execution based on upstream states
- Handle conditional execution with branching

## Why This Matters

Dependencies define the **order and conditions** under which tasks execute. Getting dependencies wrong means tasks run before their inputs are ready, tasks are skipped when they should run, or pipelines fail in confusing ways. Mastering dependency patterns is essential for building reliable workflows.

## The Concept

### Basic Dependency Syntax

Airflow provides two syntaxes for defining dependencies:

#### Bitshift Operators (Recommended)

```python
# A runs before B
task_a >> task_b

# A runs after B
task_a << task_b

# Chaining
task_a >> task_b >> task_c >> task_d
```

#### Method Syntax

```python
# Equivalent to task_a >> task_b
task_a.set_downstream(task_b)
task_b.set_upstream(task_a)
```

The bitshift syntax is more readable and is the preferred approach.

### Parallel Dependencies

Multiple tasks can run in parallel:

```python
# B and C run after A (in parallel)
task_a >> [task_b, task_c]

# D runs after both B and C complete
[task_b, task_c] >> task_d
```

```
     +---+
 +-->| B |--+
 |   +---+  |
+---+       +-->+---+
| A |           | D |
+---+       +-->+---+
 |   +---+  |
 +-->| C |--+
     +---+
```

### Cross Dependencies

When tasks need complex relationships:

```python
from airflow.models.baseoperator import cross_downstream

# All of [A, B] run before all of [C, D]
cross_downstream([task_a, task_b], [task_c, task_d])
```

```
+---+     +---+
| A |--+--| C |
+---+  X  +---+
       |
+---+  |  +---+
| B |--+--| D |
+---+     +---+
```

### Chain Helper

For long sequences:

```python
from airflow.models.baseoperator import chain

# Creates: A >> B >> C >> D >> E
chain(task_a, task_b, task_c, task_d, task_e)

# With parallel groups
chain(task_a, [task_b, task_c], task_d)
# Creates: A >> B, A >> C, B >> D, C >> D
```

### Trigger Rules

By default, a task runs when **all upstream tasks succeed**. Trigger rules change this behavior:

```python
from airflow.utils.trigger_rule import TriggerRule

task = PythonOperator(
    task_id="my_task",
    trigger_rule=TriggerRule.ONE_SUCCESS,
    python_callable=my_function
)
```

#### Available Trigger Rules

| Rule | Behavior |
|------|----------|
| `ALL_SUCCESS` | (Default) All parents succeeded |
| `ALL_FAILED` | All parents failed |
| `ALL_DONE` | All parents completed (success or failure) |
| `ONE_SUCCESS` | At least one parent succeeded |
| `ONE_FAILED` | At least one parent failed |
| `NONE_FAILED` | No parent failed (success or skipped) |
| `NONE_SKIPPED` | No parent was skipped |
| `ALWAYS` | Run regardless of parent states |

#### Common Use Case: Cleanup Task

A cleanup task should run regardless of success or failure:

```python
extract = PythonOperator(task_id="extract", ...)
transform = PythonOperator(task_id="transform", ...)
load = PythonOperator(task_id="load", ...)

cleanup = PythonOperator(
    task_id="cleanup",
    trigger_rule=TriggerRule.ALL_DONE,  # Runs even if upstream fails
    python_callable=cleanup_temp_files
)

extract >> transform >> load >> cleanup
```

#### Common Use Case: Alert on Failure

Send alerts only when something fails:

```python
alert = EmailOperator(
    task_id="send_alert",
    trigger_rule=TriggerRule.ONE_FAILED,
    to="team@company.com",
    subject="Pipeline Failed",
    html_content="Check the Airflow UI for details."
)

[task_a, task_b, task_c] >> alert
```

### Branching

Execute different paths based on conditions:

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    """Return the task_id of the branch to follow."""
    execution_date = context["execution_date"]
    
    if execution_date.weekday() == 0:  # Monday
        return "full_load"
    else:
        return "incremental_load"

branch = BranchPythonOperator(
    task_id="check_day",
    python_callable=choose_branch
)

full_load = PythonOperator(task_id="full_load", ...)
incremental_load = PythonOperator(task_id="incremental_load", ...)
merge = PythonOperator(
    task_id="merge",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
)

branch >> [full_load, incremental_load] >> merge
```

```
                 +-----------+
           +---->| full_load |---+
           |     +-----------+   |
+-------+  |                     |   +-------+
| branch|--+                     +-->| merge |
+-------+  |                     |   +-------+
           |  +---------------+  |
           +->|incremental_load|-+
              +---------------+
```

**Important:** The non-selected branch is marked as "skipped." Use `NONE_FAILED_MIN_ONE_SUCCESS` for the join task.

### Short-Circuit Pattern

Skip all downstream tasks based on a condition:

```python
from airflow.operators.python import ShortCircuitOperator

def check_data_exists(**context):
    """Return True to continue, False to skip downstream."""
    # Check if data file exists
    import os
    return os.path.exists("/data/input.csv")

check = ShortCircuitOperator(
    task_id="check_data",
    python_callable=check_data_exists
)

check >> extract >> transform >> load
```

If `check_data_exists` returns `False`, all downstream tasks are skipped.

### Latest Only Pattern

Only run for the most recent scheduled run:

```python
from airflow.operators.latest_only import LatestOnlyOperator

latest_only = LatestOnlyOperator(task_id="latest_only")

latest_only >> expensive_task
```

Useful when backfilling but you only want certain tasks to run for the most recent interval.

### Dependency Visualization

Always verify dependencies in the Graph view:

1. Go to DAGs -> Your DAG -> Graph
2. Check that arrows flow correctly
3. Look for isolated tasks (no connections)
4. Verify parallel vs. sequential execution

### Complete Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

def check_source():
    return "extract_api" if api_available() else "extract_file"

with DAG(
    dag_id="dependency_example",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # Branching based on data source
    branch = BranchPythonOperator(
        task_id="check_source",
        python_callable=check_source
    )
    
    extract_api = PythonOperator(task_id="extract_api", ...)
    extract_file = PythonOperator(task_id="extract_file", ...)
    
    # Join after branching
    join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    # Parallel transformations
    transform_a = PythonOperator(task_id="transform_a", ...)
    transform_b = PythonOperator(task_id="transform_b", ...)
    
    # Fan-in to load
    load = PythonOperator(task_id="load", ...)
    
    # Cleanup runs always
    cleanup = PythonOperator(
        task_id="cleanup",
        trigger_rule=TriggerRule.ALL_DONE,
        python_callable=cleanup_function
    )
    
    end = EmptyOperator(task_id="end")
    
    # Wire it all together
    start >> branch >> [extract_api, extract_file] >> join
    join >> [transform_a, transform_b] >> load >> cleanup >> end
```

## Summary

- Use `>>` and `<<` operators for clear dependency definition
- Parallel tasks are defined with lists: `A >> [B, C] >> D`
- Trigger rules control when tasks run based on upstream states
- Branching enables conditional execution paths
- ShortCircuit skips downstream tasks based on conditions
- Always verify dependencies in the Graph view

## Additional Resources

- [Task Dependencies - Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#task-dependencies)
- [Trigger Rules](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules)
- [Branching in Airflow](https://docs.astronomer.io/learn/airflow-branch-operator)
