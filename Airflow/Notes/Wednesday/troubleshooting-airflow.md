# Troubleshooting Airflow

## Learning Objectives
- Diagnose common Airflow issues quickly
- Debug task failures using logs and the UI
- Resolve scheduler and executor problems
- Apply best practices for preventing recurring issues

## Why This Matters

Every data engineer encounters Airflow problems: tasks that won't run, schedulers that hang, mysterious failures. **Troubleshooting skills** reduce downtime and stress. Knowing where to look and what to check turns hours of confusion into minutes of resolution.

## The Concept

### Troubleshooting Framework

When something goes wrong:

```
1. Identify the symptom
2. Check the logs
3. Verify configuration
4. Isolate the component
5. Test the fix
6. Document the solution
```

### Common Issues and Solutions

#### Issue 1: DAG Not Appearing in UI

**Symptoms:**
- DAG file exists but not visible in UI
- DAG was visible before but disappeared

**Diagnostic Steps:**

```bash
# Check for import errors
airflow dags list-import-errors

# Force DAG parsing
airflow dags reserialize

# Check DAG file directly
python /path/to/dags/my_dag.py
```

**Common Causes:**

1. **Python syntax error:**
```python
# Check for errors
python my_dag.py
# Python will show the error line
```

2. **Missing import:**
```python
# Error: ModuleNotFoundError: No module named 'pandas'
# Solution: Install the package
pip install pandas
```

3. **DAG not in `dags_folder`:**
```bash
# Check configuration
airflow config get-value core dags_folder
```

4. **DAG file starts with `.` or excluded:**
```ini
# airflow.cfg
[core]
dags_folder = /path/to/dags
ignore_file_syntax = regex:.*/test_.*
```

#### Issue 2: Tasks Stuck in Queued State

**Symptoms:**
- Tasks show as "queued" but never run
- Executor appears to be ignoring tasks

**Diagnostic Steps:**

```bash
# Check executor status
airflow celery status  # For CeleryExecutor

# Check parallelism limits
airflow config get-value core parallelism
airflow config get-value core max_active_runs_per_dag
```

**Common Causes:**

1. **No workers available:**
```bash
# Start Celery workers
airflow celery worker

# Or restart LocalExecutor scheduler
airflow scheduler
```

2. **Pool slots exhausted:**
```sql
-- Check pool usage in database
SELECT pool, slots, description FROM slot_pool;
```

3. **Parallelism limit reached:**
```ini
# airflow.cfg - increase if needed
[core]
parallelism = 32
max_active_tasks_per_dag = 16
```

#### Issue 3: Task Failures

**Symptoms:**
- Task turns red (failed)
- Pipeline stops or retries indefinitely

**Diagnostic Steps:**

1. Open the task in UI
2. Click "Log" tab
3. Look for ERROR or EXCEPTION lines
4. Check the last lines before failure

**Common Causes:**

1. **Connection issues:**
```python
# Error: NoBrokersAvailable, ConnectionRefused
# Solution: Verify connection in Admin -> Connections
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("my_connection")
```

2. **Timeout:**
```python
# Error: Task timed out
# Solution: Increase task timeout
task = PythonOperator(
    task_id="long_task",
    execution_timeout=timedelta(hours=2),  # Increase from default
    python_callable=my_function
)
```

3. **Resource exhaustion:**
```python
# Error: MemoryError, OOM Killed
# Solution: Process in batches, increase worker resources
def process_in_batches():
    for chunk in pd.read_csv("large.csv", chunksize=10000):
        process(chunk)
```

4. **Permission errors:**
```python
# Error: PermissionError, AccessDenied
# Solution: Check file/API permissions, service account roles
```

#### Issue 4: Scheduler Not Running

**Symptoms:**
- DAGs not being scheduled
- No new DAG runs appearing
- Scheduler health check failing

**Diagnostic Steps:**

```bash
# Check scheduler process
ps aux | grep "airflow scheduler"

# Check scheduler logs
tail -f $AIRFLOW_HOME/logs/scheduler/latest/scheduler.log

# Health check
curl http://localhost:8080/health | jq '.scheduler'
```

**Common Causes:**

1. **Scheduler process died:**
```bash
# Restart scheduler
airflow scheduler

# Or with systemd
sudo systemctl restart airflow-scheduler
```

2. **Database connection lost:**
```bash
# Test database connection
airflow db check
```

3. **Lock contention:**
```sql
-- Check for zombie processes (PostgreSQL)
SELECT * FROM pg_locks WHERE NOT granted;
```

#### Issue 5: Slow DAG Parsing

**Symptoms:**
- UI is slow to update DAG list
- Scheduler uses high CPU
- DAG changes take minutes to appear

**Diagnostic Steps:**

```bash
# Measure parsing time
time python /path/to/dags/slow_dag.py

# Check scheduler logs for parse times
grep "DAG.*parsed" scheduler.log
```

**Common Causes:**

1. **Heavy top-level code:**
```python
# Bad: Runs at parse time
import pandas as pd
df = pd.read_csv("config.csv")  # Slow!

# Good: Move to function
def get_config():
    import pandas as pd
    return pd.read_csv("config.csv")
```

2. **Too many DAG files:**
```python
# Consolidate related DAGs
# Use DAG factory patterns
```

3. **Increase parsing processes:**
```ini
# airflow.cfg
[scheduler]
parsing_processes = 4
min_file_process_interval = 60
```

### Debugging Techniques

#### Enable Debug Logging

```ini
# airflow.cfg
[logging]
base_log_level = DEBUG
```

#### Test Tasks Locally

```bash
# Test a specific task
airflow tasks test my_dag my_task 2024-01-15

# Run with full context
airflow tasks run my_dag my_task 2024-01-15 --local
```

#### Interactive Debugging

```python
def my_function(**context):
    import pdb; pdb.set_trace()  # Breaks into debugger
    # ... rest of code
```

For production, use logging:

```python
import logging

def my_function(**context):
    logging.info(f"Context: {context}")
    logging.info(f"XCom data: {context['ti'].xcom_pull(task_ids='previous')}")
```

#### Database Queries

Directly query Airflow's metadata:

```sql
-- Find recent failed tasks
SELECT dag_id, task_id, execution_date, state, end_date
FROM task_instance
WHERE state = 'failed'
ORDER BY end_date DESC
LIMIT 10;

-- Check DAG runs
SELECT dag_id, run_id, state, start_date, end_date
FROM dag_run
WHERE dag_id = 'my_dag'
ORDER BY start_date DESC
LIMIT 5;
```

### Prevention Best Practices

#### 1. Use Default Args

```python
default_args = {
    "owner": "data_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["oncall@company.com"],
    "execution_timeout": timedelta(hours=1)
}
```

#### 2. Implement Health Checks

```python
def check_connection(**context):
    """Pre-flight check for connections."""
    from airflow.hooks.base import BaseHook
    
    conn = BaseHook.get_connection("critical_db")
    # Quick connectivity test
    
    return True

preflight = PythonOperator(
    task_id="preflight_check",
    python_callable=check_connection
)

preflight >> main_tasks
```

#### 3. Use Idempotent Tasks

```python
def load_data(ds, **context):
    """Idempotent load - safe to re-run."""
    # Delete existing data for this date
    db.execute(f"DELETE FROM target WHERE date = '{ds}'")
    
    # Load fresh data
    db.execute(f"INSERT INTO target SELECT * FROM staging WHERE date = '{ds}'")
```

#### 4. Add Meaningful Logging

```python
import logging

def extract_data(**context):
    ds = context["ds"]
    logging.info(f"Starting extraction for {ds}")
    
    records = fetch_records(ds)
    logging.info(f"Fetched {len(records)} records")
    
    if len(records) == 0:
        logging.warning(f"No records found for {ds} - expected data?")
    
    return {"count": len(records)}
```

### Troubleshooting Checklist

When issues occur, check in order:

- [ ] Is the DAG file syntactically valid? (`python dag.py`)
- [ ] Are there import errors? (`airflow dags list-import-errors`)
- [ ] Is the scheduler running? (`ps aux | grep scheduler`)
- [ ] Is the executor healthy? (workers running, slots available)
- [ ] Are connections configured correctly? (Admin -> Connections)
- [ ] What do the task logs say? (UI -> Task -> Logs)
- [ ] Are there resource constraints? (pools, parallelism)
- [ ] Is the database responsive? (`airflow db check`)

## Summary

- Use a systematic approach: symptom, logs, config, isolate, fix
- Common issues: DAG not appearing, tasks stuck, failures, slow parsing
- Test tasks locally with `airflow tasks test`
- Query the metadata database for deep investigation
- Prevent issues with retries, timeouts, and idempotent tasks
- Add meaningful logging for faster debugging

## Additional Resources

- [Airflow Troubleshooting](https://airflow.apache.org/docs/apache-airflow/stable/howto/index.html)
- [Common Pitfalls](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Astronomer Troubleshooting Guide](https://docs.astronomer.io/learn/debugging-dags)
