# Airflow UI Overview

## Learning Objectives
- Navigate the Airflow web interface confidently
- Interpret DAG views and task states
- Access task logs and execution details
- Use the UI to trigger, pause, and debug DAGs

## Why This Matters

The Airflow UI is your **command center** for managing workflows. While you author DAGs in code, you monitor, debug, and operate them through the UI. Knowing where to find information quickly is the difference between resolving an incident in minutes versus hours.

## The Concept

### Accessing the UI

After starting the Airflow webserver:

```bash
airflow webserver --port 8080
```

Navigate to `http://localhost:8080` and log in with your credentials.

### Main Navigation

The Airflow UI has several key sections:

| Section | Purpose |
|---------|---------|
| **DAGs** | List and manage all DAGs |
| **Datasets** | View dataset-driven dependencies |
| **Security** | Manage users and permissions |
| **Browse** | Explore runs, logs, and jobs |
| **Admin** | Configure connections, pools, variables |
| **Docs** | Links to documentation |

### The DAGs View

The main landing page shows all discovered DAGs:

```
+------+--------+----------+--------+--------+---------+-------+
| DAG  | Owner  | Runs     | Schedule| Last   | Next   |Actions|
|      |        |          |        | Run    | Run    |       |
+------+--------+----------+--------+--------+---------+-------+
| etl  | team   | |||||||  | @daily | 5m ago | 19h    | > [] ||
+------+--------+----------+--------+--------+---------+-------+
```

Key elements:
- **Toggle**: Enable/disable DAG scheduling
- **DAG Name**: Click to view details
- **Recent Runs**: Color-coded status bars
- **Schedule**: When the DAG runs
- **Actions**: Trigger, refresh, delete

### Task State Colors

Airflow uses consistent colors for task states:

| Color | State | Meaning |
|-------|-------|---------|
| Green | Success | Task completed successfully |
| Red | Failed | Task failed |
| Yellow | Running | Task is currently executing |
| Orange | Up for retry | Task failed but will retry |
| Light Blue | Queued | Task is queued for execution |
| Lime | Up for reschedule | Sensor waiting |
| Pink | Removed | Task was removed from DAG |
| Gray | No status | Task has not run |

### DAG Detail Views

Click on a DAG name to access detailed views:

#### Grid View (Default)
Shows a matrix of DAG runs and task states:

```
         Run 1    Run 2    Run 3    Run 4
extract    [x]      [x]      [x]      [ ]
transform  [x]      [x]      [!]      [ ]
load       [x]      [x]      [ ]      [ ]
```

- Rows: Tasks
- Columns: DAG runs
- Cells: Task states (color-coded)

#### Graph View
Visual representation of task dependencies:

```
+--------+     +----------+     +------+
| extract| --> | transform| --> | load |
+--------+     +----------+     +------+
```

Useful for:
- Understanding workflow structure
- Identifying parallel tasks
- Debugging dependency issues

#### Calendar View
Shows DAG runs on a calendar:
- Quickly identify patterns (always fails on Mondays?)
- Spot gaps in execution
- Plan maintenance windows

#### Code View
Displays the DAG's Python source code (read-only)

### Task Instance Details

Click on any task to access details:

#### Task Instance Actions
- **Run**: Execute this specific task
- **Clear**: Reset task state (re-run)
- **Mark Success**: Manually mark as successful
- **Mark Failed**: Manually mark as failed

#### Log View
Shows task execution output:

```
[2024-01-15 10:30:00,123] INFO - Starting task...
[2024-01-15 10:30:01,456] INFO - Processing 1000 records
[2024-01-15 10:30:05,789] INFO - Task completed successfully
```

Log navigation:
- Multiple attempts (if retries occurred)
- Download logs
- Full log vs. tail

#### XCom View
Shows data passed between tasks:

| Key | Value | Timestamp |
|-----|-------|-----------|
| return_value | {"records": 100} | 2024-01-15 10:30:05 |

### Triggering DAGs

#### Manual Trigger

1. Click the "Play" button on the DAG row
2. Optionally configure trigger parameters:
   ```json
   {
     "param1": "value1",
     "run_date": "2024-01-15"
   }
   ```
3. Click "Trigger"

#### Trigger with Config

In the Trigger DAG dialog, you can pass configuration:

```python
# In your DAG, access via:
dag_run.conf.get("param1", "default_value")
```

### Admin Section

#### Connections
Store credentials for external systems:

| Connection ID | Type | Host | Notes |
|---------------|------|------|-------|
| postgres_default | Postgres | db.example.com | Production DB |
| aws_default | AWS | - | S3 access |

#### Variables
Store configuration values:

| Key | Value |
|-----|-------|
| environment | production |
| batch_size | 1000 |

Access in DAGs:
```python
from airflow.models import Variable
batch_size = Variable.get("batch_size")
```

#### Pools
Limit concurrent task execution:

| Pool | Slots | Running | Queued |
|------|-------|---------|--------|
| default | 128 | 5 | 0 |
| heavy_tasks | 4 | 2 | 3 |

### Browse Menu

#### DAG Runs
View all runs across all DAGs:
- Filter by state, DAG ID, date
- Bulk actions (clear, mark success)

#### Task Instances
View all task instances:
- Filter by state, operator, queue
- Debug long-running or stuck tasks

#### Jobs
Internal Airflow processes:
- Scheduler jobs
- Triggerer jobs
- Background workers

### Common UI Workflows

#### Debugging a Failed Task

1. Go to DAGs -> Click on the failed DAG
2. Find the failed run (red indicator)
3. Click on the failed task
4. View Logs to identify the error
5. Fix the issue in code
6. Clear the task to re-run

#### Backfilling Historical Data

1. Pause the DAG (toggle off)
2. Go to Browse -> DAG Runs
3. Trigger runs for past dates
4. Or use CLI: `airflow dags backfill -s 2024-01-01 -e 2024-01-10 my_dag`

#### Monitoring Active Runs

1. Browse -> DAG Runs (filter: state=running)
2. Watch for long-running tasks
3. Check resource pools for queued tasks

## Summary

- The DAGs page is your main dashboard for workflow management
- Task states are color-coded for quick visual assessment
- Multiple views (Grid, Graph, Calendar) serve different purposes
- Task details include logs, XComs, and execution history
- Admin section manages connections, variables, and pools
- Master the UI to operate Airflow effectively

## Additional Resources

- [Airflow UI Reference](https://airflow.apache.org/docs/apache-airflow/stable/ui.html)
- [Astronomer UI Guide](https://docs.astronomer.io/learn/airflow-ui)
- [Airflow DAG Views Explained](https://airflow.apache.org/docs/apache-airflow/stable/ui.html)
