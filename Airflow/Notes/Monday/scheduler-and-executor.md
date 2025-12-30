# Scheduler and Executor

## Learning Objectives
- Understand the Scheduler's role in Airflow architecture
- Compare different Executor types and their use cases
- Explain how tasks move from scheduled to running to complete
- Choose the appropriate executor for your deployment

## Why This Matters

The Scheduler and Executor are the **engine** of Airflow---they determine when tasks run and how they run. A misconfigured scheduler leads to missed runs; choosing the wrong executor limits scalability or wastes resources. Understanding these components is essential for both development and production deployments.

## The Concept

### The Airflow Scheduler

The **Scheduler** is Airflow's heartbeat. It continuously:

1. **Parses DAG files** to discover new workflows
2. **Checks schedules** to determine what should run
3. **Creates DagRuns** for scheduled intervals
4. **Creates TaskInstances** for each task in a DagRun
5. **Queues tasks** whose dependencies are satisfied
6. **Monitors state** and handles retries

```
+------------------+
|  DAG Files       |
| (Python files)   |
+--------+---------+
         |
         v
+--------+---------+
|    Scheduler     |
| - Parse DAGs     |
| - Check schedule |
| - Queue tasks    |
+--------+---------+
         |
         v
+--------+---------+
|    Executor      |
| - Run tasks      |
+------------------+
```

### Scheduler Configuration

Key settings in `airflow.cfg`:

```ini
[scheduler]
# How often to scan DAGs folder for changes
dag_dir_list_interval = 300

# How often to check for new tasks to schedule
scheduler_heartbeat_sec = 5

# How many DAGs to parse in parallel
parsing_processes = 2

# Minimum interval between DAG runs (prevents rapid re-runs)
min_file_process_interval = 30
```

### How Scheduling Works

When you define a DAG with a schedule:

```python
with DAG(
    dag_id="daily_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
) as dag:
    ...
```

The scheduler:

1. Calculates **data intervals** based on `start_date` and `schedule`
2. Creates a **DagRun** for each interval
3. Creates **TaskInstances** for each task in the DagRun
4. Queues tasks when their dependencies complete

#### Data Interval Example

For a daily DAG starting January 1, 2024:

| Data Interval Start | Data Interval End | Execution Date |
|---------------------|-------------------|----------------|
| 2024-01-01 00:00 | 2024-01-02 00:00 | 2024-01-02 00:00 |
| 2024-01-02 00:00 | 2024-01-03 00:00 | 2024-01-03 00:00 |

The DAG runs **after** its data interval ends.

### What is an Executor?

The **Executor** is responsible for actually running tasks. While the Scheduler decides *what* and *when*, the Executor handles *how* and *where*.

### Executor Types

#### 1. SequentialExecutor (Development Only)

Runs one task at a time. No parallelism.

```ini
[core]
executor = SequentialExecutor
```

**Use case:** Local development, debugging
**Limitation:** Cannot run tasks in parallel

#### 2. LocalExecutor

Runs tasks in parallel using local subprocesses.

```ini
[core]
executor = LocalExecutor
```

**Use case:** Single-machine deployments, small teams
**Requirement:** PostgreSQL or MySQL (not SQLite)

```
+-------------+
|  Scheduler  |
+------+------+
       |
       v
+------+------+
|LocalExecutor|
+------+------+
       |
   +---+---+
   |   |   |
   v   v   v
+---+ +---+ +---+
|P1 | |P2 | |P3 |  (Subprocesses)
+---+ +---+ +---+
```

#### 3. CeleryExecutor

Distributes tasks across multiple worker machines using Celery.

```ini
[core]
executor = CeleryExecutor

[celery]
broker_url = redis://redis:6379/0
result_backend = db+postgresql://airflow:airflow@postgres/airflow
```

**Use case:** Production, horizontal scaling
**Requirement:** Redis or RabbitMQ as message broker

```
+-------------+
|  Scheduler  |
+------+------+
       |
       v
+-------------+
|   Broker    |  (Redis/RabbitMQ)
+------+------+
       |
   +---+---+
   |   |   |
   v   v   v
+---+ +---+ +---+
|W1 | |W2 | |W3 |  (Celery Workers)
+---+ +---+ +---+
```

#### 4. KubernetesExecutor

Launches each task as a Kubernetes pod.

```ini
[core]
executor = KubernetesExecutor

[kubernetes]
namespace = airflow
worker_container_repository = apache/airflow
worker_container_tag = 2.7.3
```

**Use case:** Cloud-native deployments, resource isolation
**Benefit:** Each task gets isolated resources; scales to zero

```
+-------------+
|  Scheduler  |
+------+------+
       |
       v
+-------------+
| Kubernetes  |
|   Cluster   |
+------+------+
       |
   +---+---+
   |   |   |
   v   v   v
+---+ +---+ +---+
|Pod| |Pod| |Pod|  (Ephemeral pods)
+---+ +---+ +---+
```

### Executor Comparison

| Executor | Parallelism | Scalability | Complexity | Best For |
|----------|-------------|-------------|------------|----------|
| Sequential | None | None | Lowest | Development |
| Local | Per-machine | Vertical | Low | Small deployments |
| Celery | Distributed | Horizontal | Medium | Production clusters |
| Kubernetes | Distributed | Horizontal | High | Cloud-native, isolation |

### Task Lifecycle in Detail

When a task runs:

```
1. SCHEDULED
   Scheduler identifies task should run

2. QUEUED
   Task is sent to the Executor's queue

3. RUNNING
   Executor picks up task and starts execution

4. SUCCESS / FAILED
   Task completes; state recorded in database

5. UP_FOR_RETRY (if failed and retries remain)
   Task is rescheduled for retry
```

### Parallelism Controls

Airflow provides multiple levels of parallelism control:

```ini
[core]
# Maximum active DAG runs across all DAGs
parallelism = 32

# Maximum active DAG runs per DAG
max_active_runs_per_dag = 16

# Maximum active tasks per DAG run
max_active_tasks_per_dag = 16
```

In DAG definition:

```python
with DAG(
    dag_id="my_dag",
    max_active_runs=3,      # Limit concurrent runs of this DAG
    max_active_tasks=10,    # Limit concurrent tasks per run
    ...
) as dag:
    ...
```

### Pools for Resource Management

Pools limit concurrent task execution across DAGs:

```python
# Define in Airflow UI or CLI
# airflow pools set my_pool 5 "Limit to 5 concurrent tasks"

task = PythonOperator(
    task_id="resource_heavy_task",
    pool="my_pool",
    python_callable=my_function
)
```

## Summary

- The **Scheduler** parses DAGs, creates runs, and queues tasks
- The **Executor** determines how and where tasks run
- Choose your executor based on scale: Sequential (dev) -> Local (small) -> Celery/K8s (production)
- Use parallelism settings and pools to control resource usage
- Tasks move through states: scheduled -> queued -> running -> success/failed

## Additional Resources

- [Airflow Scheduler](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)
- [Airflow Executors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html)
- [Scaling Airflow - Astronomer](https://docs.astronomer.io/learn/airflow-scaling-workers)
