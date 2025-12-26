# Monitoring and Alerting

## Learning Objectives
- Access and interpret task logs effectively
- Configure SLAs for performance monitoring
- Set up email and Slack alerts for failures
- Integrate Airflow with external monitoring tools

## Why This Matters

In production, pipelines fail. Networks drop, APIs timeout, data arrives late. **Monitoring and alerting** ensure you know about problems before stakeholders do. Effective monitoring reduces mean time to detection (MTTD) and mean time to resolution (MTTR), keeping your data platform reliable.

## The Concept

### Accessing Task Logs

Every task execution produces logs accessible via:

#### Airflow UI
1. Navigate to DAGs -> Your DAG
2. Click on a task instance
3. Select "Log" tab

#### Log Structure
```
[2024-01-15 10:00:00,123] {taskinstance.py:1234} INFO - Dependencies all met for ...
[2024-01-15 10:00:01,456] {python.py:177} INFO - Executing: my_function
[2024-01-15 10:00:02,789] {logging_mixin.py:137} INFO - Processing 1000 records...
[2024-01-15 10:00:05,012] {taskinstance.py:1345} INFO - Marking task as SUCCESS
```

#### Log Levels

```python
import logging

def my_task(**context):
    logging.debug("Detailed debug info")    # Usually filtered out
    logging.info("Normal operation info")    # Standard level
    logging.warning("Something unexpected")  # Potential issues
    logging.error("Something failed")        # Errors
    logging.critical("System is down")       # Critical failures
```

### Remote Logging

For production, store logs externally:

```ini
# airflow.cfg
[logging]
remote_logging = True
remote_base_log_folder = s3://my-bucket/airflow-logs
remote_log_conn_id = aws_default
```

Supported backends:
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- Elasticsearch

### SLAs (Service Level Agreements)

SLAs define expected completion times:

```python
from datetime import timedelta

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when SLA is missed."""
    import logging
    logging.error(f"SLA missed for tasks: {task_list}")
    # Send alert, page on-call, etc.

with DAG(
    dag_id="sla_monitored_dag",
    sla_miss_callback=sla_miss_callback,
    ...
) as dag:
    
    # Task-level SLA
    critical_task = PythonOperator(
        task_id="critical_extraction",
        sla=timedelta(hours=2),  # Must complete within 2 hours of schedule
        python_callable=extract_critical_data
    )
    
    normal_task = PythonOperator(
        task_id="normal_processing",
        sla=timedelta(hours=4),
        python_callable=process_data
    )
```

SLA misses are:
- Logged in the database
- Visible in Browse -> SLA Misses
- Triggerable via callbacks

### Email Alerts

#### Configuration

```ini
# airflow.cfg
[smtp]
smtp_host = smtp.gmail.com
smtp_port = 587
smtp_starttls = True
smtp_user = your-email@gmail.com
smtp_password = your-app-password
smtp_mail_from = airflow@yourdomain.com
```

#### Task-Level Alerts

```python
default_args = {
    "owner": "data_team",
    "email": ["team@company.com", "oncall@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": False  # Usually too noisy
}

with DAG(dag_id="alerting_dag", default_args=default_args, ...) as dag:
    ...
```

#### Custom Email Content

```python
from airflow.operators.email import EmailOperator

send_report = EmailOperator(
    task_id="send_daily_report",
    to=["stakeholders@company.com"],
    subject="Daily Pipeline Report - {{ ds }}",
    html_content="""
    <h2>Pipeline Complete</h2>
    <p>Date: {{ ds }}</p>
    <p>Records processed: {{ ti.xcom_pull(task_ids='process')['count'] }}</p>
    """
)
```

### Slack Alerts

Using the Slack provider:

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def task_fail_slack_alert(context):
    """Send Slack notification on task failure."""
    slack_msg = f"""
        :red_circle: Task Failed
        *Task*: {context.get('task_instance').task_id}
        *DAG*: {context.get('task_instance').dag_id}
        *Execution Date*: {context.get('execution_date')}
        *Log URL*: {context.get('task_instance').log_url}
    """
    
    alert = SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook',
        message=slack_msg
    )
    
    return alert.execute(context=context)

default_args = {
    "on_failure_callback": task_fail_slack_alert
}
```

### Callbacks

Customize behavior at different lifecycle points:

```python
def on_success(context):
    print(f"Task {context['task_instance'].task_id} succeeded!")

def on_failure(context):
    print(f"Task {context['task_instance'].task_id} failed!")
    # Send PagerDuty alert, create Jira ticket, etc.

def on_retry(context):
    print(f"Task {context['task_instance'].task_id} is retrying...")

default_args = {
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
    "on_retry_callback": on_retry
}
```

DAG-level callbacks:

```python
def dag_success_callback(context):
    print("Entire DAG completed successfully!")

with DAG(
    dag_id="callback_dag",
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
    ...
) as dag:
    ...
```

### Metrics and Monitoring Tools

#### StatsD Integration

```ini
# airflow.cfg
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
```

Common metrics:
- `airflow.scheduler.scheduler_loop_count`
- `airflow.dag.{dag_id}.{task_id}.duration`
- `airflow.pool.{pool_name}.open_slots`
- `airflow.executor.queued_tasks`

#### Prometheus + Grafana

Export metrics for Prometheus:

```bash
pip install apache-airflow[statsd]
```

Use `statsd_exporter` to convert StatsD to Prometheus format.

Example Grafana dashboard metrics:
- DAG success rate over time
- Task duration trends
- Scheduler heartbeat
- Worker utilization

#### DataDog Integration

```python
# datadog_callback.py
from datadog import DogStatsd

statsd = DogStatsd()

def datadog_success_callback(context):
    statsd.increment('airflow.task.success', tags=[
        f"dag:{context['dag'].dag_id}",
        f"task:{context['task'].task_id}"
    ])

def datadog_failure_callback(context):
    statsd.increment('airflow.task.failure', tags=[
        f"dag:{context['dag'].dag_id}",
        f"task:{context['task'].task_id}"
    ])
```

### Health Checks

Monitor Airflow components:

```bash
# Scheduler health
curl http://localhost:8080/health

# Response
{
  "metadatabase": {"status": "healthy"},
  "scheduler": {"status": "healthy", "latest_scheduler_heartbeat": "2024-01-15T10:00:00"}
}
```

### Complete Monitoring Setup

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import logging

def slack_failure_alert(context):
    """Send detailed Slack alert on failure."""
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    
    message = f"""
:red_circle: *Pipeline Failure Alert*

*DAG:* {task_instance.dag_id}
*Task:* {task_instance.task_id}
*Execution Date:* {context.get('ds')}
*Try Number:* {task_instance.try_number}

*Error:*
```{str(exception)[:500]}```

<{task_instance.log_url}|View Logs>
    """
    
    alert = SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id='slack_webhook',
        message=message
    )
    alert.execute(context=context)

def slack_sla_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Alert on SLA miss."""
    message = f"""
:warning: *SLA Miss Alert*

*DAG:* {dag.dag_id}
*Tasks:* {', '.join([t.task_id for t in task_list])}
*SLA:* {slas}
    """
    # Send to Slack

def extract(**context):
    logging.info("Starting extraction...")
    # Extraction logic
    logging.info("Extraction complete: 1000 records")
    return {"records": 1000}

def transform(**context):
    logging.info("Transforming data...")
    return {"status": "complete"}

default_args = {
    "owner": "data_team",
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "on_failure_callback": slack_failure_alert,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="monitored_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    sla_miss_callback=slack_sla_alert,
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract",
        sla=timedelta(hours=1),
        python_callable=extract
    )
    
    transform_task = PythonOperator(
        task_id="transform",
        sla=timedelta(hours=2),
        python_callable=transform
    )
    
    extract_task >> transform_task
```

## Summary

- Access task logs via UI or remote storage (S3, GCS)
- SLAs define expected completion times with miss callbacks
- Email alerts are built-in; configure SMTP settings
- Integrate Slack, PagerDuty, or other tools via callbacks
- StatsD and Prometheus enable dashboards and trend analysis
- Use health endpoints to monitor Airflow components

## Additional Resources

- [Airflow Logging](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-architecture.html)
- [Airflow Metrics](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html)
- [Astronomer Alerting Guide](https://docs.astronomer.io/learn/error-notifications-in-airflow)
