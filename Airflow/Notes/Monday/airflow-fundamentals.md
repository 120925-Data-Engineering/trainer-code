# Airflow Fundamentals

## Learning Objectives
- Define what Apache Airflow is and its role in modern data engineering
- Understand how Airflow differs from cron jobs and other schedulers
- Identify key use cases where Airflow excels
- Recognize Airflow's position in the data engineering ecosystem

## Why This Matters

In Weeks 1-3, you built individual data processing capabilities: Spark for batch processing, Kafka for real-time streaming. But real-world data pipelines are not isolated---they are interconnected workflows where the output of one job becomes the input of another, where failures must be handled gracefully, and where timing and dependencies matter.

**Apache Airflow** is the conductor that orchestrates this symphony. Understanding its fundamentals is essential for building reliable, maintainable data pipelines that run in production.

## The Concept

### What is Apache Airflow?

Apache Airflow is an **open-source workflow orchestration platform** that allows you to programmatically author, schedule, and monitor workflows.

Key characteristics:
- **Workflows as Code**: Define pipelines in Python, not XML or JSON
- **Dynamic**: Generate pipelines dynamically using Python logic
- **Extensible**: Rich plugin ecosystem for integrating with any system
- **Scalable**: From single-machine to distributed across clusters

### The Problem Airflow Solves

Consider a typical data pipeline:

```
Extract data from API
       |
       v
Load into staging table
       |
       v
Transform with Spark job
       |
       v
Load into data warehouse
       |
       v
Generate reports
       |
       v
Send email notifications
```

Without Airflow, you might use:
- Cron jobs for scheduling
- Shell scripts for sequencing
- Manual monitoring for failures

This approach leads to:
- No visibility into pipeline state
- Difficult failure handling
- Complex dependency management
- No retry logic
- Scattered logs

### Airflow vs. Cron

| Feature | Cron | Airflow |
|---------|------|---------|
| Scheduling | Time-based only | Time-based + dependencies |
| Dependencies | Manual scripting | Built-in operators |
| Retries | Manual implementation | Configurable per task |
| Monitoring | Log files | Web UI dashboard |
| Backfills | Manual | Automatic catchup |
| Alerting | External tools | Built-in |
| Parallelism | Process-level | Task-level with pools |

### Airflow vs. Other Orchestrators

| Tool | Best For | Limitations |
|------|----------|-------------|
| **Airflow** | Batch workflows, ETL | Not ideal for streaming |
| **Luigi** | Simple pipelines | Less active community |
| **Prefect** | Modern Python workflows | Newer, smaller ecosystem |
| **Dagster** | Software-defined assets | Steeper learning curve |
| **Argo** | Kubernetes-native | Requires K8s expertise |

Airflow remains the industry standard due to its maturity, extensive integrations, and large community.

### Core Principles

#### 1. Workflows as Python Code

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extracting data...")

def transform():
    print("Transforming data...")

def load():
    print("Loading data...")

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
) as dag:
    
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
    
    extract_task >> transform_task >> load_task
```

#### 2. Idempotency

Tasks should produce the same result whether run once or multiple times. This enables safe retries and backfills.

#### 3. Explicit Dependencies

Dependencies between tasks are explicitly defined, not inferred:

```python
# Task B runs after Task A
task_a >> task_b

# Task C runs after both A and B complete
[task_a, task_b] >> task_c
```

#### 4. Separation of Orchestration and Execution

Airflow orchestrates **when** and **in what order** tasks run. The actual computation often happens externally (Spark cluster, database, cloud service).

### Key Use Cases

#### 1. ETL/ELT Pipelines
Extract data from sources, transform it, and load into a data warehouse.

#### 2. Machine Learning Pipelines
Orchestrate training, validation, and deployment of ML models.

#### 3. Data Quality Checks
Run validation tasks and alert on data anomalies.

#### 4. Report Generation
Schedule and distribute business reports.

#### 5. Infrastructure Management
Automate cloud resource provisioning and cleanup.

### Where Airflow Fits in Your Stack

```
+------------------+
|   Data Sources   |  (APIs, Databases, Files)
+------------------+
         |
         v
+------------------+
|     Airflow      |  (Orchestration Layer)
+------------------+
         |
    +----+----+
    |    |    |
    v    v    v
+------+ +------+ +------+
| Spark| | Kafka| |  DBT |  (Processing Tools)
+------+ +------+ +------+
         |
         v
+------------------+
|   Data Warehouse |  (Snowflake, BigQuery, etc.)
+------------------+
```

## Summary

- Apache Airflow is a workflow orchestration platform for defining pipelines as code
- It solves the problems of dependency management, monitoring, and failure handling
- Airflow excels at batch-oriented workflows with complex dependencies
- Key principles include idempotency, explicit dependencies, and separation of concerns
- Airflow integrates with the tools you already know (Spark, Kafka) to orchestrate end-to-end pipelines

## Additional Resources

- [Apache Airflow Overview](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [The Rise of Airflow - Data Engineering Podcast](https://www.dataengineeringpodcast.com/)
