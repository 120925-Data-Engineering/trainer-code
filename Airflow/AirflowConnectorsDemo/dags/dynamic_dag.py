"""
Dynamic DAG Generation Demo
============================
Demonstrates generating tasks dynamically from a configuration file.

Features:
- Reads pipeline definitions from YAML
- Creates TaskGroups for each pipeline
- Each pipeline has extract, transform, load tasks
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from pathlib import Path
import yaml
import time


# ============================================================
# Load Configuration at Parse Time
# ============================================================

CONFIG_PATH = Path(__file__).parent / "config" / "pipelines.yaml"

try:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
        PIPELINES = config.get("pipelines", [])
except FileNotFoundError:
    # Fallback if config file is missing
    PIPELINES = [
        {"name": "default", "source": "unknown", "destination": "unknown"}
    ]


# ============================================================
# Task Functions
# ============================================================

def extract_data(pipeline_name: str, source: str, **context):
    """Simulate data extraction from source."""
    print(f"[{pipeline_name}] Extracting from {source}...")
    time.sleep(1)
    
    # Simulate record count
    import random
    records = random.randint(100, 1000)
    print(f"[{pipeline_name}] Extracted {records} records")
    
    return {"pipeline": pipeline_name, "records": records}


def transform_data(pipeline_name: str, **context):
    """Simulate data transformation."""
    ti = context["ti"]
    
    # Get data from extract task within the same TaskGroup
    extract_result = ti.xcom_pull(
        task_ids=f"{pipeline_name}.extract"
    )
    
    print(f"[{pipeline_name}] Transforming data...")
    print(f"[{pipeline_name}] Input records: {extract_result['records']}")
    time.sleep(1)
    
    return {"pipeline": pipeline_name, "status": "transformed"}


def load_data(pipeline_name: str, destination: str, **context):
    """Simulate data loading to destination."""
    print(f"[{pipeline_name}] Loading to {destination}...")
    time.sleep(1)
    print(f"[{pipeline_name}] Load complete!")
    
    return {"pipeline": pipeline_name, "destination": destination}


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id="dynamic_etl",
    description="Dynamically generated ETL pipelines from YAML config",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "dynamic", "taskgroup"],
) as dag:
    
    # Start and end markers
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Generate a TaskGroup for each pipeline in the config
    for pipeline in PIPELINES:
        name = pipeline["name"]
        source = pipeline.get("source", "unknown")
        destination = pipeline.get("destination", "unknown")
        
        # TaskGroup creates a visual container in the UI
        with TaskGroup(group_id=name) as pipeline_group:
            
            extract = PythonOperator(
                task_id="extract",
                python_callable=extract_data,
                op_kwargs={
                    "pipeline_name": name,
                    "source": source
                }
            )
            
            transform = PythonOperator(
                task_id="transform",
                python_callable=transform_data,
                op_kwargs={
                    "pipeline_name": name
                }
            )
            
            load = PythonOperator(
                task_id="load",
                python_callable=load_data,
                op_kwargs={
                    "pipeline_name": name,
                    "destination": destination
                }
            )
            
            # Dependencies within the TaskGroup
            extract >> transform >> load
        
        # Connect TaskGroup to start and end
        start >> pipeline_group >> end


# Documentation
dag.doc_md = f"""
## Dynamic ETL DAG

This DAG is generated from configuration in `config/pipelines.yaml`.

### Current Pipelines:
{chr(10).join([f"- **{p['name']}**: {p.get('source', 'unknown')} -> {p.get('destination', 'unknown')}" for p in PIPELINES])}

### Adding a New Pipeline:
1. Edit `config/pipelines.yaml`
2. Add a new pipeline entry
3. The DAG will update automatically on next parse

### Structure:
Each pipeline is a TaskGroup containing:
- `extract` - Pull data from source
- `transform` - Process the data
- `load` - Push to destination
"""
