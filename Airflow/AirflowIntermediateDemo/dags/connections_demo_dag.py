"""
Connections and Hooks Demo DAG
==============================
Demonstrates using Airflow Connections and Hooks to
interact with external systems securely.

Prerequisites:
- Create a connection named 'demo_postgres' in Admin -> Connections
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime


def query_airflow_metadata(**context):
    """
    Query Airflow's own metadata database using PostgresHook.
    This shows how Hooks use Connections to access databases.
    """
    print("Connecting to database using PostgresHook...")
    
    # The Hook reads credentials from the Connection
    hook = PostgresHook(postgres_conn_id="demo_postgres")
    
    # Get records as a list of tuples
    print("\n--- Recent DAG Runs ---")
    records = hook.get_records("""
        SELECT dag_id, run_id, state, start_date 
        FROM dag_run 
        ORDER BY start_date DESC 
        LIMIT 5
    """)
    
    for record in records:
        print(f"  DAG: {record[0]}, State: {record[2]}")
    
    return {"records_found": len(records)}


def show_connection_details(**context):
    """
    Demonstrate accessing raw connection details with BaseHook.
    Useful when you need to pass credentials to other libraries.
    """
    print("Accessing connection details with BaseHook...")
    
    conn = BaseHook.get_connection("demo_postgres")
    
    # These values come from the Connection we created in the UI
    print(f"\nConnection ID: {conn.conn_id}")
    print(f"Connection Type: {conn.conn_type}")
    print(f"Host: {conn.host}")
    print(f"Schema: {conn.schema}")
    print(f"Login: {conn.login}")
    print(f"Port: {conn.port}")
    # Note: Password is available as conn.password (decrypted)
    # We don't print it for security
    print("Password: [hidden for security]")
    
    # Extra contains additional JSON configuration
    if conn.extra:
        print(f"Extra: {conn.extra_dejson}")


def count_tasks(**context):
    """
    Another example of using the Hook for queries.
    """
    hook = PostgresHook(postgres_conn_id="demo_postgres")
    
    # get_first returns just the first row
    result = hook.get_first("""
        SELECT COUNT(*) FROM task_instance
    """)
    
    task_count = result[0] if result else 0
    print(f"\nTotal task instances in database: {task_count}")
    
    return {"task_count": task_count}


# DAG Definition
with DAG(
    dag_id="connections_demo",
    description="Demonstrates Connections and Hooks for external system access",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "connections", "hooks"],
) as dag:
    
    query_task = PythonOperator(
        task_id="query_metadata",
        python_callable=query_airflow_metadata
    )
    
    show_conn_task = PythonOperator(
        task_id="show_connection",
        python_callable=show_connection_details
    )
    
    count_task = PythonOperator(
        task_id="count_tasks",
        python_callable=count_tasks
    )
    
    # Run in sequence
    query_task >> show_conn_task >> count_task


dag.doc_md = """
## Connections Demo

This DAG demonstrates how to use Airflow Connections and Hooks.

### Prerequisites:
1. Create a connection in Admin -> Connections:
   - Conn Id: `demo_postgres`
   - Conn Type: Postgres
   - Host: `postgres`
   - Schema: `airflow`
   - Login: `airflow`
   - Password: `airflow`
   - Port: `5432`

### Tasks:
1. **query_metadata** - Queries the database using PostgresHook
2. **show_connection** - Shows how to access raw connection details
3. **count_tasks** - Another query example
"""
