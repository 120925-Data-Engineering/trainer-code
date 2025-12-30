"""
Kafka Connections Demo DAG
==========================
Demonstrates using Airflow Connections to interact with Kafka.
Shows both direct Kafka operators and the BashOperator batch pattern.

Prerequisites:
- Create a connection named 'kafka_default' in Admin -> Connections
  - Conn Type: Apache Kafka (or Generic if Kafka not available)
  - Extra (JSON): {"bootstrap.servers": "kafka:9092", "group.id": "airflow_consumer_group"}
- Kafka service running in the stack
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import json


def get_kafka_connection_details(**context):
    """
    Demonstrate accessing Kafka connection details with BaseHook.
    This is useful when you need to pass broker config to custom scripts or libraries.
    """
    print("Accessing Kafka connection with BaseHook...")
    
    try:
        conn = BaseHook.get_connection("kafka_default")
        
        print(f"\nConnection ID: {conn.conn_id}")
        print(f"Connection Type: {conn.conn_type}")
        
        # Kafka config is stored in the 'extra' field as JSON
        if conn.extra:
            extra = conn.extra_dejson
            print("\nKafka Configuration:")
            for key, value in extra.items():
                print(f"  {key}: {value}")
            
            # Return for XCom - other tasks can use this
            return extra
        else:
            print("No extra configuration found in connection")
            return {}
            
    except Exception as e:
        print(f"Connection 'kafka_default' not found: {e}")
        print("Please create the connection in Admin -> Connections")
        return {}


def build_consumer_command(**context):
    """
    Build a dynamic consumer command using connection details.
    This shows how to use connection info in your tasks.
    """
    ti = context['ti']
    kafka_config = ti.xcom_pull(task_ids='get_kafka_config') or {}
    
    bootstrap_servers = kafka_config.get('bootstrap.servers', 'kafka:9092')
    group_id = kafka_config.get('group.id', 'airflow_default_group')
    
    # Build the command that would be used in a real consumer script
    command = f"""
    kafka-console-consumer \\
        --bootstrap-server {bootstrap_servers} \\
        --group {group_id} \\
        --topic test_topic \\
        --timeout-ms 5000
    """
    
    print("Generated consumer command:")
    print(command)
    
    return {"command": command, "bootstrap_servers": bootstrap_servers}


def list_kafka_topics(**context):
    """
    Use Python to list Kafka topics (alternative to BashOperator).
    Shows how you might use kafka-python library with Airflow connections.
    """
    ti = context['ti']
    kafka_config = ti.xcom_pull(task_ids='get_kafka_config') or {}
    bootstrap_servers = kafka_config.get('bootstrap.servers', 'kafka:9092')
    
    print(f"Would connect to Kafka at: {bootstrap_servers}")
    print("In a real scenario, you would use kafka-python or confluent-kafka here:")
    print("""
    from kafka.admin import KafkaAdminClient
    
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin.list_topics()
    """)
    
    return {"status": "topics_listed", "brokers": bootstrap_servers}


# DAG Definition
with DAG(
    dag_id="kafka_connections_demo",
    description="Demonstrates Connections and Hooks for Kafka integration",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "connections", "kafka"],
) as dag:
    
    # Task 1: Get Kafka connection details using BaseHook
    get_config_task = PythonOperator(
        task_id="get_kafka_config",
        python_callable=get_kafka_connection_details
    )
    
    # Task 2: Show how to build dynamic commands with connection info
    build_command_task = PythonOperator(
        task_id="build_consumer_command",
        python_callable=build_consumer_command
    )
    
    # Task 3: BashOperator pattern - list topics using CLI
    # This demonstrates the "batch pattern" mentioned in the instructor guide
    list_topics_bash = BashOperator(
        task_id="list_topics_cli",
        bash_command="""
            echo "Listing Kafka topics using CLI..."
            docker exec pipeline-kafka kafka-topics --bootstrap-server localhost:9092 --list || echo "Kafka not accessible from this container"
        """,
        # In production, you'd use the connection details:
        # bash_command='kafka-topics --bootstrap-server {{ ti.xcom_pull(task_ids="get_kafka_config")["bootstrap.servers"] }} --list'
    )
    
    # Task 4: Python-based topic listing (for comparison)
    list_topics_python = PythonOperator(
        task_id="list_topics_python",
        python_callable=list_kafka_topics
    )
    
    # Dependencies: Get config first, then parallel operations
    get_config_task >> [build_command_task, list_topics_bash, list_topics_python]


dag.doc_md = """
## Kafka Connections Demo

This DAG demonstrates how to use Airflow Connections with Kafka.

### Prerequisites:
1. Create a connection in Admin -> Connections:
   - **Conn Id:** `kafka_default`
   - **Conn Type:** Apache Kafka (or Generic)
   - **Extra (JSON):**
   ```json
   {
     "bootstrap.servers": "kafka:9092",
     "group.id": "airflow_consumer_group",
     "auto.offset.reset": "earliest"
   }
   ```

### Tasks:
1. **get_kafka_config** - Uses BaseHook to get connection details
2. **build_consumer_command** - Shows how to use config in dynamic commands
3. **list_topics_cli** - BashOperator pattern for Kafka CLI
4. **list_topics_python** - Python approach with connection info

### Key Patterns:
- **BaseHook.get_connection()** - Access raw connection details
- **XCom** - Share connection config between tasks
- **BashOperator** - Execute Kafka CLI commands (batch pattern)
"""
