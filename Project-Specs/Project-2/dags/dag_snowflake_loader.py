"""
StreamFlow Phase 2 DAG - Loads Gold Zone CSVs into Snowflake Bronze tables.

Prerequisites:
    1. Configure Airflow Connection 'snowflake_default' in Admin → Connections
    2. Create CSV_STAGE and CSV_FORMAT in Snowflake BRONZE schema
    3. Create Bronze tables (raw_user_events, raw_transactions, etc.)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os
import glob

# Path where Spark ETL writes Gold Zone CSVs (shared Docker volume)
GOLD_ZONE_PATH = '/opt/spark-data/gold'

# Maps CSV file patterns to their corresponding Bronze table names
CSV_TO_TABLE = {
    'user_events*.csv': 'raw_user_events',
    'transactions*.csv': 'raw_transactions',
    'products*.csv': 'raw_products',
    'customers*.csv': 'raw_customers',
}


def load_to_snowflake(**context):
    """Upload Gold Zone CSVs to Snowflake Bronze tables."""
    
    # SnowflakeHook reads connection details from Airflow Connection 'snowflake_default'
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    for pattern, table in CSV_TO_TABLE.items():
        # Find all CSVs matching this pattern (e.g., user_events_001.csv, user_events_002.csv)
        for csv_file in glob.glob(os.path.join(GOLD_ZONE_PATH, pattern)):
            # PUT uploads local file to Snowflake internal stage
            cursor.execute(f"PUT file://{csv_file} @CSV_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        
        # COPY INTO loads staged files into the Bronze table
        cursor.execute(f"""
            COPY INTO {table}
            FROM @CSV_STAGE
            FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
            ON_ERROR = 'CONTINUE'
        """)
    
    # Clean up stage after loading
    cursor.execute("REMOVE @CSV_STAGE")
    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id='streamflow_warehouse',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manually triggered
    catchup=False,
) as dag:
    
    # Single task: load all Gold Zone CSVs to Snowflake Bronze
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )
