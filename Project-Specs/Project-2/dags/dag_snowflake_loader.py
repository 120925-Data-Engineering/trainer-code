"""
StreamFlow Phase 2 DAG - Loads Gold Zone CSVs into Snowflake Bronze tables.

Prerequisites:
    1. Configure Airflow Connection 'snowflake_default' in Admin → Connections
    2. Create CSV_STAGE in Snowflake BRONZE schema:
       Internal stage (temporary cloud storage for file uploads).
       Snowflake cannot load local files directly - files must first be
       uploaded to a stage, then copied into tables.
       Example: CREATE STAGE CSV_STAGE;
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
    
    # Explicitly set database and schema context (adjust these to match your setup)
    # This ensures stage and table references resolve correctly
    cursor.execute("USE DATABASE STREAMFLOW")
    cursor.execute("USE SCHEMA BRONZE")
    
    # Debug: List files in Gold Zone directory
    print(f"[DEBUG] Looking for CSVs in: {GOLD_ZONE_PATH}")
    print(f"[DEBUG] Directory exists: {os.path.exists(GOLD_ZONE_PATH)}")
    if os.path.exists(GOLD_ZONE_PATH):
        print(f"[DEBUG] Directory contents: {os.listdir(GOLD_ZONE_PATH)}")
    
    for pattern, table in CSV_TO_TABLE.items():
        # Find all CSVs matching this pattern (e.g., user_events_001.csv, user_events_002.csv)
        matched_files = glob.glob(os.path.join(GOLD_ZONE_PATH, pattern))
        print(f"[DEBUG] Pattern '{pattern}' matched {len(matched_files)} files: {matched_files}")
        
        for csv_file in matched_files:
            # PUT uploads local file to Snowflake internal stage
            # Use fully-qualified stage name: DATABASE.SCHEMA.STAGE
            put_cmd = f"PUT file://{csv_file} @STREAMFLOW.BRONZE.CSV_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            print(f"[DEBUG] Executing: {put_cmd}")
            cursor.execute(put_cmd)
            print(f"[DEBUG] PUT result: {cursor.fetchall()}")
        
        # Debug: List staged files
        cursor.execute("LIST @STREAMFLOW.BRONZE.CSV_STAGE")
        staged_files = cursor.fetchall()
        print(f"[DEBUG] Files in stage after PUT: {staged_files}")
        
        # COPY INTO loads staged files into the Bronze table (inline CSV format)
        cursor.execute(f"""
            COPY INTO {table}
            FROM @STREAMFLOW.BRONZE.CSV_STAGE
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """)
    
    # Clean up staged files (REMOVE deletes files only, not the stage itself - stage persists for reuse)
    cursor.execute("REMOVE @STREAMFLOW.BRONZE.CSV_STAGE")
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
