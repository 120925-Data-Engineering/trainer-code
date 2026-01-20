"""
StreamFlow Phase 2 DAG - Loads Gold Zone CSVs into Snowflake Bronze tables.
Requires Airflow Connection 'snowflake_default' configured in Admin → Connections.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import glob
import snowflake.connector

GOLD_ZONE_PATH = '/opt/spark-data/gold'

CSV_TO_TABLE = {
    'user_events*.csv': 'raw_user_events',
    'transactions*.csv': 'raw_transactions',
    'products*.csv': 'raw_products',
    'customers*.csv': 'raw_customers',
}


def load_to_snowflake(**context):
    """Upload Gold Zone CSVs to Snowflake Bronze tables."""
    # Get connection from Airflow
    conn_info = BaseHook.get_connection('snowflake_default')
    extra = conn_info.extra_dejson or {}
    
    conn = snowflake.connector.connect(
        account=conn_info.host,
        user=conn_info.login,
        password=conn_info.password,
        warehouse=extra.get('warehouse', 'STREAMFLOW_WH'),
        database=extra.get('database', 'STREAMFLOW_DW'),
        schema=extra.get('schema', 'BRONZE'),
    )
    cursor = conn.cursor()
    
    # Upload and copy each CSV pattern
    for pattern, table in CSV_TO_TABLE.items():
        for csv_file in glob.glob(os.path.join(GOLD_ZONE_PATH, pattern)):
            cursor.execute(f"PUT file://{csv_file} @CSV_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        
        cursor.execute(f"""
            COPY INTO {table}
            FROM @CSV_STAGE
            FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
            ON_ERROR = 'CONTINUE'
        """)
    
    cursor.execute("REMOVE @CSV_STAGE")
    conn.commit()
    cursor.close()
    conn.close()


with DAG(
    dag_id='streamflow_warehouse',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )
