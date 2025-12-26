# Connections and Hooks

## Learning Objectives
- Create and manage Airflow Connections for external systems
- Understand the role of Hooks as interfaces to external services
- Use Connections and Hooks securely in your DAGs
- Implement common patterns for database and API integrations

## Why This Matters

Real-world pipelines interact with databases, APIs, cloud services, and more. **Connections** store credentials securely, while **Hooks** provide Pythonic interfaces to use those credentials. Understanding this pattern is essential for building integrations that are both secure and maintainable.

## The Concept

### What is a Connection?

A **Connection** is Airflow's way of storing credentials and connection parameters for external systems. They are stored in the metadata database (encrypted) and accessed by ID.

Connection components:
- **Conn Id**: Unique identifier (e.g., `my_postgres_db`)
- **Conn Type**: Type of connection (Postgres, AWS, HTTP, etc.)
- **Host**: Server address
- **Schema**: Database name or path
- **Login**: Username
- **Password**: Password (encrypted)
- **Port**: Connection port
- **Extra**: JSON for additional parameters

### Creating Connections

#### Via the Airflow UI

1. Go to Admin -> Connections
2. Click "+" to add a new connection
3. Fill in the fields:
   - **Connection Id**: `my_postgres`
   - **Connection Type**: Postgres
   - **Host**: `db.example.com`
   - **Schema**: `analytics`
   - **Login**: `airflow_user`
   - **Password**: `secret123`
   - **Port**: `5432`

#### Via CLI

```bash
airflow connections add 'my_postgres' \
    --conn-type 'postgres' \
    --conn-host 'db.example.com' \
    --conn-schema 'analytics' \
    --conn-login 'airflow_user' \
    --conn-password 'secret123' \
    --conn-port 5432
```

#### Via Environment Variables

```bash
export AIRFLOW_CONN_MY_POSTGRES='postgresql://airflow_user:secret123@db.example.com:5432/analytics'
```

Format: `AIRFLOW_CONN_{CONN_ID_UPPERCASE}='connection_uri'`

### What is a Hook?

A **Hook** is a Python class that uses a Connection to interact with an external system. Hooks provide methods for common operations.

```
+------------+     +-----------+     +------------------+
| Connection |---->|   Hook    |---->| External System  |
| (creds)    |     | (methods) |     | (DB, API, etc.)  |
+------------+     +-----------+     +------------------+
```

### Common Hooks

#### PostgresHook

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

def query_database():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    
    # Execute a query and get results
    records = hook.get_records("SELECT * FROM users LIMIT 10")
    
    # Execute and get a pandas DataFrame
    df = hook.get_pandas_df("SELECT * FROM orders")
    
    # Execute a command
    hook.run("INSERT INTO logs VALUES ('new entry')")
    
    return records
```

#### S3Hook (AWS)

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def download_from_s3():
    hook = S3Hook(aws_conn_id="aws_default")
    
    # Download a file
    local_path = hook.download_file(
        key="data/input.csv",
        bucket_name="my-bucket",
        local_path="/tmp/"
    )
    
    # List bucket contents
    keys = hook.list_keys(bucket_name="my-bucket", prefix="data/")
    
    # Check if file exists
    exists = hook.check_for_key(key="data/important.csv", bucket_name="my-bucket")
    
    return local_path
```

#### HttpHook

```python
from airflow.providers.http.hooks.http import HttpHook

def call_api():
    hook = HttpHook(http_conn_id="my_api", method="GET")
    
    response = hook.run(
        endpoint="/users",
        headers={"Authorization": "Bearer token123"}
    )
    
    return response.json()
```

### Using Hooks in Operators

Many operators use hooks internally:

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

# The operator uses PostgresHook internally
query_task = PostgresOperator(
    task_id="run_query",
    postgres_conn_id="my_postgres",
    sql="SELECT COUNT(*) FROM orders WHERE date = '{{ ds }}'"
)
```

### Accessing Connection Details

Sometimes you need raw connection details:

```python
from airflow.hooks.base import BaseHook

def get_connection_info():
    conn = BaseHook.get_connection("my_postgres")
    
    print(f"Host: {conn.host}")
    print(f"Schema: {conn.schema}")
    print(f"Login: {conn.login}")
    print(f"Password: {conn.password}")  # Decrypted
    print(f"Port: {conn.port}")
    print(f"Extra: {conn.extra_dejson}")  # Parsed JSON
    
    # Build a connection string
    conn_string = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    
    return conn_string
```

### Extra Parameters

The `Extra` field stores JSON for additional configuration:

```json
{
    "sslmode": "require",
    "connect_timeout": 10,
    "application_name": "airflow_etl"
}
```

Access in code:

```python
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection("my_postgres")
extras = conn.extra_dejson

sslmode = extras.get("sslmode", "prefer")
```

### Secrets Backends

For production, consider external secrets management:

```python
# airflow.cfg
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"connections_path": "airflow/connections", "url": "http://vault:8200"}
```

Supported backends:
- HashiCorp Vault
- AWS Secrets Manager
- Google Cloud Secret Manager
- Azure Key Vault

### Security Best Practices

#### 1. Never Hardcode Credentials

```python
# BAD: Credentials in code
conn = psycopg2.connect(password="secret123")

# GOOD: Use connections
hook = PostgresHook(postgres_conn_id="my_postgres")
```

#### 2. Use Least Privilege

Create database users with minimal permissions:

```sql
CREATE USER airflow_reader WITH PASSWORD 'xxx';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airflow_reader;
```

#### 3. Rotate Credentials

Regularly update passwords and access keys:

```bash
# Update via CLI
airflow connections add 'my_postgres' \
    --conn-password 'new_password' \
    --conn-uri-overwrite
```

#### 4. Audit Connection Usage

Monitor which DAGs use which connections:

```python
# Log connection usage for auditing
import logging

def my_task():
    logging.info("Accessing my_postgres connection")
    hook = PostgresHook(postgres_conn_id="my_postgres")
    ...
```

### Complete Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def extract_from_db(**context):
    """Extract data from PostgreSQL."""
    hook = PostgresHook(postgres_conn_id="source_db")
    df = hook.get_pandas_df("SELECT * FROM orders WHERE date = %(ds)s", parameters={"ds": context["ds"]})
    
    # Save to temp location
    df.to_csv("/tmp/orders.csv", index=False)
    return len(df)

def upload_to_s3(**context):
    """Upload extracted data to S3."""
    hook = S3Hook(aws_conn_id="aws_default")
    
    ds = context["ds"]
    hook.load_file(
        filename="/tmp/orders.csv",
        key=f"raw/orders/{ds}/orders.csv",
        bucket_name="data-lake-bucket",
        replace=True
    )

with DAG(
    dag_id="db_to_s3_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
) as dag:
    
    extract = PythonOperator(
        task_id="extract_from_db",
        python_callable=extract_from_db
    )
    
    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )
    
    extract >> upload
```

## Summary

- **Connections** store credentials securely in Airflow's metadata database
- **Hooks** provide Pythonic interfaces to use connections
- Create connections via UI, CLI, or environment variables
- Never hardcode credentials---always use connections
- Use secrets backends (Vault, AWS Secrets Manager) for production
- Common hooks include PostgresHook, S3Hook, and HttpHook

## Additional Resources

- [Managing Connections - Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Hooks and Operators](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/operators.html)
- [Secrets Backend Configuration](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html)
