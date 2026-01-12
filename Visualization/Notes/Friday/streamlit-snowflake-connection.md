# Connecting Streamlit to Snowflake

## Learning Objectives
- Install and configure the Snowflake connector
- Connect Streamlit to your Snowflake data warehouse
- Use caching to optimize query performance
- Manage credentials securely

## Why This Matters

Your Week 5 Snowflake data warehouse contains valuable data. Streamlit lets you build custom applications that query this data in real-time, creating dashboards that complement your Power BI reports.

## Installation

Install the Snowflake connector:

```bash
pip install snowflake-connector-python
```

For pandas integration:
```bash
pip install "snowflake-connector-python[pandas]"
```

## Secrets Configuration

Store credentials in `.streamlit/secrets.toml`:

```toml
[snowflake]
account = "xy12345.us-east-1"
user = "your_username"
password = "your_password"
role = "ANALYST_ROLE"
warehouse = "COMPUTE_WH"
database = "ANALYTICS"
schema = "GOLD"
```

**Never commit this file to version control.**

## Basic Connection

```python
import streamlit as st
import snowflake.connector
import pandas as pd

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        role=st.secrets["snowflake"]["role"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

conn = get_connection()
```

## Querying Data

### Basic Query

```python
@st.cache_data(ttl=600)
def run_query(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

data = run_query("SELECT * FROM dim_product LIMIT 100")
```

### Query to DataFrame

```python
@st.cache_data(ttl=600)
def get_dataframe(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

df = get_dataframe("SELECT * FROM fact_sales WHERE year = 2023")
st.dataframe(df)
```

## Caching Strategies

### @st.cache_data

Cache query results (data that does not change frequently):

```python
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_sales_summary():
    return get_dataframe("SELECT region, SUM(amount) FROM fact_sales GROUP BY region")
```

### @st.cache_resource

Cache connections and expensive resources:

```python
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(...)
```

### TTL (Time To Live)

```python
@st.cache_data(ttl=300)  # 5 minutes
@st.cache_data(ttl="1h")  # 1 hour
@st.cache_data(ttl="1d")  # 1 day
```

## Complete Example

```python
import streamlit as st
import snowflake.connector
import pandas as pd

st.title("Snowflake Dashboard")

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

@st.cache_data(ttl=600)
def get_regions():
    conn = get_connection()
    return pd.read_sql("SELECT DISTINCT region FROM dim_store", conn)

@st.cache_data(ttl=600)
def get_sales(region):
    conn = get_connection()
    query = f"SELECT * FROM fact_sales WHERE region = '{region}'"
    return pd.read_sql(query, conn)

# Sidebar filter
regions = get_regions()["REGION"].tolist()
selected = st.sidebar.selectbox("Region", regions)

# Display data
df = get_sales(selected)
st.metric("Total Sales", f"${df['AMOUNT'].sum():,.0f}")
st.dataframe(df)
```

## Environment Variables (Alternative)

For deployment, use environment variables:

```python
import os

conn = snowflake.connector.connect(
    account=os.environ.get("SNOWFLAKE_ACCOUNT"),
    user=os.environ.get("SNOWFLAKE_USER"),
    password=os.environ.get("SNOWFLAKE_PASSWORD"),
    ...
)
```

## Summary

- Use `snowflake-connector-python` for Snowflake connectivity
- Store credentials in `.streamlit/secrets.toml`
- Cache connections with `@st.cache_resource`
- Cache query results with `@st.cache_data` and appropriate TTL
- Use environment variables for production deployments

## Additional Resources

- [Snowflake Connector](https://docs.snowflake.com/en/user-guide/python-connector) - Official docs
- [Streamlit Secrets](https://docs.streamlit.io/library/advanced-features/secrets-management) - Secrets management
