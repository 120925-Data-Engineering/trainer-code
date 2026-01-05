# dbt Use Cases

## Learning Objectives

- Apply dbt patterns to real-world data engineering scenarios
- Build data marts for business analytics
- Handle slowly changing dimensions (SCD) with dbt snapshots
- Deploy dbt in production pipelines

## Why This Matters

Understanding dbt concepts is valuable, but applying them to real business problems is where the tool demonstrates its power. This reading presents practical use cases that you will encounter in production environments, from building customer analytics to handling historical tracking. These patterns form the foundation of modern analytics engineering practice.

## The Concept

### Use Case 1: Building a Data Mart

A **data mart** is a focused subset of a data warehouse, optimized for a specific business domain (sales, marketing, finance).

**Scenario:** Build a sales data mart with customer, product, and order dimensions feeding into an orders fact table.

**Project Structure:**
```
models/
|-- staging/
|   |-- stg_orders.sql
|   |-- stg_customers.sql
|   |-- stg_products.sql
|
|-- marts/
    |-- sales/
        |-- dim_customers.sql
        |-- dim_products.sql
        |-- dim_date.sql
        |-- fct_orders.sql
        |-- sales.yml
```

**Staging Model (stg_orders.sql):**
```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
)

SELECT
    order_id,
    customer_id,
    product_id,
    order_date::DATE AS order_date,
    quantity,
    unit_price,
    (quantity * unit_price) AS line_total,
    created_at
FROM source
WHERE order_id IS NOT NULL
```

**Dimension Model (dim_customers.sql):**
```sql
{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(line_total) AS lifetime_value
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} AS customer_key,
    c.customer_id,
    c.customer_name,
    c.email,
    c.segment,
    c.country,
    o.first_order_date,
    o.last_order_date,
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.lifetime_value, 0) AS lifetime_value,
    CASE 
        WHEN o.last_order_date >= DATEADD('day', -90, CURRENT_DATE()) THEN 'Active'
        WHEN o.last_order_date >= DATEADD('day', -365, CURRENT_DATE()) THEN 'At Risk'
        ELSE 'Churned'
    END AS customer_status
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
```

**Fact Model (fct_orders.sql):**
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT customer_id, customer_key FROM {{ ref('dim_customers') }}
),

products AS (
    SELECT product_id, product_key FROM {{ ref('dim_products') }}
),

dates AS (
    SELECT date_day, date_key FROM {{ ref('dim_date') }}
)

SELECT
    o.order_id,
    d.date_key,
    c.customer_key,
    p.product_key,
    o.quantity,
    o.unit_price,
    o.line_total,
    o.created_at
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON o.product_id = p.product_id
LEFT JOIN dates d ON o.order_date = d.date_day
```

### Use Case 2: Slowly Changing Dimensions (SCD Type 2)

Track historical changes to dimension attributes using dbt **snapshots**.

**Scenario:** Customer addresses change over time. Track the full history for accurate historical reporting.

**Snapshot Definition:**
```sql
-- snapshots/customer_snapshot.sql
{% snapshot customer_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    customer_name,
    email,
    address,
    city,
    state,
    country,
    segment,
    updated_at
FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
```

**Run Snapshot:**
```bash
dbt snapshot
```

**Resulting Table (snapshots.customer_snapshot):**

| customer_id | address | dbt_valid_from | dbt_valid_to | dbt_scd_id |
|-------------|---------|----------------|--------------|------------|
| C001 | 123 Old St | 2024-01-01 | 2024-06-15 | abc123 |
| C001 | 456 New Ave | 2024-06-15 | NULL | def456 |

**Using Snapshots in Models:**
```sql
-- Get current customer record
SELECT *
FROM {{ ref('customer_snapshot') }}
WHERE dbt_valid_to IS NULL

-- Get customer record as of a specific date
SELECT *
FROM {{ ref('customer_snapshot') }}
WHERE '2024-03-15' BETWEEN dbt_valid_from AND COALESCE(dbt_valid_to, '9999-12-31')
```

### Use Case 3: Event Stream Processing

Process event data with incremental models for efficient aggregation.

**Scenario:** Aggregate user clickstream events into daily session metrics.

**Raw Events (stg_events.sql):**
```sql
SELECT
    event_id,
    user_id,
    session_id,
    event_type,
    event_timestamp,
    page_url,
    event_properties
FROM {{ source('analytics', 'events') }}
```

**Incremental Sessions (int_sessions.sql):**
```sql
{{ config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='merge'
) }}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    {% if is_incremental() %}
        WHERE event_timestamp >= DATEADD('hour', -6, (SELECT MAX(session_end) FROM {{ this }}))
    {% endif %}
)

SELECT
    session_id,
    user_id,
    MIN(event_timestamp) AS session_start,
    MAX(event_timestamp) AS session_end,
    COUNT(*) AS event_count,
    COUNT(DISTINCT page_url) AS pages_viewed,
    DATEDIFF('second', MIN(event_timestamp), MAX(event_timestamp)) AS session_duration_seconds
FROM events
GROUP BY session_id, user_id
```

**Daily Aggregation (fct_daily_sessions.sql):**
```sql
{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('day', session_start)::DATE AS session_date,
    COUNT(DISTINCT session_id) AS total_sessions,
    COUNT(DISTINCT user_id) AS unique_users,
    AVG(event_count) AS avg_events_per_session,
    AVG(session_duration_seconds) AS avg_session_duration,
    SUM(CASE WHEN pages_viewed > 1 THEN 1 ELSE 0 END) AS engaged_sessions
FROM {{ ref('int_sessions') }}
GROUP BY 1
```

### Use Case 4: Data Quality Monitoring

Build data quality checks into your dbt project.

**Quality Tests in YAML:**
```yaml
# models/marts/sales/sales.yml
version: 2

models:
  - name: fct_orders
    description: "Order fact table"
    tests:
      - dbt_utils.recency:
          datepart: day
          field: order_date
          interval: 2
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: line_total
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100000
      - name: customer_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_key
```

**Custom Quality Macro:**
```sql
-- macros/test_row_count_match.sql
{% test row_count_match(model, compare_model) %}

WITH source_count AS (
    SELECT COUNT(*) AS cnt FROM {{ model }}
),
compare_count AS (
    SELECT COUNT(*) AS cnt FROM {{ compare_model }}
)

SELECT source_count.cnt
FROM source_count, compare_count
WHERE source_count.cnt != compare_count.cnt

{% endtest %}
```

### Use Case 5: Production Deployment

Deploy dbt in a production environment with CI/CD.

**GitHub Actions Workflow:**
```yaml
# .github/workflows/dbt_deploy.yml
name: dbt Production Deploy

on:
  push:
    branches: [main]

jobs:
  dbt_run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dbt
        run: pip install dbt-snowflake
      
      - name: Run dbt
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        run: |
          dbt deps
          dbt run --target prod
          dbt test --target prod
```

**Airflow Integration (from Week 4):**
```python
# dags/dbt_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data-team',
    'retries': 1
}

with DAG(
    dag_id='dbt_daily_run',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/dbt && dbt run --target prod'
    )
    
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/dbt && dbt test --target prod'
    )
    
    dbt_run >> dbt_test
```

### Use Case 6: Multi-Environment Development

Develop safely with environment separation.

**profiles.yml:**
```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      schema: DBT_{{ env_var('USER') }}  # Personal dev schema
      # ... other settings
    
    ci:
      type: snowflake
      schema: DBT_CI
      # ... other settings
    
    prod:
      type: snowflake
      schema: ANALYTICS
      # ... other settings
```

**Development Workflow:**
```bash
# 1. Develop in personal schema
dbt run --select my_model

# 2. Test locally
dbt test --select my_model

# 3. Build documentation
dbt docs generate && dbt docs serve

# 4. Commit and push (CI runs in ci environment)
git push origin feature/new-model

# 5. Merge to main (deploys to prod)
```

## Summary

- **Data marts** combine staging, dimension, and fact models for business analytics
- **Snapshots** implement SCD Type 2 for historical dimension tracking
- **Incremental models** process event streams efficiently
- **Testing** ensures data quality at every layer
- **CI/CD** and **Airflow** enable production deployment
- **Environment separation** protects production while enabling development

## Additional Resources

- [dbt Documentation: Snapshots](https://docs.getdbt.com/docs/build/snapshots)
- [dbt Documentation: Testing](https://docs.getdbt.com/docs/build/tests)
- [dbt Documentation: Deployment](https://docs.getdbt.com/docs/deploy/deployments)
- [dbt Best Practices: Production Deployment](https://docs.getdbt.com/guides/best-practices)
