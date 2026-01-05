# dbt Sources, Staging, and ref() Relationships

## Learning Objectives

- Define sources to represent raw data tables
- Build staging models that cleanse and standardize source data
- Use ref() to create dependencies between models
- Understand how dbt builds and visualizes the dependency graph (DAG)

## Why This Matters

Sources and the ref() function are the foundation of dbt's power. Sources connect your dbt project to raw data (Bronze layer), while ref() creates relationships between models that dbt uses to determine execution order. Staging models sit between sources and business logic, providing a clean, consistent interface for downstream transformations.

## The Concept

### What are Sources?

**Sources** are declarations of raw data tables that exist in your warehouse. They represent your Bronze layer or any external data that dbt does not manage.

**Why Declare Sources?**
- Document raw data dependencies
- Enable source freshness checks
- Provide a clear boundary between raw and transformed data
- Generate lineage in documentation

### Defining Sources

Sources are defined in YAML schema files:

```yaml
# models/staging/sources.yml
version: 2

sources:
  - name: raw_ecommerce
    description: "Raw e-commerce data from the transactional system"
    database: RAW_DB
    schema: ECOMMERCE
    
    tables:
      - name: orders
        description: "Raw order transactions"
        columns:
          - name: order_id
            description: "Primary key"
          - name: customer_id
            description: "Customer identifier"
          - name: order_date
            description: "Date of order"
          - name: amount
            description: "Order amount in cents"
      
      - name: customers
        description: "Customer master data"
        columns:
          - name: customer_id
            description: "Primary key"
          - name: email
            description: "Customer email"
      
      - name: products
        description: "Product catalog"
```

### Using source()

Reference source tables in your models with the `source()` function:

```sql
-- models/staging/stg_orders.sql
SELECT
    order_id,
    customer_id,
    order_date,
    amount
FROM {{ source('raw_ecommerce', 'orders') }}
```

**Syntax:** `{{ source('source_name', 'table_name') }}`

**Compiles To:**
```sql
SELECT
    order_id,
    customer_id,
    order_date,
    amount
FROM RAW_DB.ECOMMERCE.orders
```

### Source Freshness

Check if source data is up-to-date:

```yaml
sources:
  - name: raw_ecommerce
    database: RAW_DB
    schema: ECOMMERCE
    tables:
      - name: orders
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
        loaded_at_field: _loaded_at  # Timestamp column to check
```

**Run Freshness Check:**
```bash
dbt source freshness
```

**Output:**
```
Source raw_ecommerce.orders: PASS (age: 2 hours)
```

### What are Staging Models?

**Staging models** are the first transformation layer. They:
- Rename columns to consistent conventions
- Cast data types
- Apply basic cleaning (nulls, trimming)
- Serve as a stable interface for downstream models

**Characteristics:**
- One staging model per source table
- Prefixed with `stg_`
- Minimal business logic
- Materialize as views (fast to rebuild)

### Building Staging Models

**Example: stg_orders.sql**
```sql
-- models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('raw_ecommerce', 'orders') }}
),

renamed AS (
    SELECT
        -- Primary key
        order_id,
        
        -- Foreign keys
        customer_id,
        
        -- Dates
        order_date::DATE AS order_date,
        
        -- Amounts (convert cents to dollars)
        (amount / 100.0)::DECIMAL(12, 2) AS order_amount,
        
        -- Metadata
        _loaded_at AS loaded_at
    FROM source
    WHERE order_id IS NOT NULL  -- Filter invalid records
)

SELECT * FROM renamed
```

**Example: stg_customers.sql**
```sql
-- models/staging/stg_customers.sql
WITH source AS (
    SELECT * FROM {{ source('raw_ecommerce', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        TRIM(LOWER(email)) AS email,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        created_at::TIMESTAMP_NTZ AS created_at,
        updated_at::TIMESTAMP_NTZ AS updated_at
    FROM source
)

SELECT * FROM renamed
```

### The ref() Function

The `ref()` function references other dbt models. It is the core mechanism for building dependencies.

**Usage:**
```sql
-- models/marts/core/fct_orders.sql
SELECT
    o.order_id,
    o.order_date,
    o.order_amount,
    c.customer_id,
    c.full_name AS customer_name,
    c.email AS customer_email
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id
```

**What ref() Does:**
1. Resolves the model name to the correct database/schema
2. Creates a dependency (fct_orders depends on stg_orders and stg_customers)
3. Ensures dependent models run first

### The DAG (Directed Acyclic Graph)

dbt builds a **DAG** (Directed Acyclic Graph) from your ref() relationships. This graph determines execution order.

```
sources                 staging                 marts
--------               ---------               -------
raw.orders      ->     stg_orders      ->     fct_orders
                                         /
raw.customers   ->     stg_customers   -
                                         \
raw.products    ->     stg_products     ->    dim_products
```

**View the DAG:**
```bash
dbt docs generate
dbt docs serve
```

The documentation site includes an interactive lineage graph.

### Selecting Models

Use the `--select` flag to run specific models:

```bash
# Run a single model
dbt run --select stg_orders

# Run a model and its upstream dependencies
dbt run --select +fct_orders

# Run a model and its downstream dependents
dbt run --select stg_orders+

# Run models in a folder
dbt run --select staging

# Run models with a tag
dbt run --select tag:daily
```

**Selection Syntax:**

| Syntax | Meaning |
|--------|---------|
| `model_name` | Run specific model |
| `+model_name` | Run model and all upstream |
| `model_name+` | Run model and all downstream |
| `+model_name+` | Run model and all up/downstream |
| `folder_name` | Run all models in folder |
| `tag:tagname` | Run models with specific tag |

### Configuring Staging Models

Configure staging models in the schema file or dbt_project.yml:

**Schema File (models/staging/staging.yml):**
```yaml
version: 2

models:
  - name: stg_orders
    description: "Cleansed orders from raw layer"
    config:
      materialized: view
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: order_amount
        tests:
          - not_null
```

**dbt_project.yml:**
```yaml
models:
  my_project:
    staging:
      +materialized: view
      +schema: staging
```

### Complete Flow: Sources to Marts

**1. Declare Sources (sources.yml):**
```yaml
sources:
  - name: raw_ecommerce
    tables:
      - name: orders
      - name: customers
```

**2. Build Staging Models:**
```sql
-- stg_orders.sql
SELECT ... FROM {{ source('raw_ecommerce', 'orders') }}

-- stg_customers.sql
SELECT ... FROM {{ source('raw_ecommerce', 'customers') }}
```

**3. Build Mart Models:**
```sql
-- fct_orders.sql
SELECT ...
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON ...
```

**4. Run dbt:**
```bash
dbt run
```

dbt executes in order:
1. stg_orders (depends on source)
2. stg_customers (depends on source)
3. fct_orders (depends on staging models)

### Best Practices

| Practice | Recommendation |
|----------|----------------|
| **One source, one staging model** | stg_orders for raw.orders |
| **Staging does minimal logic** | Rename, cast, filter nulls |
| **Business logic in marts** | Joins, calculations, aggregations |
| **Always use ref()** | Never hardcode table names |
| **Document sources** | Add descriptions and freshness |
| **Test staging models** | unique, not_null on keys |

## Summary

- **Sources** declare raw tables that dbt does not manage (Bronze layer)
- Use `source()` to reference raw tables; use `ref()` to reference dbt models
- **Staging models** cleanse and standardize source data (Silver layer)
- **ref()** creates dependencies that dbt uses to build the execution DAG
- The **DAG** ensures models run in the correct order
- Use **selection syntax** to run specific models or paths
- Follow **naming conventions** and keep staging models simple

## Additional Resources

- [dbt Documentation: Sources](https://docs.getdbt.com/docs/build/sources)
- [dbt Documentation: ref() Function](https://docs.getdbt.com/reference/dbt-jinja-functions/ref)
- [dbt Best Practices: Staging Models](https://docs.getdbt.com/guides/best-practices/how-we-structure/2-staging)
