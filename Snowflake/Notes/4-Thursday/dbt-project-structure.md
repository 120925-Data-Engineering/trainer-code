# dbt Project Structure

## Learning Objectives

- Understand the anatomy of a dbt project
- Identify the purpose of each directory and configuration file
- Navigate models, seeds, snapshots, tests, and macros
- Configure dbt_project.yml for your project needs

## Why This Matters

A well-organized dbt project is maintainable, scalable, and understandable by your team. Understanding the project structure helps you place files correctly, configure behaviors, and collaborate effectively. As your transformation logic grows, a clear structure prevents the project from becoming an unmanageable tangle of SQL files.

## The Concept

### Project Directory Structure

A typical dbt project has the following structure:

```
my_dbt_project/
|-- dbt_project.yml          # Project configuration
|-- profiles.yml              # Connection profiles (usually in ~/.dbt/)
|-- packages.yml              # External package dependencies
|
|-- models/                   # SQL transformation models
|   |-- staging/
|   |   |-- stg_orders.sql
|   |   |-- stg_customers.sql
|   |   |-- staging.yml       # Schema file for staging models
|   |
|   |-- marts/
|       |-- core/
|       |   |-- dim_customers.sql
|       |   |-- fct_orders.sql
|       |   |-- core.yml
|       |
|       |-- marketing/
|           |-- marketing_funnel.sql
|
|-- seeds/                    # CSV files loaded as tables
|   |-- country_codes.csv
|
|-- snapshots/                # SCD Type 2 snapshots
|   |-- customer_snapshot.sql
|
|-- tests/                    # Custom test SQL files
|   |-- assert_positive_amounts.sql
|
|-- macros/                   # Reusable Jinja macros
|   |-- generate_schema_name.sql
|
|-- analyses/                 # Ad-hoc analytical queries
|   |-- monthly_revenue.sql
|
|-- docs/                     # Custom documentation
    |-- overview.md
```

### dbt_project.yml

The `dbt_project.yml` file is the main configuration file for your project.

```yaml
name: 'my_analytics_project'
version: '1.0.0'
config-version: 2

# Profile to use for connections
profile: 'snowflake_profile'

# Directory configurations
model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
analysis-paths: ["analyses"]

# Compiled and run artifacts
target-path: "target"
clean-targets: ["target", "dbt_packages"]

# Model configurations
models:
  my_analytics_project:
    # Default materialization
    +materialized: view
    
    staging:
      +materialized: view
      +schema: staging
    
    marts:
      +materialized: table
      core:
        +schema: core
      marketing:
        +schema: marketing

# Seed configurations
seeds:
  my_analytics_project:
    +schema: seeds
```

**Key Settings:**

| Setting | Description |
|---------|-------------|
| `name` | Project name (used in model paths) |
| `profile` | Which connection profile to use |
| `model-paths` | Where to find model SQL files |
| `+materialized` | Default materialization strategy |
| `+schema` | Custom schema suffix for models |

### profiles.yml

The `profiles.yml` file contains database connection credentials. It is typically stored in `~/.dbt/profiles.yml` (not in the project directory) to avoid committing secrets.

```yaml
snowflake_profile:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: myaccount.us-east-1
      user: my_user
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: TRANSFORMER_ROLE
      database: ANALYTICS_DEV
      warehouse: TRANSFORM_WH
      schema: DBT_MYUSER
      threads: 4
    
    prod:
      type: snowflake
      account: myaccount.us-east-1
      user: prod_user
      password: "{{ env_var('SNOWFLAKE_PROD_PASSWORD') }}"
      role: TRANSFORMER_ROLE
      database: ANALYTICS_PROD
      warehouse: TRANSFORM_WH
      schema: DBT_PROD
      threads: 8
```

**Target Selection:**
```bash
# Use dev target (default)
dbt run

# Use prod target
dbt run --target prod
```

### Models Directory

Models are the core of dbt. Each `.sql` file in the `models/` directory is a model.

**Model Organization Best Practices:**

```
models/
|-- staging/              # Silver layer: cleansed from sources
|   |-- stripe/           # Organized by source system
|   |   |-- stg_stripe__payments.sql
|   |   |-- stg_stripe__customers.sql
|   |-- shopify/
|       |-- stg_shopify__orders.sql
|
|-- intermediate/         # Optional: complex transformations
|   |-- int_orders_joined.sql
|
|-- marts/                # Gold layer: business-ready
    |-- core/             # Core business entities
    |   |-- dim_customers.sql
    |   |-- fct_orders.sql
    |-- finance/          # Finance-specific models
    |-- marketing/        # Marketing-specific models
```

**Naming Conventions:**

| Prefix | Layer | Example |
|--------|-------|---------|
| `stg_` | Staging (Silver) | `stg_stripe__payments.sql` |
| `int_` | Intermediate | `int_orders_with_payments.sql` |
| `dim_` | Dimension (Gold) | `dim_customers.sql` |
| `fct_` | Fact (Gold) | `fct_orders.sql` |

### Schema Files (YAML)

Schema files (`.yml`) define metadata for models, including:
- Descriptions
- Column documentation
- Tests

```yaml
# models/staging/staging.yml
version: 2

models:
  - name: stg_orders
    description: "Cleansed orders from the raw layer"
    columns:
      - name: order_id
        description: "Primary key"
        tests:
          - unique
          - not_null
      - name: customer_id
        description: "Foreign key to customers"
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: order_total
        description: "Total order amount in USD"
        tests:
          - not_null
```

### Seeds

Seeds are CSV files that dbt loads into your warehouse as tables. Use them for:
- Reference data (country codes, status mappings)
- Lookup tables
- Static data that changes infrequently

```
seeds/
|-- country_codes.csv
|-- order_status_mapping.csv
```

**Example CSV (country_codes.csv):**
```csv
country_code,country_name,region
US,United States,North America
CA,Canada,North America
GB,United Kingdom,Europe
```

**Load Seeds:**
```bash
dbt seed
```

**Reference in Models:**
```sql
SELECT 
    o.order_id,
    c.country_name
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('country_codes') }} c ON o.country_code = c.country_code
```

### Snapshots

Snapshots implement SCD Type 2 (Slowly Changing Dimensions) to track historical changes.

```sql
-- snapshots/customer_snapshot.sql
{% snapshot customer_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT * FROM {{ source('raw', 'customers') }}

{% endsnapshot %}
```

**Run Snapshots:**
```bash
dbt snapshot
```

**Result:** A table with `dbt_valid_from`, `dbt_valid_to`, and `dbt_scd_id` columns tracking history.

### Macros

Macros are reusable Jinja functions. They reduce code duplication and enable dynamic SQL.

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name) %}
    ({{ column_name }} / 100.0)::DECIMAL(12, 2)
{% endmacro %}
```

**Usage in Model:**
```sql
SELECT
    order_id,
    {{ cents_to_dollars('amount_cents') }} AS amount_dollars
FROM {{ ref('stg_orders') }}
```

**Compiles To:**
```sql
SELECT
    order_id,
    (amount_cents / 100.0)::DECIMAL(12, 2) AS amount_dollars
FROM analytics.stg_orders
```

### Tests Directory

Custom data tests go in the `tests/` directory. These are SQL queries that should return zero rows if the test passes.

```sql
-- tests/assert_positive_order_totals.sql
SELECT order_id, order_total
FROM {{ ref('fct_orders') }}
WHERE order_total < 0
```

**Run Tests:**
```bash
dbt test
```

### Packages

Extend dbt with community packages via `packages.yml`:

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.0
  - package: dbt-labs/codegen
    version: 0.12.0
```

**Install Packages:**
```bash
dbt deps
```

**Use Package Macros:**
```sql
SELECT * FROM {{ dbt_utils.star(from=ref('stg_orders')) }}
```

### Common dbt Commands

| Command | Description |
|---------|-------------|
| `dbt init` | Create a new project |
| `dbt run` | Run all models |
| `dbt run --select staging` | Run models in staging folder |
| `dbt test` | Run all tests |
| `dbt seed` | Load seed CSV files |
| `dbt snapshot` | Run snapshots |
| `dbt docs generate` | Generate documentation |
| `dbt docs serve` | Serve documentation locally |
| `dbt deps` | Install packages |
| `dbt compile` | Compile SQL without running |
| `dbt debug` | Test database connection |

## Summary

- The **dbt_project.yml** file configures project settings and model behaviors
- **profiles.yml** contains database connection credentials (keep outside project)
- **Models** are organized by layer: staging, intermediate, marts
- **Schema files (.yml)** document models and define tests
- **Seeds** load static CSV data; **Snapshots** track historical changes
- **Macros** provide reusable Jinja functions for DRY code
- Use **naming conventions** (stg_, dim_, fct_) for clarity

## Additional Resources

- [dbt Documentation: Project Structure](https://docs.getdbt.com/docs/build/projects)
- [dbt Documentation: Configurations](https://docs.getdbt.com/reference/dbt_project.yml)
- [dbt Best Practices: How We Structure Our dbt Projects](https://docs.getdbt.com/guides/best-practices/how-we-structure/1-guide-overview)
