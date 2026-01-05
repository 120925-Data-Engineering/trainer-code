# dbt Transformations

## Learning Objectives

- Implement complex transformations using dbt and Jinja
- Create and use macros for reusable logic
- Build incremental models for efficient large-scale processing
- Understand materialization strategies and when to use each

## Why This Matters

As data volumes grow, efficient transformations become critical. Full table rebuilds may take hours and consume significant compute resources. dbt's advanced features like incremental models, Jinja templating, and macros enable you to build performant, maintainable transformation pipelines that scale with your data.

## The Concept

### Jinja Templating in dbt

dbt uses **Jinja**, a Python templating language, to add dynamic behavior to SQL. Jinja enables:
- Control flow (if/else, loops)
- Variable substitution
- Macro calls
- Dynamic SQL generation

**Basic Jinja Syntax:**

| Syntax | Purpose |
|--------|---------|
| `{{ ... }}` | Output expression (variables, functions) |
| `{% ... %}` | Execute statement (if, for, set) |
| `{# ... #}` | Comment (not rendered) |

### Variables and Expressions

```sql
-- models/marts/revenue_report.sql
{% set fiscal_year_start = '2024-04-01' %}

SELECT
    order_date,
    order_amount
FROM {{ ref('stg_orders') }}
WHERE order_date >= '{{ fiscal_year_start }}'
```

**Using dbt Variables:**
```sql
-- Access variables defined in dbt_project.yml or CLI
SELECT *
FROM {{ ref('stg_orders') }}
WHERE order_date >= '{{ var("start_date", "2024-01-01") }}'
```

**CLI Override:**
```bash
dbt run --vars '{"start_date": "2024-06-01"}'
```

### Control Flow

**If/Else:**
```sql
SELECT
    order_id,
    order_amount,
    {% if target.name == 'prod' %}
        -- In production, use actual data
        customer_email
    {% else %}
        -- In dev, mask PII
        MD5(customer_email) AS customer_email
    {% endif %}
FROM {{ ref('stg_orders') }}
```

**For Loops:**
```sql
{% set payment_methods = ['credit_card', 'debit_card', 'paypal', 'gift_card'] %}

SELECT
    order_id,
    {% for method in payment_methods %}
        SUM(CASE WHEN payment_method = '{{ method }}' THEN amount ELSE 0 END) 
            AS {{ method }}_amount
        {% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ ref('stg_payments') }}
GROUP BY order_id
```

**Compiles To:**
```sql
SELECT
    order_id,
    SUM(CASE WHEN payment_method = 'credit_card' THEN amount ELSE 0 END) AS credit_card_amount,
    SUM(CASE WHEN payment_method = 'debit_card' THEN amount ELSE 0 END) AS debit_card_amount,
    SUM(CASE WHEN payment_method = 'paypal' THEN amount ELSE 0 END) AS paypal_amount,
    SUM(CASE WHEN payment_method = 'gift_card' THEN amount ELSE 0 END) AS gift_card_amount
FROM analytics.stg_payments
GROUP BY order_id
```

### Macros

**Macros** are reusable Jinja functions defined in the `macros/` directory.

**Creating a Macro:**
```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, precision=2) %}
    ({{ column_name }} / 100.0)::DECIMAL(12, {{ precision }})
{% endmacro %}
```

**Using the Macro:**
```sql
SELECT
    order_id,
    {{ cents_to_dollars('amount_cents') }} AS amount_dollars,
    {{ cents_to_dollars('tax_cents', 4) }} AS tax_dollars
FROM {{ ref('stg_orders') }}
```

**Macro with Logic:**
```sql
-- macros/generate_date_spine.sql
{% macro generate_date_spine(start_date, end_date) %}
    SELECT
        DATEADD('day', SEQ4(), '{{ start_date }}'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('day', '{{ start_date }}', '{{ end_date }}') + 1))
{% endmacro %}
```

**Usage:**
```sql
WITH date_spine AS (
    {{ generate_date_spine('2024-01-01', '2024-12-31') }}
)
SELECT * FROM date_spine
```

### Materializations

Materializations determine how dbt creates objects in the warehouse.

| Materialization | Description | Use Case |
|-----------------|-------------|----------|
| `view` | Creates a SQL view | Staging models, simple transforms |
| `table` | Creates a table (full refresh) | Mart models, dimension tables |
| `incremental` | Appends/merges new data | Large fact tables, event logs |
| `ephemeral` | CTE only, not materialized | Intermediate calculations |

**Configuring Materialization:**
```sql
-- In model file
{{ config(materialized='table') }}

SELECT * FROM {{ ref('stg_orders') }}
```

**Or in dbt_project.yml:**
```yaml
models:
  my_project:
    marts:
      +materialized: table
```

### Incremental Models

**Incremental models** process only new or changed data, dramatically reducing compute for large tables.

**Basic Incremental Model:**
```sql
-- models/marts/fct_orders.sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    order_id,
    customer_id,
    order_date,
    order_amount,
    processed_at
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    -- Only process rows newer than the last run
    WHERE processed_at > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}
```

**How It Works:**
1. **First Run:** Full table build (like `table` materialization)
2. **Subsequent Runs:** Only new rows (WHERE clause filters)
3. **Merge/Append:** New rows inserted or merged based on unique_key

**Key Components:**

| Component | Purpose |
|-----------|---------|
| `is_incremental()` | Returns TRUE on incremental runs |
| `{{ this }}` | References the current model's table |
| `unique_key` | Column(s) for merge deduplication |

**Incremental Strategies:**

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'  -- or 'delete+insert', 'append'
) }}
```

| Strategy | Behavior |
|----------|----------|
| `append` | Insert only, no updates |
| `delete+insert` | Delete matching keys, then insert |
| `merge` | MERGE statement (update or insert) |

**Full Refresh:**
Override incremental behavior when needed:
```bash
dbt run --full-refresh --select fct_orders
```

### Advanced Incremental Patterns

**Lookback Window (Handle Late-Arriving Data):**
```sql
{% if is_incremental() %}
    WHERE event_date >= DATEADD('day', -3, (SELECT MAX(event_date) FROM {{ this }}))
{% endif %}
```

**Partitioned Incremental (Snowflake):**
```sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    cluster_by=['event_date']
) }}
```

### Ephemeral Models

Ephemeral models are not materialized; they become CTEs in downstream models.

```sql
-- models/staging/stg_exchange_rates.sql
{{ config(materialized='ephemeral') }}

SELECT
    currency_code,
    rate_to_usd,
    rate_date
FROM {{ source('finance', 'exchange_rates') }}
```

**When Used in Another Model:**
```sql
SELECT
    o.order_id,
    o.amount * e.rate_to_usd AS amount_usd
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_exchange_rates') }} e  -- This becomes a CTE
    ON o.currency = e.currency_code
```

**Compiles To:**
```sql
WITH stg_exchange_rates AS (
    SELECT currency_code, rate_to_usd, rate_date
    FROM finance.exchange_rates
)
SELECT
    o.order_id,
    o.amount * e.rate_to_usd AS amount_usd
FROM analytics.stg_orders o
LEFT JOIN stg_exchange_rates e
    ON o.currency = e.currency_code
```

### Using dbt_utils Package

The **dbt_utils** package provides common macros:

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.0
```

**Common dbt_utils Macros:**

```sql
-- Generate surrogate key
{{ dbt_utils.generate_surrogate_key(['order_id', 'line_item_id']) }}

-- Star (select all columns)
{{ dbt_utils.star(from=ref('stg_orders'), except=['_loaded_at']) }}

-- Pivot
{{ dbt_utils.pivot('payment_method', dbt_utils.get_column_values(...)) }}

-- Date spine
{{ dbt_utils.date_spine(
    datepart='day',
    start_date="'2024-01-01'",
    end_date="'2024-12-31'"
) }}
```

### Testing Transformations

Test your models with built-in and custom tests:

**Schema Tests (in YAML):**
```yaml
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: order_amount
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
```

**Custom Singular Tests:**
```sql
-- tests/assert_total_revenue_positive.sql
SELECT SUM(order_amount)
FROM {{ ref('fct_orders') }}
HAVING SUM(order_amount) < 0
```

## Summary

- **Jinja templating** enables dynamic SQL with variables, loops, and conditionals
- **Macros** encapsulate reusable logic in the `macros/` directory
- **Materializations** control how dbt creates objects (view, table, incremental, ephemeral)
- **Incremental models** process only new data, essential for large tables
- Use `is_incremental()` and `{{ this }}` to build incremental logic
- The **dbt_utils** package provides common helper macros
- Choose materialization based on **data volume, refresh frequency, and query patterns**

## Additional Resources

- [dbt Documentation: Jinja and Macros](https://docs.getdbt.com/docs/build/jinja-macros)
- [dbt Documentation: Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt Documentation: Materializations](https://docs.getdbt.com/docs/build/materializations)
- [dbt_utils Package](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)
