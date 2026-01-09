# Exercise: Build a Complete Data Mart

## Overview
**Day:** 5-Friday  
**Duration:** 3-4 hours  
**Mode:** Individual (Capstone Code Lab)  
**Prerequisites:** All previous Week 5 exercises completed

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| dbt Transformations | [dbt-transformations.md](../../content/5-Friday/dbt-transformations.md) | Jinja templating, macros, materializations |
| dbt Use Cases | [dbt-use-cases.md](../../content/5-Friday/dbt-use-cases.md) | Building data marts, SCD handling |
| Dimensional Modeling | [dimensional-modeling.md](../../content/3-Wednesday/dimensional-modeling.md) | Star schema, facts, dimensions (review) |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Design a complete dbt project from scratch
2. Implement all three layers (staging, intermediate, mart)
3. Apply testing and documentation best practices
4. Build a production-ready data mart

---

## The Scenario

You are building a **Customer Analytics Mart** for TechMart (from the Wednesday exercise). The mart should enable analysts to answer:

1. Who are our most valuable customers?
2. What is the customer lifetime value (LTV)?
3. How does purchase behavior vary by customer segment?
4. What is the customer retention rate?

---

## Project Requirements

### Data Sources (from BRONZE layer)

Use the data you loaded earlier in the week:
- `BRONZE.RAW_EVENTS` - Customer interaction events
- `BRONZE.RAW_PRODUCTS` or `BRONZE.LOADED_ORDERS` - Transaction data

If you need more data, create it:

```sql
-- Sample customer transactions
CREATE OR REPLACE TABLE BRONZE.RAW_TRANSACTIONS (
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO BRONZE.RAW_TRANSACTIONS (raw_data)
SELECT PARSE_JSON(column1) FROM VALUES
('{"transaction_id": "T001", "customer_id": "C001", "transaction_date": "2024-01-15", "amount": 150.00, "product_category": "Electronics"}'),
('{"transaction_id": "T002", "customer_id": "C001", "transaction_date": "2024-01-20", "amount": 75.50, "product_category": "Accessories"}'),
('{"transaction_id": "T003", "customer_id": "C002", "transaction_date": "2024-01-16", "amount": 299.99, "product_category": "Electronics"}'),
('{"transaction_id": "T004", "customer_id": "C001", "transaction_date": "2024-02-01", "amount": 45.00, "product_category": "Accessories"}'),
('{"transaction_id": "T005", "customer_id": "C003", "transaction_date": "2024-02-05", "amount": 500.00, "product_category": "Electronics"}'),
('{"transaction_id": "T006", "customer_id": "C002", "transaction_date": "2024-02-10", "amount": 125.00, "product_category": "Audio"}'),
('{"transaction_id": "T007", "customer_id": "C001", "transaction_date": "2024-02-15", "amount": 89.99, "product_category": "Electronics"}'),
('{"transaction_id": "T008", "customer_id": "C004", "transaction_date": "2024-02-20", "amount": 350.00, "product_category": "Electronics"}');
```

---

## Required Models

### Layer 1: Staging

Create `models/staging/stg_transactions.sql`:
- Extract fields from JSON
- Standardize data types
- Add processing metadata

Create `models/staging/stg_events.sql` (if not done already):
- Same pattern for events

### Layer 2: Intermediate

Create `models/intermediate/int_customer_transactions.sql`:
- Join/aggregate transaction data at customer level
- Calculate per-customer metrics

### Layer 3: Marts

Create `models/marts/dim_customers.sql`:
- Customer dimension with derived attributes
- Segmentation (High Value, Medium, Low based on spend)

Create `models/marts/fct_customer_metrics.sql`:
- Customer fact table with:
  - Total lifetime value
  - Transaction count
  - Average order value
  - Days since first purchase
  - Days since last purchase
  - Favorite category

---

## Core Tasks

### Task 1: Project Setup (15 mins)

Create your dbt project structure:
```
models/
  staging/
    sources.yml
    stg_transactions.sql
    stg_events.sql
  intermediate/
    int_customer_transactions.sql
  marts/
    dim_customers.sql
    fct_customer_metrics.sql
    marts.yml
```

### Task 2: Build Staging Models (45 mins)

**stg_transactions.sql:**
```sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_transactions') }}
),

transformed AS (
    SELECT
        raw_data:transaction_id::STRING AS transaction_id,
        raw_data:customer_id::STRING AS customer_id,
        raw_data:transaction_date::DATE AS transaction_date,
        raw_data:amount::DECIMAL(10,2) AS amount,
        raw_data:product_category::STRING AS product_category,
        _loaded_at
    FROM source
)

SELECT * FROM transformed
```

### Task 3: Build Intermediate Model (30 mins)

**int_customer_transactions.sql:**
```sql
{{
    config(
        materialized='view'
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

customer_agg AS (
    SELECT
        customer_id,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_spend,
        AVG(amount) AS avg_order_value,
        MIN(transaction_date) AS first_purchase_date,
        MAX(transaction_date) AS last_purchase_date,
        COUNT(DISTINCT product_category) AS categories_purchased
    FROM transactions
    GROUP BY customer_id
)

SELECT * FROM customer_agg
```

### Task 4: Build Mart Models (60 mins)

**fct_customer_metrics.sql:**
```sql
{{
    config(
        materialized='table'
    )
}}

WITH customer_txn AS (
    SELECT * FROM {{ ref('int_customer_transactions') }}
),

-- Add favorite category
category_rank AS (
    SELECT 
        customer_id,
        product_category,
        SUM(amount) AS category_spend,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY SUM(amount) DESC
        ) AS rank
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id, product_category
),

favorite_category AS (
    SELECT customer_id, product_category AS favorite_category
    FROM category_rank
    WHERE rank = 1
),

final AS (
    SELECT
        c.customer_id,
        c.transaction_count,
        c.total_spend AS lifetime_value,
        ROUND(c.avg_order_value, 2) AS avg_order_value,
        c.first_purchase_date,
        c.last_purchase_date,
        DATEDIFF('day', c.first_purchase_date, CURRENT_DATE()) AS customer_age_days,
        DATEDIFF('day', c.last_purchase_date, CURRENT_DATE()) AS days_since_last_purchase,
        c.categories_purchased,
        f.favorite_category,
        -- Customer segment
        CASE
            WHEN c.total_spend >= 500 THEN 'High Value'
            WHEN c.total_spend >= 200 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_segment,
        CURRENT_TIMESTAMP() AS _refreshed_at
    FROM customer_txn c
    LEFT JOIN favorite_category f ON c.customer_id = f.customer_id
)

SELECT * FROM final
```

### Task 5: Add Tests (30 mins)

Create `models/marts/marts.yml`:
```yaml
version: 2

models:
  - name: fct_customer_metrics
    description: "Customer-level metrics and segmentation"
    columns:
      - name: customer_id
        description: "Unique customer identifier"
        tests:
          - unique
          - not_null
      - name: lifetime_value
        tests:
          - not_null
      - name: customer_segment
        tests:
          - accepted_values:
              values: ['High Value', 'Medium Value', 'Low Value']
```

### Task 6: Generate Documentation (20 mins)

```bash
dbt docs generate
dbt docs serve --port 8082
```

Verify:
- All models appear in the lineage graph
- Descriptions are complete
- Tests are documented

### Task 7: Run Full Pipeline (15 mins)

```bash
# Run everything
dbt build

# Should show:
# - Models run successfully
# - Tests pass
```

---

## Deliverables

1. **Complete dbt Project:** All model files
2. **Lineage Screenshot:** Full DAG from sources to marts
3. **Test Results:** Output showing all tests pass
4. **Analysis Queries:** 3 SQL queries against your marts answering business questions

---

## Analysis Queries to Write

Using your completed mart, write queries to answer:

1. **Top Customers:** Who are the top 5 customers by lifetime value?

2. **Segment Distribution:** How many customers are in each segment, and what is the total value per segment?

3. **Category Analysis:** What is the most popular favorite category?

---

## Definition of Done

- [ ] Staging models created with source definitions
- [ ] Intermediate model aggregates customer data
- [ ] Mart model includes all required metrics
- [ ] Customer segmentation logic works correctly
- [ ] All tests pass
- [ ] Documentation generated
- [ ] Lineage graph shows complete flow
- [ ] Analysis queries completed

---

## Evaluation Criteria

| Criterion | Points |
|-----------|--------|
| Staging models correctly extract data | 15 |
| Intermediate model aggregates properly | 15 |
| Mart includes all required metrics | 20 |
| Segmentation logic is correct | 10 |
| Tests are comprehensive | 15 |
| Documentation is complete | 10 |
| Analysis queries are correct | 15 |

---

## Stretch Goals

1. Add a `dim_customers` dimension table with SCD Type 2 tracking
2. Create an incremental version of the fact table
3. Add custom data quality tests
4. Create a macro for the segmentation logic
