# dbt Testing

## Learning Objectives
- Explain dbt's built-in test types and their purposes
- Write schema tests using YAML configuration
- Create custom singular tests for complex business logic
- Develop testing strategies for dbt projects

## Why This Matters

In Week 5, you built dbt models that transform raw data into analytics-ready tables. Those transformations contain business logic that can break in subtle ways: a join that silently drops records, a filter that excludes valid data, or an aggregation that double-counts.

dbt's testing framework is designed specifically for data transformations. Unlike application tests that check code behavior, dbt tests check data correctness. They run against your actual models, validating that the data meets your expectations after every transformation.

When you run `dbt test`, you are asking: "Does my data look right?" This is a fundamentally different question from "Does my code run?" and it is the question that matters most to downstream consumers of your data.

## The Concept

### Types of dbt Tests

dbt supports several categories of tests:

| Test Type | Where Defined | What It Tests |
|-----------|---------------|---------------|
| Schema Tests | `schema.yml` | Column-level properties (unique, not_null, etc.) |
| Singular Tests | `tests/*.sql` | Custom SQL assertions |
| Generic Tests | `macros/*.sql` | Reusable custom tests |
| Source Tests | `sources.yml` | Source data freshness and integrity |

### Built-in Schema Tests

dbt includes four built-in tests that cover the most common data quality needs:

#### unique

Validates that a column contains no duplicate values.

```yaml
models:
  - name: customers
    columns:
      - name: customer_id
        tests:
          - unique
```

**When to use**: Primary keys, natural keys, any column that should identify a single record.

**How it works**: dbt runs a query like:
```sql
SELECT customer_id
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1
```

If any rows are returned, the test fails.

#### not_null

Validates that a column contains no null values.

```yaml
models:
  - name: customers
    columns:
      - name: customer_id
        tests:
          - not_null
          - unique
```

**When to use**: Required fields, foreign keys, any column essential for downstream use.

**How it works**:
```sql
SELECT customer_id
FROM customers
WHERE customer_id IS NULL
```

#### accepted_values

Validates that a column only contains values from a specified list.

```yaml
models:
  - name: orders
    columns:
      - name: status
        tests:
          - accepted_values:
              values: ['pending', 'shipped', 'delivered', 'cancelled']
```

**When to use**: Status fields, category columns, any column with a known set of valid values.

#### relationships

Validates that every value in a column exists in another table (referential integrity).

```yaml
models:
  - name: orders
    columns:
      - name: customer_id
        tests:
          - relationships:
              to: ref('customers')
              field: customer_id
```

**When to use**: Foreign key relationships, ensuring joins will not drop records.

### Combining Tests

A column can have multiple tests:

```yaml
models:
  - name: customers
    columns:
      - name: customer_id
        description: "Unique identifier for each customer"
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - unique
          - not_null
      - name: status
        tests:
          - not_null
          - accepted_values:
              values: ['active', 'inactive', 'pending']
```

### Singular Tests

Singular tests are custom SQL queries stored in the `tests/` directory. Any query that returns rows indicates a test failure.

A singular test should return the rows that violate your expectation. If the query returns zero rows, the test passes.

**Example: Order total should equal sum of line items**

```sql
-- tests/assert_order_totals_match.sql

-- This test returns orders where the total does not match line item sum
-- An empty result means all orders match correctly

SELECT
    o.order_id,
    o.total_amount,
    l.line_item_sum,
    o.total_amount - l.line_item_sum as discrepancy
FROM {{ ref('orders') }} o
LEFT JOIN (
    SELECT 
        order_id, 
        SUM(quantity * unit_price) as line_item_sum
    FROM {{ ref('line_items') }}
    GROUP BY order_id
) l ON o.order_id = l.order_id
WHERE o.total_amount != l.line_item_sum
   OR l.line_item_sum IS NULL
```

### Generic Tests

Generic tests are reusable test templates defined as macros. They allow you to create custom tests that can be applied to any column via YAML configuration.

**Example: Test that values are positive**

Create `macros/test_positive.sql`:

```sql
{% test positive(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
```

Use it in your schema:

```yaml
models:
  - name: transactions
    columns:
      - name: amount
        tests:
          - positive
```

### Source Freshness Tests

dbt can test that source data is being updated regularly:

```yaml
sources:
  - name: raw
    database: raw_db
    schema: public
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: customers
      - name: orders
```

Run freshness tests with:
```bash
dbt source freshness
```

### Test Severity

Tests can be configured with different severity levels:

```yaml
models:
  - name: customers
    columns:
      - name: email
        tests:
          - unique:
              severity: warn  # Will not fail the run
          - not_null:
              severity: error  # Will fail the run (default)
```

Use `warn` for tests that indicate potential issues but should not block deployment.

### Testing Strategies

#### 1. Test Every Model

At minimum, test:
- Primary key: unique + not_null
- Foreign keys: relationships
- Status/category columns: accepted_values

#### 2. Test Business Rules

Translate business requirements into tests:
- "Orders cannot have negative totals" becomes a singular test
- "Every order must have at least one line item" becomes a singular test

#### 3. Test Transformation Logic

Create tests that validate your transformations:
- After a join, verify no unexpected record loss
- After an aggregation, verify totals match source

#### 4. Incremental Model Testing

For incremental models, test that:
- New records are being added
- Existing records are not duplicated
- Updates are applied correctly

## Code Example

### Complete Schema.yml Example

```yaml
# models/marts/schema.yml

version: 2

models:
  - name: dim_customers
    description: "Customer dimension table"
    columns:
      - name: customer_key
        description: "Surrogate key for the customer"
        tests:
          - unique
          - not_null
      
      - name: customer_id
        description: "Natural key from source system"
        tests:
          - unique
          - not_null
      
      - name: email
        description: "Customer email address"
        tests:
          - unique
      
      - name: customer_status
        description: "Current customer status"
        tests:
          - not_null
          - accepted_values:
              values: ['active', 'inactive', 'churned']

  - name: fct_orders
    description: "Fact table for orders"
    columns:
      - name: order_key
        description: "Surrogate key for the order"
        tests:
          - unique
          - not_null
      
      - name: customer_key
        description: "Foreign key to dim_customers"
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_key
      
      - name: order_total
        description: "Total order amount"
        tests:
          - not_null
          - positive_value

  - name: fct_line_items
    description: "Fact table for order line items"
    columns:
      - name: line_item_key
        tests:
          - unique
          - not_null
      
      - name: order_key
        tests:
          - not_null
          - relationships:
              to: ref('fct_orders')
              field: order_key
      
      - name: product_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_products')
              field: product_key
```

### Singular Tests

```sql
-- tests/assert_no_orphan_line_items.sql
-- Line items should always reference a valid order

SELECT li.*
FROM {{ ref('fct_line_items') }} li
LEFT JOIN {{ ref('fct_orders') }} o ON li.order_key = o.order_key
WHERE o.order_key IS NULL
```

```sql
-- tests/assert_order_dates_logical.sql
-- Order date should not be in the future

SELECT 
    order_key,
    order_date,
    CURRENT_DATE as today
FROM {{ ref('fct_orders') }}
WHERE order_date > CURRENT_DATE
```

```sql
-- tests/assert_customer_has_orders.sql
-- Every customer in dim_customers should have at least one order
-- Use warn severity since some customers may be new

{{ config(severity='warn') }}

SELECT 
    c.customer_key,
    c.customer_id
FROM {{ ref('dim_customers') }} c
LEFT JOIN {{ ref('fct_orders') }} o ON c.customer_key = o.customer_key
WHERE o.order_key IS NULL
  AND c.created_date < DATEADD(day, -30, CURRENT_DATE)  -- Exclude recent customers
```

### Custom Generic Tests

```sql
-- macros/tests/test_positive_value.sql
{% test positive_value(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}
```

```sql
-- macros/tests/test_not_future_date.sql
{% test not_future_date(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} > CURRENT_DATE

{% endtest %}
```

### Running Tests

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select dim_customers

# Run tests for a model and its downstream dependencies
dbt test --select dim_customers+

# Run only schema tests
dbt test --select test_type:schema_change

# Run only singular tests
dbt test --select test_type:singular

# Run tests with verbose output
dbt test --debug
```

## Summary

- dbt tests validate **data correctness**, not just code execution
- **Built-in tests** (unique, not_null, accepted_values, relationships) cover common validation needs
- **Singular tests** are custom SQL queries that return rows for failures
- **Generic tests** are reusable test templates defined as macros
- **Source freshness tests** ensure upstream data is being updated
- **Severity levels** (warn, error) control whether tests block deployment
- Every model should have tests on primary keys, foreign keys, and business-critical rules
- Tests are defined in `schema.yml` and run with `dbt test`

## Additional Resources

- [dbt Testing Documentation](https://docs.getdbt.com/docs/build/tests) - Official dbt testing guide
- [dbt-expectations Package](https://github.com/calogica/dbt-expectations) - Extended test library inspired by Great Expectations
- [dbt Testing Best Practices](https://docs.getdbt.com/best-practices/testing) - Official best practices guide
