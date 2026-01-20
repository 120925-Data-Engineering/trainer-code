# Writing Effective Test Cases for Data

## Learning Objectives
- Write test cases that validate both raw and transformed data
- Identify edge cases and boundary conditions in data
- Create regression tests that catch unintended changes
- Apply systematic test case design to data workflows

## Why This Matters

Knowing that dbt has testing features is one thing; knowing what to test is another. Many teams write tests that only catch obvious errors while missing subtle bugs that cause real damage.

Consider a test that checks if `order_total` is not null. This test will pass even if every order has a total of zero---technically not null, but almost certainly wrong. Effective test design goes beyond presence checks to validate correctness.

Writing good test cases requires thinking like both a data engineer and a data consumer. You need to anticipate how data can go wrong and craft tests that catch those failures before they impact decisions.

## The Concept

### The Test Case Design Process

Systematic test case design follows these steps:

1. **Understand the Data Contract**: What does correct data look like?
2. **Identify Invariants**: What must always be true?
3. **Find Edge Cases**: What unusual situations could occur?
4. **Design Assertions**: What queries will verify correctness?
5. **Determine Criticality**: Which tests should block deployment?

### Testing Raw Data

Raw data arrives from source systems with minimal transformation. Test cases for raw data focus on:

#### Structure Tests
- Expected columns exist
- Data types are correct
- No unexpected columns appear

```sql
-- Verify expected columns in raw table
SELECT 
    CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END as result
FROM information_schema.columns
WHERE table_name = 'raw_orders'
  AND column_name IN ('order_id', 'customer_id', 'order_date', 'total', 'status')
```

#### Volume Tests
- Row count is within expected range
- No unexpected empty tables
- Volume changes are reasonable

```sql
-- Verify row count is reasonable
SELECT 
    CASE 
        WHEN COUNT(*) BETWEEN 1000 AND 10000000 THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM raw_orders
```

#### Freshness Tests
- Data was updated recently
- No gaps in time series data
- Latest records are recent enough

```sql
-- Verify data is fresh
SELECT 
    CASE 
        WHEN MAX(created_at) >= DATEADD(day, -1, CURRENT_DATE) THEN 'PASS'
        ELSE 'FAIL'
    END as result
FROM raw_orders
```

### Testing Transformed Data

Transformed data has undergone business logic. Test cases must verify that logic was applied correctly.

#### Transformation Correctness
- Calculations produce expected results
- Joins do not drop or duplicate records
- Filters include/exclude appropriate data

**Example: Test that join preserves record count**

```sql
-- Before join: count source records
WITH source_count AS (
    SELECT COUNT(*) as cnt FROM {{ ref('stg_orders') }}
),
-- After join: count result records
result_count AS (
    SELECT COUNT(*) as cnt FROM {{ ref('fct_orders') }}
)
-- Test: no records should be lost in the join
SELECT 
    s.cnt as source_records,
    r.cnt as result_records,
    s.cnt - r.cnt as records_lost
FROM source_count s, result_count r
WHERE s.cnt != r.cnt
```

#### Business Rule Validation
- Domain-specific rules are enforced
- Calculations match business definitions
- Categories are assigned correctly

**Example: Test revenue calculation**

```sql
-- Revenue should equal quantity times price
SELECT 
    order_id,
    quantity,
    unit_price,
    revenue,
    quantity * unit_price as expected_revenue
FROM {{ ref('fct_line_items') }}
WHERE revenue != quantity * unit_price
```

### Edge Case Identification

Edge cases are unusual but valid situations that often expose bugs. Systematically consider:

#### Boundary Values
- Zero values (quantity 0, price 0)
- Maximum values (largest order, oldest customer)
- Minimum values (single item order)

```sql
-- Test: zero quantity orders should have zero revenue
SELECT *
FROM {{ ref('fct_line_items') }}
WHERE quantity = 0 AND revenue != 0
```

#### Null Handling
- Nulls in optional fields
- Nulls propagating through calculations
- Nulls in join columns

```sql
-- Test: null handling in aggregations
SELECT 
    customer_id,
    total_orders,
    total_revenue
FROM {{ ref('customer_summary') }}
WHERE total_orders > 0 AND total_revenue IS NULL
```

#### Date Edge Cases
- Beginning of month/year
- Leap years
- Timezone boundaries
- Future dates
- Historical dates

```sql
-- Test: no orders from the future
SELECT *
FROM {{ ref('fct_orders') }}
WHERE order_date > CURRENT_DATE
```

#### Special Characters
- Unicode characters in names
- Leading/trailing whitespace
- Empty strings vs nulls

```sql
-- Test: no empty strings masquerading as data
SELECT *
FROM {{ ref('dim_customers') }}
WHERE TRIM(customer_name) = ''
```

### Regression Testing

Regression tests catch unintended changes to existing behavior. They are especially important when:

- Refactoring transformation logic
- Updating join conditions
- Changing filter criteria
- Modifying business calculations

#### Snapshot Comparison

Compare current output to a known-good baseline:

```sql
-- Compare current output to baseline
SELECT 
    c.customer_id,
    c.total_lifetime_value as current_value,
    b.total_lifetime_value as baseline_value,
    ABS(c.total_lifetime_value - b.total_lifetime_value) as difference
FROM {{ ref('customer_summary') }} c
JOIN baseline_customer_summary b ON c.customer_id = b.customer_id
WHERE ABS(c.total_lifetime_value - b.total_lifetime_value) > 0.01
```

#### Trend Validation

Check that metrics follow expected patterns:

```sql
-- Revenue should not decrease significantly day-over-day
WITH daily_revenue AS (
    SELECT 
        order_date,
        SUM(total) as revenue,
        LAG(SUM(total)) OVER (ORDER BY order_date) as prev_day_revenue
    FROM {{ ref('fct_orders') }}
    GROUP BY order_date
)
SELECT *
FROM daily_revenue
WHERE revenue < prev_day_revenue * 0.5  -- Flag 50%+ drops
```

### Test Case Categories

Organize tests into categories for clarity:

| Category | Purpose | Example |
|----------|---------|---------|
| Smoke Tests | Basic sanity checks | Table is not empty |
| Validity Tests | Format and constraint checks | Email matches pattern |
| Completeness Tests | Required data is present | Primary key not null |
| Accuracy Tests | Values are correct | Sum matches expected |
| Consistency Tests | Data agrees across sources | FK relationships valid |
| Regression Tests | Changes are intentional | Metrics match baseline |

### Test Case Template

Use a consistent template for documenting test cases:

```markdown
## Test Case: [Name]

**Description:** [What this test validates]

**Preconditions:** [What must be true before the test runs]

**Test Query:**
[SQL that returns rows on failure]

**Expected Result:** [What a passing result looks like]

**Severity:** [Error/Warn]

**Owner:** [Team or person responsible]
```

## Code Example

### Comprehensive Test Suite

```sql
-- tests/assert_order_integrity.sql
-- This file contains multiple assertions about order data integrity

-- 1. Orders must have positive totals
-- Returns orders with non-positive totals
SELECT 
    order_id,
    order_total,
    'Non-positive order total' as violation
FROM {{ ref('fct_orders') }}
WHERE order_total <= 0

UNION ALL

-- 2. Order date must not be in the future
SELECT 
    order_id,
    order_date,
    'Future order date' as violation
FROM {{ ref('fct_orders') }}
WHERE order_date > CURRENT_DATE

UNION ALL

-- 3. Every order must have at least one line item
SELECT 
    o.order_id,
    NULL as order_date,
    'Order with no line items' as violation
FROM {{ ref('fct_orders') }} o
LEFT JOIN {{ ref('fct_line_items') }} li ON o.order_id = li.order_id
WHERE li.order_id IS NULL

UNION ALL

-- 4. Line item sum must match order total
SELECT 
    o.order_id,
    o.order_total,
    'Line items do not sum to order total' as violation
FROM {{ ref('fct_orders') }} o
LEFT JOIN (
    SELECT order_id, SUM(line_total) as calc_total
    FROM {{ ref('fct_line_items') }}
    GROUP BY order_id
) li ON o.order_id = li.order_id
WHERE ABS(o.order_total - li.calc_total) > 0.01
```

### Edge Case Test Suite

```sql
-- tests/assert_edge_cases.sql

-- Test: Zero quantity line items
SELECT 
    line_item_id,
    'Zero quantity with non-zero total' as issue
FROM {{ ref('fct_line_items') }}
WHERE quantity = 0 AND line_total != 0

UNION ALL

-- Test: Null customer on orders
SELECT 
    order_id,
    'Order without customer' as issue
FROM {{ ref('fct_orders') }}
WHERE customer_id IS NULL

UNION ALL

-- Test: Duplicate email addresses
SELECT 
    email,
    'Duplicate email found' as issue
FROM {{ ref('dim_customers') }}
WHERE email IS NOT NULL
GROUP BY email
HAVING COUNT(*) > 1

UNION ALL

-- Test: Negative prices
SELECT 
    product_id,
    'Negative unit price' as issue
FROM {{ ref('dim_products') }}
WHERE unit_price < 0
```

### Python Test Case Generator

```python
"""
Generate test cases for data quality validation.
"""

from typing import List, Dict
from dataclasses import dataclass


@dataclass
class TestCase:
    """Represents a single test case."""
    name: str
    description: str
    query: str
    severity: str = "error"


def generate_primary_key_tests(table: str, pk_column: str) -> List[TestCase]:
    """Generate standard primary key tests."""
    return [
        TestCase(
            name=f"{table}_{pk_column}_unique",
            description=f"Verify {pk_column} is unique in {table}",
            query=f"""
                SELECT {pk_column}, COUNT(*) as cnt
                FROM {table}
                GROUP BY {pk_column}
                HAVING COUNT(*) > 1
            """
        ),
        TestCase(
            name=f"{table}_{pk_column}_not_null",
            description=f"Verify {pk_column} is never null in {table}",
            query=f"""
                SELECT *
                FROM {table}
                WHERE {pk_column} IS NULL
            """
        )
    ]


def generate_foreign_key_test(
    child_table: str, 
    child_column: str,
    parent_table: str,
    parent_column: str
) -> TestCase:
    """Generate foreign key relationship test."""
    return TestCase(
        name=f"{child_table}_{child_column}_fk",
        description=f"Verify {child_column} references valid {parent_table}",
        query=f"""
            SELECT c.{child_column}
            FROM {child_table} c
            LEFT JOIN {parent_table} p ON c.{child_column} = p.{parent_column}
            WHERE c.{child_column} IS NOT NULL
              AND p.{parent_column} IS NULL
        """
    )


def generate_range_test(
    table: str,
    column: str,
    min_value: float,
    max_value: float
) -> TestCase:
    """Generate numeric range validation test."""
    return TestCase(
        name=f"{table}_{column}_range",
        description=f"Verify {column} is between {min_value} and {max_value}",
        query=f"""
            SELECT {column}
            FROM {table}
            WHERE {column} < {min_value} OR {column} > {max_value}
        """,
        severity="warn"
    )


# Example: Generate test suite
if __name__ == "__main__":
    tests = []
    
    # Primary key tests
    tests.extend(generate_primary_key_tests("fct_orders", "order_id"))
    tests.extend(generate_primary_key_tests("dim_customers", "customer_id"))
    
    # Foreign key tests
    tests.append(generate_foreign_key_test(
        "fct_orders", "customer_id",
        "dim_customers", "customer_id"
    ))
    
    # Range tests
    tests.append(generate_range_test("fct_orders", "order_total", 0, 1000000))
    
    # Output test cases
    for test in tests:
        print(f"-- {test.name}")
        print(f"-- {test.description}")
        print(f"-- Severity: {test.severity}")
        print(test.query)
        print()
```

## Summary

- Effective test cases require understanding the **data contract**---what correct data looks like
- Test **raw data** for structure, volume, and freshness
- Test **transformed data** for calculation correctness, join integrity, and business rule compliance
- Systematically identify **edge cases**: boundary values, nulls, dates, and special characters
- **Regression tests** catch unintended changes when refactoring
- Organize tests into categories: smoke, validity, completeness, accuracy, consistency, and regression
- Use a consistent **template** for documenting test cases
- Remember that the goal is to catch bugs **before** they impact business decisions

## Additional Resources

- [The Art of Software Testing](https://www.wiley.com/en-us/The+Art+of+Software+Testing%2C+3rd+Edition-p-9781118031964) - Classic text on test case design
- [dbt Testing Best Practices](https://docs.getdbt.com/best-practices/testing) - Official dbt guidance
- [Data Testing Pyramid](https://dataqualitypro.com/data-testing-pyramid/) - Framework for data test design
