# Automated Testing for Data Workflows

## Learning Objectives
- Identify the different types of tests applicable to data workflows
- Explain when to use unit, integration, and end-to-end tests
- Recognize the key testing frameworks used in data engineering
- Understand how to build a testing culture within data teams

## Why This Matters

Data engineering has historically lagged behind software engineering in testing practices. While application developers have embraced test-driven development, many data teams still rely on manual spot-checks and hope for the best. This gap has real consequences.

Consider a data pipeline that feeds a machine learning model for credit decisions. If a bug silently changes how income is calculated, the model could start denying loans to qualified applicants---or approving risky ones. Without automated tests, such issues might go undetected for weeks or months, causing financial and reputational damage.

**Automated testing for data workflows** brings the same rigor that software engineering has enjoyed for decades. By testing early and often, you catch bugs before they corrupt data, reduce manual validation effort, and build confidence that your pipelines work correctly.

## The Concept

### The Testing Pyramid for Data

The testing pyramid, a concept borrowed from software engineering, applies to data workflows with some adaptations:

```
           /\
          /  \
         / E2E \        <- End-to-End Tests (Fewest)
        /--------\
       /          \
      / Integration \   <- Integration Tests (Some)
     /----------------\
    /                  \
   /      Unit Tests    \
  /________________________\  <- Unit Tests (Most)
```

**Unit Tests** form the base of the pyramid. They are numerous, fast, and test individual components in isolation.

**Integration Tests** sit in the middle. They test how components work together but may not cover the entire pipeline.

**End-to-End Tests** are at the top. They are fewer in number, slower to run, but validate complete workflows.

### Types of Tests for Data Workflows

#### Unit Tests

Unit tests validate individual functions, transformations, or SQL queries in isolation.

**What to Test:**
- Python transformation functions
- SQL query logic (using mock data)
- Data validation functions
- Configuration parsing

**Characteristics:**
- Fast (milliseconds to seconds)
- No external dependencies
- Can run without database connections
- Should cover edge cases

**Example Scenarios:**
- Test that a date parsing function handles various formats
- Test that a revenue calculation returns correct results for known inputs
- Test that null handling logic works as expected

#### Integration Tests

Integration tests validate that multiple components work together correctly.

**What to Test:**
- Database connections and queries
- API integrations
- File reading and writing
- Orchestration tool connections

**Characteristics:**
- Slower than unit tests (seconds to minutes)
- May require test databases or containers
- Test realistic data flows
- Focus on boundaries between systems

**Example Scenarios:**
- Test that a dbt model produces expected output when given specific source data
- Test that an Airflow task can read from S3 and write to Snowflake
- Test that a Kafka consumer correctly processes messages

#### End-to-End Tests

End-to-end tests validate complete pipelines from source to destination.

**What to Test:**
- Full pipeline execution
- Data freshness and completeness
- SLA compliance
- Cross-system data consistency

**Characteristics:**
- Slowest tests (minutes to hours)
- Require realistic environments
- Most representative of production behavior
- Most expensive to maintain

**Example Scenarios:**
- Run the entire daily pipeline with synthetic data and validate outputs
- Test that data flows correctly from raw ingestion through to analytics tables
- Validate that dashboard queries return expected results after a pipeline run

### Data-Specific Test Categories

Beyond the pyramid, data workflows require specific test types:

#### Schema Tests

Validate that data structures match expectations.

```
- Column names are correct
- Data types are as expected
- Required columns exist
- No unexpected columns appear
```

#### Data Quality Tests

Validate that data content meets quality standards.

```
- Primary keys are unique
- Foreign key relationships hold
- Values fall within expected ranges
- No unexpected nulls
- Business rules are satisfied
```

#### Freshness Tests

Validate that data is sufficiently current.

```
- Source data was updated within SLA
- Pipeline completed on time
- No stale data in analytics tables
```

#### Volume Tests

Validate that data volumes are reasonable.

```
- Row counts within expected range
- No unexpected empty tables
- No sudden spikes or drops in volume
```

### Testing Frameworks for Data

| Framework | Use Case | Language |
|-----------|----------|----------|
| pytest | General Python testing | Python |
| dbt test | Model and data testing | SQL/YAML |
| Great Expectations | Data validation | Python |
| Soda Core | Data quality checks | YAML/SQL |
| dbt-expectations | Extended dbt tests | SQL/YAML |

#### pytest

The standard Python testing framework. Ideal for unit testing transformation functions and Python-based pipeline logic.

```python
def test_calculate_revenue():
    result = calculate_revenue(quantity=10, price=5.50)
    assert result == 55.00
```

#### dbt test

Built-in testing for dbt models. Includes schema tests and custom data tests.

```yaml
# schema.yml
models:
  - name: customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
```

#### Great Expectations

A comprehensive data validation framework with rich expectation libraries.

```python
# Validate a DataFrame
validator.expect_column_values_to_be_between(
    column="age",
    min_value=0,
    max_value=120
)
```

### Building a Testing Culture

Technical tools alone are not sufficient. Building a testing culture requires:

#### 1. Make Testing Easy

- Provide templates and examples
- Automate test execution in CI
- Create testing utilities for common patterns

#### 2. Make Testing Visible

- Display test coverage metrics
- Celebrate catching bugs before production
- Include test status in code reviews

#### 3. Make Testing Required

- Block merges on test failures
- Require tests for new features
- Include testing in sprint planning

#### 4. Make Testing Collaborative

- Pair programming on test writing
- Run test review sessions
- Share testing best practices

### Test Data Strategies

One of the biggest challenges is obtaining realistic test data:

**Synthetic Data**: Generate fake data that mimics production patterns. Safe for privacy but may miss edge cases.

**Anonymized Production Data**: Real data with PII removed or obfuscated. More realistic but requires careful handling.

**Snapshot Data**: Point-in-time copies of production data. Useful for regression testing.

**Fixture Data**: Small, hand-crafted datasets for specific test scenarios. Precise but time-consuming to create.

## Code Example

### Unit Test Example with pytest

```python
"""
Unit tests for data transformation functions.
"""

import pytest
from decimal import Decimal
from datetime import date

from transformations.revenue import (
    calculate_revenue,
    apply_discount,
    categorize_customer
)


class TestCalculateRevenue:
    """Tests for revenue calculation function."""

    def test_basic_calculation(self):
        """Test simple revenue calculation."""
        result = calculate_revenue(quantity=10, unit_price=5.00)
        assert result == Decimal("50.00")

    def test_zero_quantity(self):
        """Test that zero quantity returns zero revenue."""
        result = calculate_revenue(quantity=0, unit_price=100.00)
        assert result == Decimal("0.00")

    def test_decimal_precision(self):
        """Test that decimal precision is maintained."""
        result = calculate_revenue(quantity=3, unit_price=10.33)
        assert result == Decimal("30.99")

    def test_negative_quantity_raises_error(self):
        """Test that negative quantity raises ValueError."""
        with pytest.raises(ValueError, match="Quantity cannot be negative"):
            calculate_revenue(quantity=-5, unit_price=10.00)


class TestApplyDiscount:
    """Tests for discount application."""

    @pytest.mark.parametrize("original,discount_pct,expected", [
        (100.00, 10, 90.00),
        (100.00, 0, 100.00),
        (100.00, 100, 0.00),
        (50.00, 25, 37.50),
    ])
    def test_discount_calculations(self, original, discount_pct, expected):
        """Test various discount scenarios."""
        result = apply_discount(original, discount_pct)
        assert result == Decimal(str(expected))


class TestCategorizeCustomer:
    """Tests for customer categorization."""

    def test_new_customer(self):
        """Test categorization of new customer."""
        result = categorize_customer(
            first_purchase_date=date(2024, 1, 1),
            total_purchases=1,
            current_date=date(2024, 1, 15)
        )
        assert result == "new"

    def test_loyal_customer(self):
        """Test categorization of loyal customer."""
        result = categorize_customer(
            first_purchase_date=date(2020, 1, 1),
            total_purchases=50,
            current_date=date(2024, 1, 15)
        )
        assert result == "loyal"

    def test_churned_customer(self):
        """Test categorization of churned customer."""
        result = categorize_customer(
            first_purchase_date=date(2020, 1, 1),
            total_purchases=5,
            last_purchase_date=date(2022, 6, 1),
            current_date=date(2024, 1, 15)
        )
        assert result == "churned"
```

### Integration Test with Test Database

```python
"""
Integration tests for database operations.
"""

import pytest
import sqlalchemy
from sqlalchemy import create_engine, text


@pytest.fixture(scope="module")
def test_db():
    """Create a test database connection."""
    engine = create_engine("postgresql://test:test@localhost:5432/test_db")
    
    # Set up test data
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS test_customers (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100)
            )
        """))
        conn.execute(text("""
            INSERT INTO test_customers VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com')
        """))
        conn.commit()
    
    yield engine
    
    # Tear down
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test_customers"))
        conn.commit()


def test_customer_query(test_db):
    """Test that customer query returns expected results."""
    from pipelines.customer_analytics import get_active_customers
    
    result = get_active_customers(test_db)
    
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
```

### Great Expectations Suite

```python
"""
Data quality expectations for customer data.
"""

from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation import Expectation


def create_customer_expectations():
    """Create expectations for customer data."""
    suite = ExpectationSuite(expectation_suite_name="customer_quality")
    
    expectations = [
        # Column existence
        {"expectation_type": "expect_table_columns_to_match_ordered_list",
         "kwargs": {"column_list": ["customer_id", "name", "email", "created_at"]}},
        
        # Uniqueness
        {"expectation_type": "expect_column_values_to_be_unique",
         "kwargs": {"column": "customer_id"}},
        
        # Not null
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "customer_id"}},
        
        # Email format
        {"expectation_type": "expect_column_values_to_match_regex",
         "kwargs": {"column": "email", "regex": r"^[\w\.-]+@[\w\.-]+\.\w+$"}},
        
        # Row count
        {"expectation_type": "expect_table_row_count_to_be_between",
         "kwargs": {"min_value": 1000, "max_value": 10000000}},
    ]
    
    for exp in expectations:
        suite.add_expectation(exp)
    
    return suite
```

## Summary

- **Testing data workflows** brings software engineering rigor to data pipelines, catching bugs before they corrupt production data
- The **testing pyramid** applies: many unit tests, some integration tests, fewer end-to-end tests
- **Unit tests** validate individual functions quickly and without dependencies
- **Integration tests** verify that components work together correctly
- **End-to-end tests** validate complete pipeline behavior
- Data-specific tests include **schema**, **quality**, **freshness**, and **volume** tests
- Frameworks like **pytest**, **dbt test**, and **Great Expectations** support different testing needs
- Building a **testing culture** requires making testing easy, visible, required, and collaborative

## Additional Resources

- [Great Expectations Documentation](https://docs.greatexpectations.io/) - Comprehensive data validation framework
- [dbt Testing Documentation](https://docs.getdbt.com/docs/build/tests) - Official dbt testing guide
- [Data Engineering Testing Best Practices](https://www.startdataengineering.com/post/data-engineering-test-best-practices/) - Practical testing patterns
