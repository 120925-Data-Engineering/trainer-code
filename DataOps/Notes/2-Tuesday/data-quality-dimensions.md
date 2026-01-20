# Data Quality Dimensions

## Learning Objectives
- Define the six dimensions of data quality
- Explain how each dimension impacts business decisions
- Identify measurement approaches for each dimension
- Apply data quality dimensions to evaluate real datasets

## Why This Matters

You have built pipelines that move and transform data. But how do you know if that data is actually good? A pipeline can run perfectly while producing garbage---technically correct execution with fundamentally flawed output.

Consider a retail company analyzing customer purchase patterns. Their analytics show that 30% of customers have no recorded purchases in the last year. Is this data quality issue, indicating missing transactions, or a genuine insight that 30% of customers have churned? Without understanding data quality dimensions, you cannot answer this question with confidence.

**Data quality dimensions** provide a framework for evaluating whether data is fit for its intended purpose. By measuring and monitoring these dimensions, you transform vague concerns about "bad data" into specific, actionable metrics.

## The Concept

### The Six Dimensions of Data Quality

Data quality is not a single property but a multifaceted concept. The industry has converged on six primary dimensions:

| Dimension | Definition | Key Question |
|-----------|------------|--------------|
| Accuracy | Data correctly represents the real-world entity | Does this value match reality? |
| Completeness | All required data is present | Is anything missing? |
| Timeliness | Data is available when needed | Is this data current enough? |
| Consistency | Data agrees across sources and over time | Do these values match? |
| Validity | Data conforms to defined formats and rules | Is this value in the right format? |
| Uniqueness | No duplicate records exist | Is this entity recorded only once? |

### Accuracy

**Definition**: Data accurately represents the true state of the real-world entity or event it describes.

**Examples of Accuracy Issues**:
- A customer's address was entered incorrectly during registration
- A sensor reading is wrong due to calibration error
- A product price was updated in the source system but the old value remains in the warehouse

**Measurement Approaches**:
- Compare sample records against source documents
- Cross-reference with external authoritative sources
- Track data corrections over time

**Challenges**:
- Determining the "source of truth" for comparison
- Accuracy issues are often discovered late, during downstream use
- Some inaccuracies are hard to detect without business context

### Completeness

**Definition**: All required data elements are present and populated.

**Levels of Completeness**:

1. **Schema Completeness**: Are all required columns present?
2. **Column Completeness**: What percentage of a column's values are populated?
3. **Record Completeness**: Does each record have all its required fields?

**Examples of Completeness Issues**:
- Customer records missing email addresses
- Orders without shipping addresses
- Product records without category assignments

**Measurement Approaches**:
```
Column Completeness = (Non-null values / Total values) x 100
Record Completeness = (Fully populated records / Total records) x 100
```

**Considerations**:
- Not all nulls indicate quality issues (some fields are optional)
- Define explicit rules for which fields must be populated
- Consider completeness thresholds rather than requiring 100%

### Timeliness

**Definition**: Data is available when needed and reflects a sufficiently recent state.

**Two Aspects of Timeliness**:

1. **Currency**: How recent is the data? (When was it last updated?)
2. **Latency**: How quickly is new data available? (Time from event to availability)

**Examples of Timeliness Issues**:
- A daily pipeline that runs at midnight but analysts need data by 6 AM
- Stock inventory data that is 24 hours old, leading to overselling
- Customer preferences captured but not reflected in recommendations until the next day

**Measurement Approaches**:
- Track data freshness (time since last update)
- Measure pipeline latency (time from source event to warehouse)
- Compare expected vs actual arrival times

**Context Matters**:
- Real-time analytics require minute-level freshness
- Monthly reports may tolerate day-old data
- Always define timeliness requirements based on use case

### Consistency

**Definition**: Data is coherent within itself and across different sources or time periods.

**Types of Consistency**:

1. **Cross-Source Consistency**: Same entity has same values across systems
2. **Temporal Consistency**: Values are logical over time
3. **Referential Consistency**: Foreign key relationships are valid

**Examples of Consistency Issues**:
- Customer address differs between billing and shipping systems
- Order total does not equal sum of line items
- Product referenced in order does not exist in product table
- A customer's age decreases between two data snapshots

**Measurement Approaches**:
- Cross-system reconciliation reports
- Referential integrity checks
- Logical validation rules (sum checks, date ordering)

**Common Causes**:
- Lack of master data management
- Systems updated at different times
- Data entry in multiple systems without synchronization

### Validity

**Definition**: Data conforms to the defined business rules, formats, and constraints.

**Types of Validity**:

1. **Format Validity**: Data matches expected format (email format, date format)
2. **Range Validity**: Values fall within acceptable bounds
3. **Domain Validity**: Values belong to an allowed set
4. **Rule Validity**: Business rules are satisfied

**Examples of Validity Issues**:
- Email addresses without @ symbol
- Dates in wrong format (MM/DD/YYYY vs DD/MM/YYYY)
- Age values greater than 150 or less than 0
- Status values not in the allowed set (active, inactive, pending)

**Measurement Approaches**:
- Regular expression matching for formats
- Range checks for numeric values
- Lookup validation for domain values
- Business rule validation scripts

**Validity vs Accuracy**:
- A value can be valid but inaccurate: "john@example.com" is a valid email format, but John's actual email might be different
- Validity is about format and rules; accuracy is about truth

### Uniqueness

**Definition**: Each real-world entity is represented only once in the dataset.

**Examples of Uniqueness Issues**:
- Same customer appears twice with slightly different names
- Duplicate transactions due to retry logic
- Products with multiple SKUs for the same item

**Measurement Approaches**:
```
Uniqueness = (Distinct records / Total records) x 100
```

**Challenges**:
- Duplicate detection is complex (fuzzy matching)
- Some duplicates are intentional (same customer, different accounts)
- Deduplication can cause data loss if done incorrectly

**Detection Techniques**:
- Exact match on primary keys
- Fuzzy matching on names and addresses
- Statistical outlier detection for unusual patterns

### Relationships Between Dimensions

The dimensions are not independent. Issues in one dimension often indicate issues in others:

- **Incomplete data appears accurate**: Missing records are not wrong, they are just absent
- **Invalid data is usually inaccurate**: If a date is in the wrong format, it probably does not represent reality
- **Duplicates reduce uniqueness and accuracy**: Multiple conflicting records cannot all be correct
- **Stale data becomes inaccurate**: Truth changes, and old data diverges from reality

## Code Example

### Measuring Data Quality in SQL

```sql
-- Completeness: Measure null rates for key columns
SELECT 
    COUNT(*) as total_records,
    COUNT(customer_id) as customer_id_count,
    COUNT(email) as email_count,
    COUNT(created_at) as created_at_count,
    ROUND(100.0 * COUNT(email) / COUNT(*), 2) as email_completeness_pct
FROM customers;

-- Uniqueness: Find duplicate customer IDs
SELECT 
    customer_id,
    COUNT(*) as duplicate_count
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Validity: Check email format
SELECT 
    COUNT(*) as total_emails,
    COUNT(CASE WHEN email LIKE '%@%.%' THEN 1 END) as valid_format_count,
    ROUND(100.0 * COUNT(CASE WHEN email LIKE '%@%.%' THEN 1 END) / COUNT(*), 2) as validity_pct
FROM customers
WHERE email IS NOT NULL;

-- Consistency: Check referential integrity
SELECT 
    o.order_id,
    o.customer_id
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Timeliness: Check data freshness
SELECT 
    MAX(updated_at) as most_recent_update,
    DATEDIFF(hour, MAX(updated_at), CURRENT_TIMESTAMP) as hours_since_update
FROM customers;

-- Accuracy: Compare against known values (sample check)
SELECT 
    c.customer_id,
    c.email as warehouse_email,
    v.email as verified_email,
    CASE WHEN c.email = v.email THEN 'Match' ELSE 'Mismatch' END as accuracy_status
FROM customers c
INNER JOIN verified_customer_data v ON c.customer_id = v.customer_id;
```

### Data Quality Report in Python

```python
"""
Generate a data quality report measuring all six dimensions.
"""

import pandas as pd
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class QualityMetrics:
    """Data quality metrics for a dataset."""
    total_records: int
    completeness: Dict[str, float]
    uniqueness: float
    validity: Dict[str, float]
    timeliness_hours: float


def measure_completeness(df: pd.DataFrame, columns: list) -> Dict[str, float]:
    """Calculate completeness percentage for specified columns."""
    metrics = {}
    for col in columns:
        if col in df.columns:
            non_null = df[col].notna().sum()
            metrics[col] = round(100.0 * non_null / len(df), 2)
    return metrics


def measure_uniqueness(df: pd.DataFrame, key_column: str) -> float:
    """Calculate uniqueness based on key column."""
    unique_count = df[key_column].nunique()
    total_count = len(df)
    return round(100.0 * unique_count / total_count, 2)


def measure_email_validity(df: pd.DataFrame, email_column: str) -> float:
    """Check email format validity."""
    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    valid_emails = df[email_column].str.match(email_pattern, na=False).sum()
    total_emails = df[email_column].notna().sum()
    if total_emails == 0:
        return 0.0
    return round(100.0 * valid_emails / total_emails, 2)


def generate_quality_report(df: pd.DataFrame) -> QualityMetrics:
    """Generate comprehensive data quality report."""
    
    # Completeness for key columns
    completeness = measure_completeness(
        df, 
        ['customer_id', 'email', 'name', 'created_at']
    )
    
    # Uniqueness on primary key
    uniqueness = measure_uniqueness(df, 'customer_id')
    
    # Validity checks
    validity = {
        'email_format': measure_email_validity(df, 'email')
    }
    
    # Timeliness (hours since most recent record)
    if 'created_at' in df.columns:
        most_recent = pd.to_datetime(df['created_at']).max()
        hours_old = (pd.Timestamp.now() - most_recent).total_seconds() / 3600
    else:
        hours_old = -1
    
    return QualityMetrics(
        total_records=len(df),
        completeness=completeness,
        uniqueness=uniqueness,
        validity=validity,
        timeliness_hours=round(hours_old, 2)
    )


# Example usage
if __name__ == "__main__":
    # Sample data
    df = pd.DataFrame({
        'customer_id': [1, 2, 3, 3, 4],  # Note: duplicate ID
        'email': ['a@b.com', 'invalid', None, 'c@d.org', 'e@f.net'],
        'name': ['Alice', 'Bob', None, 'Charlie', 'Diana'],
        'created_at': ['2024-01-01', '2024-01-02', '2024-01-03', 
                       '2024-01-03', '2024-01-04']
    })
    
    report = generate_quality_report(df)
    print(f"Total Records: {report.total_records}")
    print(f"Completeness: {report.completeness}")
    print(f"Uniqueness: {report.uniqueness}%")
    print(f"Validity: {report.validity}")
    print(f"Hours Since Last Update: {report.timeliness_hours}")
```

## Summary

- **Data quality** is measured across six dimensions: accuracy, completeness, timeliness, consistency, validity, and uniqueness
- **Accuracy** asks whether data reflects reality; it is the hardest dimension to measure automatically
- **Completeness** measures the presence of required data elements at schema, column, and record levels
- **Timeliness** considers both currency (how recent) and latency (how quickly available)
- **Consistency** ensures data agrees across sources, over time, and in referential relationships
- **Validity** checks that data conforms to expected formats, ranges, and business rules
- **Uniqueness** ensures each entity is represented exactly once
- These dimensions are interconnected---issues in one often indicate issues in others

## Additional Resources

- [DAMA-DMBOK Data Quality Dimensions](https://www.dama.org/cpages/body-of-knowledge) - Industry standard framework
- [MIT CDOIQ Data Quality Research](https://cdoiq.mit.edu/) - Academic research on data quality
- [ISO 8000 Data Quality Standard](https://www.iso.org/standard/50798.html) - International standard for data quality
