# Slowly Changing Dimensions (SCD)

## Learning Objectives

- Understand what slowly changing dimensions are
- Learn the different SCD types (0, 1, 2, 3, 4, 6)
- Know when to use each SCD type
- Apply SCD patterns in data warehouse design

## Why This Matters

Dimension data changes over time: customers move, products get recategorized, employees change departments. How you handle these changes affects your ability to analyze historical data accurately. SCD patterns provide standard approaches for managing dimension changes.

## The Concept

### What is a Slowly Changing Dimension?

A Slowly Changing Dimension (SCD) is a dimension whose attributes change slowly and unpredictably over time, rather than at regular intervals.

**Examples:**
- Customer address changes when they move
- Product category changes during reorganization
- Employee department changes during transfer
- Price tier changes based on market conditions

**The Challenge:**
```
Current State:                Original State:
Alice lives in NYC            Alice lived in LA

Question: When orders from 2020 were made, where did Alice live?
```

### SCD Type 0: Retain Original

Keep the original value forever; never update.

**Implementation:**
```sql
-- Original insert
INSERT INTO dim_customer (customer_key, name, city)
VALUES (1, 'Alice', 'Los Angeles');

-- Later: Alice moved to NYC
-- Type 0: No update! Keep 'Los Angeles' forever
```

**Use When:**
- Original value has special meaning (signup date, birth date)
- Value should never change for historical accuracy
- Regulatory requirement to preserve original

### SCD Type 1: Overwrite

Replace the old value with the new value. No history is kept.

**Implementation:**
```sql
-- Original
| customer_key | name  | city        |
|--------------|-------|-------------|
| 1            | Alice | Los Angeles |

-- After update (Type 1)
UPDATE dim_customer SET city = 'New York' WHERE customer_key = 1;

| customer_key | name  | city        |
|--------------|-------|-------------|
| 1            | Alice | New York    |

-- Los Angeles is lost forever
```

**Use When:**
- Historical values are not important
- Data corrections (fixing typos)
- Simplicity is valued over history
- Changes are rare and minor

**Advantages:**
- Simple to implement
- No dimension growth
- Always shows current state

**Disadvantages:**
- History is lost
- Cannot analyze "as was" states

### SCD Type 2: Add New Row (History Tracking)

Create a new row for each change, preserving complete history.

**Implementation:**
```sql
dim_customer
+------+-------+-------------+------------+------------+-----------+
| key  | name  | city        | start_date | end_date   | is_current|
+------+-------+-------------+------------+------------+-----------+
| 1    | Alice | Los Angeles | 2020-01-01 | 2023-06-30 | N         |
| 2    | Alice | New York    | 2023-07-01 | 9999-12-31 | Y         |
+------+-------+-------------+------------+------------+-----------+
```

**Key Components:**
- **Surrogate key**: New key for each version
- **Natural key**: Business identifier stays same
- **Start date**: When version became effective
- **End date**: When version ended (often far future for current)
- **Current flag**: Easy filter for current records

**Loading New Version:**
```sql
-- Step 1: Close existing record
UPDATE dim_customer
SET end_date = '2023-06-30', is_current = 'N'
WHERE customer_id = 'CUST-001' AND is_current = 'Y';

-- Step 2: Insert new version
INSERT INTO dim_customer 
    (customer_key, customer_id, name, city, start_date, end_date, is_current)
VALUES 
    (2, 'CUST-001', 'Alice', 'New York', '2023-07-01', '9999-12-31', 'Y');
```

**Querying History:**
```sql
-- Current state
SELECT * FROM dim_customer WHERE is_current = 'Y';

-- Historical state (point-in-time)
SELECT * FROM dim_customer
WHERE '2022-03-15' BETWEEN start_date AND end_date;
```

**Use When:**
- Historical accuracy is critical
- Regulatory/audit requirements
- Need "as-was" analysis

**Advantages:**
- Complete history preserved
- Accurate historical analysis

**Disadvantages:**
- Dimension table grows
- ETL more complex
- Queries need date filtering

### SCD Type 3: Add New Column

Add a column for the previous value. Limited history.

**Implementation:**
```sql
dim_customer
+------+-------+-------------+-----------------+
| key  | name  | current_city| previous_city   |
+------+-------+-------------+-----------------+
| 1    | Alice | New York    | Los Angeles     |
+------+-------+-------------+-----------------+
```

**Use When:**
- Only care about current and previous values
- Change is rare (once or twice)
- Simple analysis needs

**Advantages:**
- Simple structure
- No dimension growth

**Disadvantages:**
- Limited history (only 1-2 versions)
- Schema changes for more history

### SCD Type 4: Add Mini-Dimension

Separate rapidly changing attributes into a mini-dimension.

**Implementation:**
```sql
-- Main customer dimension (static attributes)
dim_customer
+------+-------+------------+---------------+
| key  | name  | signup_date| loyalty_level |
+------+-------+------------+---------------+
| 1    | Alice | 2020-01-01 | Gold          |
+------+-------+------------+---------------+

-- Mini-dimension for changing demographics (SCD Type 1 or 2)
dim_customer_profile
+------+----------+--------+-------------+
| key  | age_band | income | risk_score  |
+------+----------+--------+-------------+
| 101  | 30-40    | High   | Low         |
| 102  | 40-50    | High   | Medium      |
+------+----------+--------+-------------+

-- Fact table references both
fact_sales
+----------+------+-------------+-------+
| sale_key | cust | cust_profile| amount|
+----------+------+-------------+-------+
| 1        | 1    | 101         | 500   |
| 2        | 1    | 102         | 750   |
+----------+------+-------------+-------+
```

**Use When:**
- Some attributes change frequently (demographics)
- Others change slowly (name, ID)
- Want to separate concerns

### SCD Type 6: Hybrid (1 + 2 + 3)

Combines Types 1, 2, and 3.

**Implementation:**
```sql
dim_customer
+------+-------+-------------+-------------+----------+----------+---------+
| key  | name  | current_city| history_city| start    | end      | current |
+------+-------+-------------+-------------+----------+----------+---------+
| 1    | Alice | New York    | Los Angeles | 2020-01  | 2023-06  | N       |
| 2    | Alice | New York    | New York    | 2023-07  | 9999-12  | Y       |
+------+-------+-------------+-------------+----------+----------+---------+
```

**Key Points:**
- Type 2: Multiple rows with date ranges
- Type 3: Previous value column
- Type 1: Current value overwritten in all rows

**Use When:**
- Need full Type 2 history
- Also want easy access to current value on all rows
- Complex analysis requirements

### SCD Type Comparison

| Type | History | Complexity | Use Case |
|------|---------|------------|----------|
| 0 | None (original only) | Minimal | Fixed attributes |
| 1 | None (overwrite) | Minimal | Corrections, unimportant changes |
| 2 | Full (new rows) | High | Audit, historical analysis |
| 3 | Limited (columns) | Low | Before/after comparison |
| 4 | Separated | Medium | Mixed change rates |
| 6 | Full + current | High | Complex requirements |

### Choosing SCD Type

```
Is history important?
    No --> Type 0 or Type 1
    Yes:
        Need complete history?
            Yes --> Type 2 (or Type 6 for complexity)
            No --> Type 3
        
        Attributes change at different rates?
            Yes --> Type 4 (mini-dimension)
```

### BigQuery SCD Type 2 Pattern

```sql
-- Merge pattern for SCD Type 2
MERGE dim_customer AS target
USING staging_customer AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE
WHEN MATCHED AND (target.city != source.city) THEN
    UPDATE SET 
        end_date = CURRENT_DATE(),
        is_current = FALSE
WHEN NOT MATCHED THEN
    INSERT (customer_key, customer_id, name, city, start_date, end_date, is_current)
    VALUES (GENERATE_UUID(), source.customer_id, source.name, source.city, 
            CURRENT_DATE(), DATE '9999-12-31', TRUE);

-- Insert new versions for changed records
INSERT INTO dim_customer 
SELECT GENERATE_UUID(), customer_id, name, city, CURRENT_DATE(), DATE '9999-12-31', TRUE
FROM staging_customer s
WHERE EXISTS (
    SELECT 1 FROM dim_customer d 
    WHERE d.customer_id = s.customer_id 
    AND d.is_current = FALSE 
    AND d.end_date = CURRENT_DATE()
);
```

## Summary

- **Type 0**: Retain original, never update
- **Type 1**: Overwrite old with new, no history
- **Type 2**: Create new row for each change, full history
- **Type 3**: Add column for previous value, limited history
- **Type 4**: Split into mini-dimensions for different change rates
- **Type 6**: Hybrid combining Types 1, 2, and 3
- Choose based on historical analysis needs and complexity tolerance
- Type 2 is most common for dimension history tracking

## Additional Resources

- [Kimball SCD Techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
- [SCD Implementation Guide](https://www.sqlservercentral.com/articles/understanding-slowly-changing-dimensions-scd)
- [BigQuery SCD Patterns](https://cloud.google.com/architecture/building-slowly-changing-dimensions-bigquery)
