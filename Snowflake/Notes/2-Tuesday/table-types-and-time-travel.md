# Table Types and Time Travel

## Learning Objectives
- Understand the three types of tables in Snowflake (Permanent, Transient, Temporary)
- Know when to use each table type based on durability and cost requirements
- Use Time Travel to query historical data and recover from mistakes
- Create and query views, including secure views

## Why This Matters

In any data platform, you need flexibility in how data is stored. Some tables hold critical production data that must be recoverable at all costs. Others are temporary staging areas that can be recreated if lost. Snowflake provides three table types to match these different needs, each with different durability guarantees and storage costs.

Time Travel is one of Snowflake's most powerful features. Imagine being able to "undo" an accidental DELETE or UPDATE, or query your data exactly as it looked yesterday. This capability is built directly into Snowflake - no additional setup or configuration required.

## The Three Table Types

Snowflake offers three types of tables, each designed for different use cases:

### Permanent Tables (Default)

Permanent tables are the default and most durable option. They provide:

- **Time Travel**: Up to 90 days (Enterprise Edition) or 1 day (Standard Edition)
- **Fail-Safe**: 7 additional days of disaster recovery managed by Snowflake
- **Use Case**: Production tables, critical business data, anything you cannot afford to lose

```sql
-- Permanent table (default, no keyword needed)
CREATE TABLE orders (
    order_id STRING,
    customer_id STRING,
    amount DECIMAL(10,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Transient Tables

Transient tables reduce storage costs by eliminating Fail-Safe:

- **Time Travel**: Up to 1 day only
- **Fail-Safe**: None
- **Use Case**: Staging tables, intermediate processing, data that can be recreated

```sql
-- Transient table - note the TRANSIENT keyword
CREATE TRANSIENT TABLE orders_staging (
    order_id STRING,
    customer_id STRING,
    amount DECIMAL(10,2)
);
```

### Temporary Tables

Temporary tables exist only for the duration of your session:

- **Time Travel**: Up to 1 day (while session is active)
- **Fail-Safe**: None
- **Scope**: Session-only - the table disappears when you disconnect
- **Use Case**: Session-specific calculations, intermediate results within a script

```sql
-- Temporary table - only exists in your current session
CREATE TEMPORARY TABLE temp_calculations (
    metric_name STRING,
    metric_value DECIMAL(10,2)
);
```

> **Spark Bridge**: Temporary tables in Snowflake are similar to `createTempView` in Spark, but they persist across multiple queries within the same session rather than just within a single Spark application.

## Comparison Summary

| Table Type  | Time Travel    | Fail-Safe | Storage Cost | Use Case               |
|-------------|----------------|-----------|--------------|------------------------|
| PERMANENT   | Up to 90 days  | 7 days    | Highest      | Production, critical   |
| TRANSIENT   | Up to 1 day    | None      | Medium       | Staging, intermediate  |
| TEMPORARY   | Up to 1 day    | None      | Lowest       | Session calculations   |

## Time Travel: Querying the Past

Time Travel allows you to access historical versions of your data. This is like having an automatic "undo" button for your tables.

### Query Data from the Past

Use the `AT` clause to query data as it existed at a specific point:

```sql
-- Query data as it was 60 seconds ago
SELECT * FROM orders AT (OFFSET => -60);

-- Query data at a specific timestamp
SELECT * FROM orders AT (TIMESTAMP => '2024-01-15 10:00:00'::TIMESTAMP);

-- Query data before a specific query was executed
SELECT * FROM orders BEFORE (STATEMENT => '<query_id>');
```

### Recover Deleted Data

Made a mistake? Time Travel lets you recover:

```sql
-- Oops! We accidentally deleted important records
DELETE FROM orders WHERE customer_id = 'C100';

-- Restore the deleted records from 2 minutes ago
INSERT INTO orders
SELECT * FROM orders AT (OFFSET => -120) 
WHERE customer_id = 'C100';
```

### Clone from a Point in Time

Need to restore an entire table? Create a clone from history:

```sql
-- Create a copy of the table as it was 1 hour ago
CREATE TABLE orders_restored CLONE orders AT (OFFSET => -3600);
```

### Recover Dropped Tables

Even dropped tables can be recovered during the retention period:

```sql
-- Accidentally dropped a table?
DROP TABLE important_data;

-- Bring it back!
UNDROP TABLE important_data;
```

> **Key Insight**: In regular Spark, once you overwrite or delete data, it's gone forever. Snowflake automatically keeps historical versions, giving you a safety net without any extra setup.

## Views in Snowflake

Views work similarly to Spark SQL views - they are saved queries that can be referenced like tables.

### Standard Views

```sql
-- Create a view that filters high-value orders
CREATE OR REPLACE VIEW v_high_value_orders AS
SELECT 
    order_id,
    customer_id,
    amount
FROM orders
WHERE amount > 1000;

-- Query the view like a table
SELECT * FROM v_high_value_orders;
```

### Secure Views

Secure views hide the view definition from users who query them. This is useful when:
- The query logic itself is sensitive
- You want to prevent users from inferring underlying data through query optimization

```sql
-- Create a secure view - definition is hidden from non-owners
CREATE OR REPLACE SECURE VIEW v_customer_summary AS
SELECT 
    region,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_value
FROM customers
GROUP BY region;
```

## Practical Guidelines

When choosing a table type, consider these questions:

1. **Is this production data?** Use PERMANENT tables.
2. **Can this be recreated from source?** TRANSIENT is often sufficient for staging.
3. **Is this just for this session?** TEMPORARY tables keep your schema clean.
4. **How long do you need history?** Adjust Time Travel retention accordingly.

```sql
-- Set Time Travel retention (in days) for a table
ALTER TABLE orders SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- Check current retention settings
SHOW TABLES LIKE 'orders';
```

## Summary

- **Three table types** give you control over durability vs. cost
- **Permanent**: Full protection, highest cost - use for critical data
- **Transient**: No Fail-Safe, lower cost - use for staging/intermediate
- **Temporary**: Session-scoped - use for calculations
- **Time Travel** lets you query historical data and recover from mistakes
- **UNDROP** can recover accidentally dropped tables
- **Secure views** hide query logic from unauthorized users

## Additional Resources
- [Snowflake Table Types Documentation](https://docs.snowflake.com/en/user-guide/tables-temp-transient)
- [Time Travel Guide](https://docs.snowflake.com/en/user-guide/data-time-travel)
- [Working with Views](https://docs.snowflake.com/en/user-guide/views-introduction)
