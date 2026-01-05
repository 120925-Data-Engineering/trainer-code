# Snowflake Schemas and Objects

## Learning Objectives

- Understand Snowflake's object hierarchy (Account, Database, Schema)
- Identify the different object types available in Snowflake
- Apply naming conventions for consistent, maintainable data architectures
- Recognize how schemas organize objects within the Medallion architecture

## Why This Matters

Effective schema organization is fundamental to building maintainable data platforms. In your journey *From Data Lakes to Data Warehouses*, understanding Snowflake's object hierarchy enables you to implement the Medallion architecture you learned about yesterday, enforce access control, and create intuitive navigation for data consumers.

## The Concept

### Object Hierarchy

Snowflake organizes objects in a three-level hierarchy:

```
Account
  |
  +-- Database
        |
        +-- Schema
              |
              +-- Tables, Views, Stages, File Formats, Sequences, etc.
```

**Account:**
- The top-level container for all Snowflake objects
- Unique within a cloud region
- Contains all databases, warehouses, users, and roles

**Database:**
- A logical grouping of schemas
- Separates major data domains or environments
- Examples: `RAW_DB`, `ANALYTICS_DB`, `DEV_DB`

**Schema:**
- A logical grouping of database objects
- Provides namespace isolation within a database
- Examples: `BRONZE`, `SILVER`, `GOLD`, `STAGING`

### Fully Qualified Names

Objects can be referenced using fully qualified names:

```sql
-- Fully qualified: DATABASE.SCHEMA.OBJECT
SELECT * FROM ANALYTICS_DB.GOLD.DAILY_SALES;

-- With context set, shorter references work
USE DATABASE ANALYTICS_DB;
USE SCHEMA GOLD;
SELECT * FROM DAILY_SALES;
```

### Database Objects

Snowflake supports various object types within schemas:

#### Tables

Tables store structured data in rows and columns.

**Table Types:**

| Type | Description | Time Travel | Fail-Safe |
|------|-------------|-------------|-----------|
| **Permanent** | Default table type, full durability | Yes (up to 90 days) | Yes (7 days) |
| **Transient** | Reduced storage costs, less durability | Yes (up to 1 day) | No |
| **Temporary** | Session-scoped, auto-dropped | Yes (up to 1 day) | No |

```sql
-- Permanent table (default)
CREATE TABLE orders (
    order_id STRING,
    customer_id STRING,
    order_date DATE
);

-- Transient table (reduced costs for staging data)
CREATE TRANSIENT TABLE staging_orders (
    raw_data VARIANT
);

-- Temporary table (session-only)
CREATE TEMPORARY TABLE temp_calculations (
    metric_name STRING,
    metric_value DECIMAL(10,2)
);
```

#### Views

Views are saved queries that appear as virtual tables.

**View Types:**

| Type | Description | Performance |
|------|-------------|-------------|
| **Standard View** | Query executed at runtime | No pre-computation |
| **Secure View** | Hides definition from unauthorized users | Same as standard |
| **Materialized View** | Pre-computed, auto-refreshed | Faster queries, storage cost |

```sql
-- Standard view
CREATE VIEW v_active_customers AS
SELECT customer_id, customer_name, email
FROM customers
WHERE status = 'ACTIVE';

-- Secure view (Enterprise+)
CREATE SECURE VIEW v_sensitive_data AS
SELECT * FROM customer_pii;

-- Materialized view (Enterprise+)
CREATE MATERIALIZED VIEW mv_daily_sales AS
SELECT order_date, SUM(amount) AS total_sales
FROM orders
GROUP BY order_date;
```

#### Stages

Stages are storage locations for data files used in loading and unloading operations.

**Stage Types:**

| Type | Description | Use Case |
|------|-------------|----------|
| **Internal (User)** | Personal stage per user | Ad-hoc uploads |
| **Internal (Table)** | Auto-created per table | Table-specific loads |
| **Internal (Named)** | Explicitly created | Shared team uploads |
| **External** | Points to cloud storage | S3, Azure Blob, GCS |

```sql
-- Create an internal named stage
CREATE STAGE my_stage;

-- Create an external stage pointing to S3
CREATE STAGE s3_stage
    URL = 's3://my-bucket/data/'
    CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...');

-- List files in a stage
LIST @my_stage;
```

#### File Formats

File formats define how data files should be parsed during loading.

```sql
-- CSV file format
CREATE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '');

-- JSON file format
CREATE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- Parquet file format
CREATE FILE FORMAT parquet_format
    TYPE = 'PARQUET';
```

#### Sequences

Sequences generate unique numeric values, useful for surrogate keys.

```sql
-- Create a sequence
CREATE SEQUENCE order_seq START = 1 INCREMENT = 1;

-- Use in INSERT
INSERT INTO orders (order_id, customer_id)
VALUES (order_seq.NEXTVAL, 'CUST001');

-- Get current value
SELECT order_seq.NEXTVAL;
```

### Other Objects

| Object | Purpose |
|--------|---------|
| **Streams** | Track data changes (CDC) for incremental processing |
| **Tasks** | Schedule and automate SQL statements |
| **Pipes** | Enable continuous data loading (Snowpipe) |
| **Procedures** | Stored logic using JavaScript or SQL |
| **Functions (UDFs)** | Custom functions in SQL, JavaScript, or Python |

You will explore Streams, Tasks, and UDFs in detail on Wednesday.

## Naming Conventions

Consistent naming improves discoverability and maintainability.

### Recommended Conventions

**Databases:**
```
<LAYER>_DB or <DOMAIN>_DB
Examples: RAW_DB, ANALYTICS_DB, FINANCE_DB
```

**Schemas:**
```
<LAYER> or <DOMAIN>
Examples: BRONZE, SILVER, GOLD, STAGING, REPORTING
```

**Tables:**
```
<entity>_<descriptor>
Examples: orders, customer_addresses, daily_sales_metrics
```

**Views:**
```
v_<entity> or vw_<entity>
Examples: v_active_customers, vw_order_summary
```

**Stages:**
```
stg_<source>_<descriptor>
Examples: stg_s3_raw_orders, stg_internal_uploads
```

### Case Sensitivity

Snowflake converts unquoted identifiers to uppercase. Use consistent casing:

```sql
-- These are equivalent
SELECT * FROM orders;
SELECT * FROM ORDERS;
SELECT * FROM Orders;

-- To preserve case, use double quotes (not recommended)
SELECT * FROM "Mixed_Case_Table";
```

**Best Practice:** Use uppercase or lowercase consistently without quotes.

## Schema Organization for Medallion Architecture

Applying what you learned yesterday, here is how you might organize schemas:

```
ANALYTICS_DB
  |
  +-- BRONZE (raw data, VARIANT tables)
  |     +-- raw_orders
  |     +-- raw_customers
  |
  +-- SILVER (cleansed data, typed tables)
  |     +-- orders
  |     +-- customers
  |     +-- products
  |
  +-- GOLD (aggregated, denormalized)
        +-- daily_order_metrics
        +-- customer_360
        +-- sales_dashboard
```

## Summary

- Snowflake's hierarchy: **Account > Database > Schema > Objects**
- Key object types include **Tables** (permanent, transient, temporary), **Views**, **Stages**, and **File Formats**
- Stages and File Formats are essential for data loading operations
- Use consistent **naming conventions** for maintainability
- Schema organization supports the **Medallion architecture** pattern

## Additional Resources

- [Snowflake Documentation: Database Objects](https://docs.snowflake.com/en/user-guide/databases-schemas-objects)
- [Snowflake Documentation: Stages](https://docs.snowflake.com/en/user-guide/data-load-overview)
- [Snowflake Documentation: Views](https://docs.snowflake.com/en/user-guide/views-introduction)
