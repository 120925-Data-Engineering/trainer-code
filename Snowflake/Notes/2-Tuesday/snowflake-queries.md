# Snowflake Queries and Semi-Structured Data

## Learning Objectives

- Write effective SQL queries in Snowflake
- Understand the VARIANT data type for semi-structured data
- Query JSON, Parquet, and other semi-structured formats
- Apply LATERAL FLATTEN to extract nested and array data

## Why This Matters

Modern data pipelines frequently ingest semi-structured data such as JSON from APIs, event logs, and IoT devices. Snowflake's native support for semi-structured data eliminates the need for complex ETL preprocessing, allowing you to query JSON alongside traditional relational data. This capability is essential for implementing the Bronze layer of your Medallion architecture, where raw data arrives in its original format.

## The Concept

### Standard SQL Queries

Snowflake supports ANSI SQL with extensions. Standard query patterns work as expected:

```sql
-- Basic SELECT
SELECT customer_id, customer_name, email
FROM customers
WHERE status = 'ACTIVE'
ORDER BY customer_name;

-- Aggregation
SELECT 
    DATE_TRUNC('month', order_date) AS order_month,
    COUNT(*) AS order_count,
    SUM(order_total) AS total_revenue
FROM orders
GROUP BY order_month
ORDER BY order_month;

-- Joins
SELECT 
    o.order_id,
    o.order_date,
    c.customer_name,
    p.product_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id;

-- Subqueries and CTEs
WITH monthly_totals AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', order_date) AS month,
        SUM(order_total) AS monthly_spend
    FROM orders
    GROUP BY customer_id, month
)
SELECT * FROM monthly_totals WHERE monthly_spend > 1000;
```

### The VARIANT Data Type

Snowflake's **VARIANT** data type stores semi-structured data (JSON, Avro, ORC, Parquet, XML) in a single column. The data is stored in a compressed, efficient format that supports direct querying.

**Creating a Table with VARIANT:**
```sql
CREATE TABLE raw_events (
    event_id INTEGER,
    event_timestamp TIMESTAMP_NTZ,
    event_data VARIANT  -- Stores JSON or other semi-structured data
);
```

**Inserting JSON Data:**
```sql
-- Direct JSON insert using PARSE_JSON
INSERT INTO raw_events (event_id, event_timestamp, event_data)
VALUES (
    1,
    CURRENT_TIMESTAMP(),
    PARSE_JSON('{
        "user_id": "U123",
        "action": "purchase",
        "items": [
            {"sku": "SKU001", "qty": 2, "price": 29.99},
            {"sku": "SKU002", "qty": 1, "price": 49.99}
        ],
        "metadata": {
            "browser": "Chrome",
            "device": "mobile"
        }
    }')
);
```

### Querying VARIANT Data

Access nested elements using **colon notation** (`:`) and **bracket notation** (`[]`):

**Colon Notation (Object Keys):**
```sql
SELECT 
    event_id,
    event_data:user_id AS user_id,
    event_data:action AS action,
    event_data:metadata:browser AS browser
FROM raw_events;
```

**Note:** Results from VARIANT columns are returned as VARIANT type with quotes. Cast to specific types:

```sql
SELECT 
    event_id,
    event_data:user_id::STRING AS user_id,
    event_data:action::STRING AS action,
    event_data:metadata:browser::STRING AS browser
FROM raw_events;
```

**Bracket Notation (Array Elements):**
```sql
SELECT 
    event_id,
    event_data:items[0]:sku::STRING AS first_item_sku,
    event_data:items[0]:price::DECIMAL(10,2) AS first_item_price
FROM raw_events;
```

### LATERAL FLATTEN for Arrays and Objects

**FLATTEN** is a table function that expands arrays or objects into rows. Combined with **LATERAL**, it allows you to join the flattened results with the parent row.

**Flatten an Array:**
```sql
-- Each array element becomes a separate row
SELECT 
    e.event_id,
    e.event_data:user_id::STRING AS user_id,
    f.value:sku::STRING AS item_sku,
    f.value:qty::INTEGER AS item_quantity,
    f.value:price::DECIMAL(10,2) AS item_price
FROM raw_events e,
LATERAL FLATTEN(input => e.event_data:items) f;
```

**FLATTEN Output Columns:**

| Column | Description |
|--------|-------------|
| `SEQ` | Sequence number for rows produced |
| `KEY` | Key for object elements (null for arrays) |
| `PATH` | Path to the element |
| `INDEX` | Array index (null for objects) |
| `VALUE` | The actual element value |
| `THIS` | The entire array/object being flattened |

**Flatten with Additional Context:**
```sql
SELECT 
    e.event_id,
    f.index AS item_index,
    f.value:sku::STRING AS sku,
    f.value:qty::INTEGER AS qty
FROM raw_events e,
LATERAL FLATTEN(input => e.event_data:items) f
WHERE f.value:qty::INTEGER > 1;
```

**Nested FLATTEN:**

For deeply nested structures, chain multiple FLATTEN operations:

```sql
-- Assume items contain a 'tags' array
SELECT 
    e.event_id,
    items.value:sku::STRING AS sku,
    tags.value::STRING AS tag
FROM raw_events e,
LATERAL FLATTEN(input => e.event_data:items) items,
LATERAL FLATTEN(input => items.value:tags) tags;
```

### FLATTEN on Objects

FLATTEN also works on objects, turning key-value pairs into rows:

```sql
-- Flatten the metadata object
SELECT 
    e.event_id,
    f.key AS metadata_key,
    f.value::STRING AS metadata_value
FROM raw_events e,
LATERAL FLATTEN(input => e.event_data:metadata) f;

-- Result:
-- event_id | metadata_key | metadata_value
-- 1        | browser      | Chrome
-- 1        | device       | mobile
```

### Recursive FLATTEN

Use `RECURSIVE => TRUE` for nested arrays/objects:

```sql
SELECT 
    e.event_id,
    f.path AS element_path,
    f.key AS element_key,
    f.value AS element_value
FROM raw_events e,
LATERAL FLATTEN(input => e.event_data, recursive => TRUE) f;
```

### Loading Semi-Structured Files

Load JSON, Parquet, or other formats directly into VARIANT columns:

```sql
-- Create file format for JSON
CREATE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- Create stage (or use existing)
CREATE STAGE json_stage;

-- Copy into VARIANT column
COPY INTO raw_events (event_id, event_timestamp, event_data)
FROM (
    SELECT 
        $1:event_id::INTEGER,
        $1:timestamp::TIMESTAMP_NTZ,
        $1  -- The entire JSON object
    FROM @json_stage
)
FILE_FORMAT = (FORMAT_NAME = json_format);
```

### Querying Parquet Directly

Snowflake can query Parquet files in stages without loading:

```sql
-- Query Parquet file directly from stage
SELECT 
    $1:order_id::STRING,
    $1:customer_id::STRING,
    $1:order_date::DATE
FROM @my_stage/orders.parquet
(FILE_FORMAT => 'parquet_format');
```

### Common Patterns

**Check if Key Exists:**
```sql
SELECT *
FROM raw_events
WHERE event_data:metadata:browser IS NOT NULL;
```

**Handle Missing Keys Safely:**
```sql
SELECT 
    COALESCE(event_data:optional_field::STRING, 'default_value') AS field_value
FROM raw_events;
```

**Filter on Nested Values:**
```sql
SELECT *
FROM raw_events
WHERE event_data:action::STRING = 'purchase'
  AND event_data:metadata:device::STRING = 'mobile';
```

**Aggregate Array Lengths:**
```sql
SELECT 
    event_id,
    ARRAY_SIZE(event_data:items) AS item_count
FROM raw_events;
```

## Summary

- Snowflake supports standard **ANSI SQL** with powerful extensions
- The **VARIANT** data type natively stores JSON, Parquet, and other semi-structured formats
- Use **colon notation** (`:`) to access nested object keys
- Use **bracket notation** (`[]`) to access array elements
- **Cast** VARIANT results to specific types using `::TYPE`
- **LATERAL FLATTEN** expands arrays and objects into rows for relational querying
- Semi-structured support eliminates preprocessing, enabling faster Bronze layer ingestion

## Additional Resources

- [Snowflake Documentation: Querying Semi-Structured Data](https://docs.snowflake.com/en/user-guide/querying-semistructured)
- [Snowflake Documentation: FLATTEN](https://docs.snowflake.com/en/sql-reference/functions/flatten)
- [Snowflake Documentation: VARIANT Data Type](https://docs.snowflake.com/en/sql-reference/data-types-semistructured)
