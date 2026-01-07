# Star Schema

## Learning Objectives

- Understand the star schema structure
- Learn how to design fact and dimension tables
- Know the benefits and trade-offs of star schemas
- Apply star schema design to practical scenarios

## Why This Matters

The star schema is the most widely used dimensional modeling pattern. It is the foundation for data warehouses, BI tools, and analytical systems. Mastering star schema design enables you to build intuitive, high-performance analytics solutions.

## The Concept

### What is a Star Schema?

A star schema consists of one central fact table surrounded by dimension tables, forming a star shape when visualized.

```
                    +----------------+
                    |   dim_date     |
                    +-------+--------+
                            |
+----------------+   +------+-------+   +----------------+
|  dim_customer  |---|  fact_sales  |---|  dim_product   |
+----------------+   +------+-------+   +----------------+
                            |
                    +-------+--------+
                    |   dim_store    |
                    +----------------+
```

### Components

**Fact Table (Center):**
- Contains measurable business metrics
- Has foreign keys to all dimensions
- Usually the largest table

**Dimension Tables (Points of Star):**
- Contain descriptive attributes
- Have primary keys matching fact FKs
- Provide context for analysis

### Star Schema Example: Retail Sales

**Fact Table:**
```sql
CREATE TABLE fact_sales (
    sale_key INT64,           -- Surrogate key (optional)
    date_key INT64,           -- FK to dim_date
    customer_key INT64,       -- FK to dim_customer
    product_key INT64,        -- FK to dim_product
    store_key INT64,          -- FK to dim_store
    quantity INT64,           -- Measure
    unit_price NUMERIC(10,2), -- Measure
    discount NUMERIC(10,2),   -- Measure
    total_amount NUMERIC(10,2)-- Measure
);
```

**Date Dimension:**
```sql
CREATE TABLE dim_date (
    date_key INT64,           -- PK (often YYYYMMDD format)
    full_date DATE,
    day_of_week STRING,
    day_name STRING,
    day_of_month INT64,
    month INT64,
    month_name STRING,
    quarter INT64,
    year INT64,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);
```

**Customer Dimension:**
```sql
CREATE TABLE dim_customer (
    customer_key INT64,       -- Surrogate PK
    customer_id STRING,       -- Natural/business key
    name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    segment STRING,
    tier STRING
);
```

**Product Dimension:**
```sql
CREATE TABLE dim_product (
    product_key INT64,        -- Surrogate PK
    product_id STRING,        -- Natural key (SKU)
    name STRING,
    description STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    supplier STRING,
    unit_cost NUMERIC(10,2)
);
```

**Store Dimension:**
```sql
CREATE TABLE dim_store (
    store_key INT64,          -- Surrogate PK
    store_id STRING,          -- Natural key
    store_name STRING,
    address STRING,
    city STRING,
    state STRING,
    region STRING,
    country STRING,
    store_type STRING,
    open_date DATE
);
```

### Querying the Star Schema

Star schemas enable intuitive, powerful queries:

**Sales by Month and Category:**
```sql
SELECT
    d.year,
    d.month_name,
    p.category,
    SUM(f.total_amount) AS total_sales,
    SUM(f.quantity) AS units_sold
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY d.year, d.month_name, p.category
ORDER BY d.year, d.month_name;
```

**Top Customers by Region:**
```sql
SELECT
    s.region,
    c.name AS customer_name,
    SUM(f.total_amount) AS total_spent
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_store s ON f.store_key = s.store_key
GROUP BY s.region, c.name
ORDER BY s.region, total_spent DESC;
```

### Surrogate Keys

Dimension tables use surrogate keys (generated integers) instead of natural keys.

**Why Surrogate Keys?**
- Natural keys can change (customer email, product SKU)
- Surrogate keys are stable and efficient
- Enable tracking history (SCD Type 2)
- Simple joins (integer comparison)

```sql
-- Natural key might change
customer_id: "CUST-001" -> "CUST-A001" (after migration)

-- Surrogate key is stable
customer_key: 12345 (never changes)
```

### Benefits of Star Schema

| Benefit | Description |
|---------|-------------|
| Simplicity | Easy to understand and query |
| Performance | Fewer joins, efficient aggregation |
| BI Tool Friendly | Works well with Tableau, Power BI, Looker |
| Flexibility | Add dimensions without major redesign |
| Denormalized | Fast reads, predictable query patterns |

### Star Schema Design Guidelines

**1. One Fact Table Per Business Process**
```
Sales Process --> fact_sales
Inventory --> fact_inventory
Orders --> fact_orders
```

**2. Include All Keys in Fact Table**
```sql
-- Every relevant dimension should have a FK in the fact
fact_sales: date_key, customer_key, product_key, store_key, promo_key
```

**3. Use Descriptive Dimension Columns**
```sql
-- Good: Include useful attributes
dim_date: year, quarter, month_name, day_name, is_holiday

-- Avoid: Missing useful attributes
dim_date: date_value (only)
```

**4. Handle Nulls with Unknown Records**
```sql
-- Instead of NULL foreign keys, use an "Unknown" dimension row
INSERT INTO dim_customer (customer_key, name) VALUES (-1, 'Unknown');

-- Then reference it when customer is missing
UPDATE fact_sales SET customer_key = -1 WHERE customer_key IS NULL;
```

**5. Conformed Dimensions**
Share dimension tables across multiple fact tables:
```
fact_sales ----+
               |---> dim_date (shared)
fact_inventory-+

fact_sales ----+
               |---> dim_product (shared)
fact_inventory-+
```

### Physical Implementation in BigQuery

```sql
-- Create date dimension
CREATE TABLE warehouse.dim_date AS
SELECT
    CAST(FORMAT_DATE('%Y%m%d', d) AS INT64) AS date_key,
    d AS full_date,
    FORMAT_DATE('%A', d) AS day_name,
    EXTRACT(DAYOFWEEK FROM d) AS day_of_week,
    EXTRACT(DAY FROM d) AS day_of_month,
    EXTRACT(MONTH FROM d) AS month,
    FORMAT_DATE('%B', d) AS month_name,
    EXTRACT(QUARTER FROM d) AS quarter,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(DAYOFWEEK FROM d) IN (1, 7) AS is_weekend
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31')) AS d;

-- Create fact table with partitioning
CREATE TABLE warehouse.fact_sales (
    date_key INT64,
    customer_key INT64,
    product_key INT64,
    store_key INT64,
    quantity INT64,
    unit_price NUMERIC(10,2),
    total_amount NUMERIC(10,2)
)
PARTITION BY RANGE_BUCKET(date_key, GENERATE_ARRAY(20200101, 20301231, 10000))
CLUSTER BY customer_key, product_key;
```

### Star Schema vs. Normalized

| Aspect | Star Schema | Normalized (3NF) |
|--------|-------------|------------------|
| Joins | Few (star pattern) | Many (complex) |
| Redundancy | Intentional | Minimized |
| Query speed | Fast | Slower |
| Updates | Complex | Simple |
| Best for | OLAP | OLTP |

## Summary

- Star schema has one central fact table surrounded by dimension tables
- Fact tables contain measures and foreign keys
- Dimension tables contain descriptive attributes and primary keys
- Use surrogate keys for stability and SCD support
- Star schemas are intuitive, fast for queries, and BI-tool friendly
- Conformed dimensions enable consistency across fact tables
- Design guidelines: one fact per process, all keys in fact, descriptive dimensions

## Additional Resources

- [Star Schema Design (Kimball)](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/star-schema-olap-cube/)
- [BigQuery Best Practices for Star Schema](https://cloud.google.com/bigquery/docs/best-practices-performance-patterns)
- [Star vs Snowflake Schema](https://www.guru99.com/star-snowflake-data-warehousing.html)
