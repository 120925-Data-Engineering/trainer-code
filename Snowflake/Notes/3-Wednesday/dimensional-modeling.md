# Dimensional Modeling

## Learning Objectives

- Understand dimensional modeling concepts and terminology
- Differentiate between fact tables and dimension tables
- Design star and snowflake schemas
- Apply dimensional modeling to build effective Gold layer analytics

## Why This Matters

Dimensional modeling is the foundation of analytical data warehouses. While your Bronze and Silver layers focus on data quality and standardization, the Gold layer is where dimensional modeling shines. Well-designed dimensional models enable fast, intuitive queries for business users and power the dashboards and reports that drive organizational decisions.

## The Concept

### What is Dimensional Modeling?

**Dimensional modeling** is a data warehouse design technique that organizes data for analytical queries. It prioritizes:
- **Query performance**: Optimized for aggregation and filtering
- **User understanding**: Intuitive structure for business users
- **BI tool compatibility**: Works well with visualization tools

The two primary components are **fact tables** and **dimension tables**.

### Fact Tables

**Fact tables** store measurable, quantitative data about business events. Each row represents a single event or transaction.

**Characteristics:**
- Contains **measures** (numeric values to aggregate)
- Contains **foreign keys** to dimension tables
- Typically the largest tables in the warehouse
- Grain defined by the level of detail (e.g., one row per order line)

**Types of Facts:**

| Type | Description | Example |
|------|-------------|---------|
| **Additive** | Can be summed across all dimensions | Revenue, Quantity |
| **Semi-Additive** | Can be summed across some dimensions | Account Balance |
| **Non-Additive** | Cannot be summed; must be averaged/counted | Unit Price, Ratio |

**Example Fact Table:**
```sql
CREATE TABLE gold.fact_sales (
    -- Foreign keys
    date_key INTEGER,
    product_key INTEGER,
    customer_key INTEGER,
    store_key INTEGER,
    
    -- Degenerate dimension (no separate table)
    order_number STRING,
    
    -- Measures
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    sales_amount DECIMAL(12,2),
    cost_amount DECIMAL(12,2),
    profit_amount DECIMAL(12,2)
);
```

### Dimension Tables

**Dimension tables** store descriptive attributes that provide context for facts. They answer the "who, what, where, when, why" of business events.

**Characteristics:**
- Contains **attributes** (descriptive text and categories)
- Contains a **surrogate key** (synthetic primary key)
- May contain a **natural key** (business identifier)
- Relatively smaller than fact tables
- Often denormalized for query simplicity

**Example Dimension Tables:**

```sql
-- Date dimension
CREATE TABLE gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    day_of_week STRING,
    day_name STRING,
    month_number INTEGER,
    month_name STRING,
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    fiscal_quarter INTEGER,
    fiscal_year INTEGER
);

-- Product dimension
CREATE TABLE gold.dim_product (
    product_key INTEGER PRIMARY KEY,
    product_id STRING,           -- Natural key
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2),
    is_active BOOLEAN
);

-- Customer dimension
CREATE TABLE gold.dim_customer (
    customer_key INTEGER PRIMARY KEY,
    customer_id STRING,          -- Natural key
    customer_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    segment STRING,
    registration_date DATE
);
```

### Star Schema

A **star schema** arranges a central fact table surrounded by dimension tables, forming a star shape.

```
                dim_date
                   |
                   |
dim_product ---[fact_sales]--- dim_customer
                   |
                   |
               dim_store
```

**Characteristics:**
- Dimensions directly connected to fact table
- Dimensions are denormalized (all attributes in one table)
- Simple, fast queries with minimal joins
- Preferred for most analytical workloads

**Query Example:**
```sql
SELECT 
    d.year,
    d.quarter,
    p.category,
    c.segment,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.profit_amount) AS total_profit
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.date_key = d.date_key
JOIN gold.dim_product p ON f.product_key = p.product_key
JOIN gold.dim_customer c ON f.customer_key = c.customer_key
WHERE d.year = 2024
GROUP BY d.year, d.quarter, p.category, c.segment
ORDER BY total_sales DESC;
```

### Snowflake Schema

A **snowflake schema** normalizes dimension tables into sub-dimensions, reducing redundancy at the cost of query complexity.

```
                   dim_date
                      |
                      |
dim_category -- dim_product ---[fact_sales]--- dim_customer -- dim_geography
                      |                              |
                  dim_brand                      dim_segment
```

**Characteristics:**
- Dimensions normalized into multiple tables
- Reduces storage (less redundancy)
- More complex queries (additional joins)
- Useful when dimension hierarchies change frequently

**Trade-offs:**

| Aspect | Star Schema | Snowflake Schema |
|--------|-------------|------------------|
| **Query Simplicity** | Simple | Complex |
| **Query Performance** | Faster | Slower (more joins) |
| **Storage** | Higher | Lower |
| **Maintenance** | Easier | Harder |
| **Best For** | Analytics | Storage optimization |

**Recommendation:** Start with star schema unless you have specific reasons for snowflake.

### Surrogate Keys vs. Natural Keys

**Natural Key:** The business identifier from the source system (e.g., `customer_id = 'CUST001'`)

**Surrogate Key:** A synthetic, system-generated identifier (e.g., `customer_key = 12345`)

**Why Use Surrogate Keys?**
- Handle source key changes
- Support slowly changing dimensions
- Improve join performance (integers vs. strings)
- Protect against source system changes

```sql
-- Generate surrogate keys
CREATE SEQUENCE customer_key_seq;

INSERT INTO dim_customer (customer_key, customer_id, customer_name)
SELECT 
    customer_key_seq.NEXTVAL,
    customer_id,
    customer_name
FROM staging_customers;
```

### Slowly Changing Dimensions (SCD)

Dimensions change over time. How you handle changes affects historical reporting.

**SCD Type 1: Overwrite**
- Replace old value with new value
- No history preserved
- Simple but loses historical context

```sql
UPDATE dim_customer
SET address = '456 New Street'
WHERE customer_id = 'CUST001';
```

**SCD Type 2: Add New Row**
- Create new row for each change
- Preserve full history
- Requires effective dates and current flag

```sql
-- Expire current row
UPDATE dim_customer
SET current_flag = FALSE, end_date = CURRENT_DATE()
WHERE customer_id = 'CUST001' AND current_flag = TRUE;

-- Insert new row
INSERT INTO dim_customer (customer_key, customer_id, address, start_date, end_date, current_flag)
VALUES (NEXTVAL('seq'), 'CUST001', '456 New Street', CURRENT_DATE(), '9999-12-31', TRUE);
```

**SCD Type 3: Add New Column**
- Add column for previous value
- Limited history (typically one previous value)

```sql
ALTER TABLE dim_customer ADD COLUMN previous_address STRING;

UPDATE dim_customer
SET previous_address = address, address = '456 New Street'
WHERE customer_id = 'CUST001';
```

### Date Dimension Best Practices

Every dimensional model needs a robust date dimension:

```sql
-- Populate date dimension
INSERT INTO dim_date
SELECT 
    TO_NUMBER(TO_CHAR(d.date_value, 'YYYYMMDD')) AS date_key,
    d.date_value AS full_date,
    DAYOFWEEK(d.date_value) AS day_of_week,
    DAYNAME(d.date_value) AS day_name,
    MONTH(d.date_value) AS month_number,
    MONTHNAME(d.date_value) AS month_name,
    QUARTER(d.date_value) AS quarter,
    YEAR(d.date_value) AS year,
    DAYOFWEEK(d.date_value) IN (0, 6) AS is_weekend,
    FALSE AS is_holiday  -- Populate separately
FROM (
    SELECT DATEADD('day', SEQ4(), '2020-01-01')::DATE AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))  -- 10 years
) d;
```

## Summary

- **Dimensional modeling** organizes data for analytical queries using fact and dimension tables
- **Fact tables** contain measures and foreign keys; **dimension tables** contain descriptive attributes
- **Star schema** is simple and performant; **snowflake schema** reduces storage but adds complexity
- Use **surrogate keys** for flexibility and performance
- Handle dimension changes with **SCD patterns** (Type 1, 2, or 3)
- A robust **date dimension** is essential for time-based analysis

## Additional Resources

- [Kimball Group: Dimensional Modeling Techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Snowflake Documentation: Data Modeling](https://docs.snowflake.com/en/user-guide/data-modeling)
- [The Data Warehouse Toolkit by Ralph Kimball](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/)
