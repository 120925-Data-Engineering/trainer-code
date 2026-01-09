# Exercise: Dimensional Modeling

## Overview
**Day:** 3-Wednesday  
**Duration:** 3-4 hours  
**Mode:** Individual (Hybrid - Design + Implementation)  
**Prerequisites:** Gold schema exists

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Dimensional Modeling | [dimensional-modeling.md](../../content/3-Wednesday/dimensional-modeling.md) | Star schema, dimensions, facts, normalization |
| Data Loading Best Practices | [data-loading-best-practices.md](../../content/3-Wednesday/data-loading-best-practices.md) | Performance optimization, file sizing |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Design a star schema for a business scenario
2. Create dimension tables with appropriate attributes
3. Create fact tables with measures and foreign keys
4. Write analytical queries that join facts and dimensions

---

## The Scenario

**GameZone** is an online video game retailer. They need a data warehouse to analyze:
- Sales performance by region and time
- Product popularity across categories
- Customer purchasing patterns

You have been given sample data representing their operational systems.

---

## Core Tasks

### Task 1: Understand the Source Data (30 mins)

Review the source data structure:

**Transactions (Operational):**
```
transaction_id, customer_id, product_id, store_id, 
transaction_date, quantity, unit_price, discount_percent
```

**Customers (Operational):**
```
customer_id, name, email, registration_date, 
loyalty_tier, city, state, country
```

**Products (Operational):**
```
product_id, name, category, subcategory, 
publisher, release_date, list_price
```

**Stores (Operational):**
```
store_id, store_name, region, country, 
store_type (online/retail)
```

Document:
1. What business questions should the warehouse answer?
2. What is the grain (level of detail) of the fact table?
3. Which attributes belong in dimensions vs facts?

---

### Task 2: Design the Star Schema (45 mins)

Create a design document with:

1. **Fact Table Definition:**
   - Name: `FCT_SALES`
   - Grain: One row per transaction line
   - Measures: quantity, revenue, discount_amount
   - Foreign keys: to all dimensions

2. **Dimension Tables:**
   - `DIM_DATE` - Time dimension
   - `DIM_CUSTOMER` - Customer dimension
   - `DIM_PRODUCT` - Product dimension
   - `DIM_STORE` - Store/location dimension

3. **Schema Diagram:** Draw the star schema (use template in `templates/star_schema.mermaid`)

---

### Task 3: Implement the Date Dimension (30 mins)

```sql
USE DATABASE <YOUR_NAME>_DEV_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE DIM_DATE AS
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS date_key,
    d AS full_date,
    DAY(d) AS day_of_month,
    DAYOFWEEK(d) AS day_of_week,
    DAYNAME(d) AS day_name,
    MONTH(d) AS month_num,
    MONTHNAME(d) AS month_name,
    QUARTER(d) AS quarter,
    YEAR(d) AS year,
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT DATEADD('day', SEQ4(), '2020-01-01')::DATE AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 2191))
);

SELECT * FROM DIM_DATE WHERE year = 2024 LIMIT 10;
```

---

### Task 4: Implement Remaining Dimensions (45 mins)

Create the other dimension tables:

```sql
-- DIM_CUSTOMER
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_key INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id STRING,  -- Natural key
    customer_name STRING,
    loyalty_tier STRING,
    city STRING,
    state STRING,
    country STRING,
    registration_date DATE,
    effective_date DATE DEFAULT CURRENT_DATE(),
    is_current BOOLEAN DEFAULT TRUE
);

-- Insert sample data
INSERT INTO DIM_CUSTOMER (customer_id, customer_name, loyalty_tier, city, state, country, registration_date)
VALUES 
    ('C001', 'John Smith', 'Gold', 'New York', 'NY', 'USA', '2023-01-15'),
    ('C002', 'Jane Doe', 'Silver', 'Los Angeles', 'CA', 'USA', '2023-03-20'),
    ('C003', 'Bob Wilson', 'Bronze', 'Chicago', 'IL', 'USA', '2023-06-01');
```

**Your Task:** Create and populate:
- `DIM_PRODUCT` with at least 5 products
- `DIM_STORE` with at least 3 stores

---

### Task 5: Create the Fact Table (30 mins)

```sql
CREATE OR REPLACE TABLE FCT_SALES (
    -- Degenerate dimension (transaction ID)
    transaction_id STRING,
    
    -- Foreign keys to dimensions
    date_key INTEGER,
    customer_key INTEGER,
    product_key INTEGER,
    store_key INTEGER,
    
    -- Measures (additive facts)
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2),
    revenue DECIMAL(12,2),
    discount_amount DECIMAL(10,2),
    
    -- Timestamps
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample transactions
INSERT INTO FCT_SALES (transaction_id, date_key, customer_key, product_key, store_key, 
                       quantity, unit_price, discount_percent, revenue, discount_amount)
VALUES
    ('T001', 20240115, 1, 1, 1, 2, 59.99, 10, 107.98, 12.00),
    ('T002', 20240115, 2, 2, 2, 1, 29.99, 0, 29.99, 0),
    ('T003', 20240116, 1, 3, 1, 3, 49.99, 15, 127.47, 22.50);
```

**Your Task:** Insert at least 10 more transactions with varied dates, customers, and products.

---

### Task 6: Write Analytical Queries (45 mins)

Use your star schema to answer business questions:

**Query 1: Revenue by Month**
```sql
SELECT 
    d.year,
    d.month_name,
    SUM(f.revenue) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS transaction_count
FROM FCT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.year, d.month_name, d.month_num
ORDER BY d.year, d.month_num;
```

**Your Tasks - Write queries for:**

2. **Revenue by Customer Tier:** Show total revenue and average order value by loyalty tier.

3. **Top Products:** List top 5 products by revenue.

4. **Store Performance:** Compare online vs retail store revenue.

5. **Weekend vs Weekday:** Is there a difference in sales patterns?

---

### Task 7: Create Summary Views (20 mins)

Create reusable views for common analyses:

```sql
CREATE OR REPLACE VIEW V_DAILY_SALES_SUMMARY AS
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    SUM(f.revenue) AS daily_revenue,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM FCT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.full_date, d.day_name, d.is_weekend;
```

**Your Task:** Create at least one more summary view of your choice.

---

## Deliverables

1. **Design Document:** Star schema diagram and dimension descriptions
2. **SQL Script:** `dimensional_model.sql` with all DDL and DML
3. **Query File:** `analytics_queries.sql` with all analytical queries
4. **Views:** At least 2 summary views created

---

## Definition of Done

- [ ] Star schema designed with diagram
- [ ] DIM_DATE created and populated
- [ ] DIM_CUSTOMER created with 3+ customers
- [ ] DIM_PRODUCT created with 5+ products
- [ ] DIM_STORE created with 3+ stores
- [ ] FCT_SALES created with 10+ transactions
- [ ] All 5 analytical queries completed
- [ ] At least 2 summary views created

---

## Evaluation Criteria

| Criterion | Points |
|-----------|--------|
| Schema design is valid star schema | 20 |
| Dimensions have appropriate attributes | 20 |
| Fact table has correct grain and measures | 20 |
| Analytical queries are correct | 25 |
| Views are reusable and well-designed | 15 |
