-- For this demo we are going to create a star schema from the TPC-H sample data set
-- One fact, and a few dimensions - and we'll focus on how this improves our querying on large datasets

-- Adding date dimension - things we would want to know about a date (so we don't have to derive/find them)
-- later or per query
CREATE OR REPLACE TABLE DIM_DATE AS
SELECT
    -- Surrogate key (YYYYMMDD format as integer)
    TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) AS date_key,
    
    -- Date attributes
    date_day AS full_date,
    DAY(date_day) AS day_of_month,
    DAYOFWEEK(date_day) AS day_of_week,
    DAYNAME(date_day) AS day_name,
    
    -- Week attributes
    WEEKOFYEAR(date_day) AS week_of_year,
    
    -- Month attributes
    MONTH(date_day) AS month_num,
    MONTHNAME(date_day) AS month_name,
    
    -- Quarter and Year
    QUARTER(date_day) AS quarter,
    YEAR(date_day) AS year,
    
    -- Fiscal dimensions (assuming calendar = fiscal for demo)
    QUARTER(date_day) AS fiscal_quarter,
    YEAR(date_day) AS fiscal_year,
    
    -- Useful flags
    CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday  -- Would be populated from a holiday calendar
FROM (
    -- Start from 1992 to cover TPC-H data range (1992-1998) plus future dates
    SELECT DATEADD('day', SEQ4(), '1992-01-01')::DATE AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 15000))  -- ~41 years (1992-2033)
);

SELECT * FROM DIM_DATE WHERE year = 1992 AND month_num = 1 LIMIT 10;
SELECT COUNT(*) AS total_dates FROM DIM_DATE;

SELECT * FROM DIM_DATE;

-- Creating a customer dimension 
-- The customer data will come from the TPC-H sample data 
-- If we had a production database, the custoemr data would come from that table
-- NOTE: The "source of truth" i.e. where our website or main app is writing customer data to
-- is still the main customer table inside TPC-H (which we are treating as our production database)
-- The dimension table pulls new info in over time - because dimensions "slowly change"
-- We aren't replacing our main 3NF db - just optimizing for fast queries of certain info 

CREATE OR REPLACE TABLE DIM_CUSTOMER AS
SELECT 
    C_CUSTKEY AS customer_key,
    C_CUSTKEY AS customer_id,  -- Natural key preserved
    C_NAME AS customer_name,
    C_MKTSEGMENT AS market_segment,
    C_NATIONKEY AS nation_key,
    'Active' AS customer_status  -- Example derived attribute
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

SELECT * FROM DIM_CUSTOMER LIMIT 10;
SELECT market_segment, COUNT(*) FROM DIM_CUSTOMER GROUP BY market_segment;



-- Lets build our final dimension table 
CREATE OR REPLACE TABLE DIM_PRODUCT AS
SELECT
    P_PARTKEY AS product_key,
    P_PARTKEY AS product_id,  -- Natural key preserved
    P_NAME AS product_name,
    P_MFGR AS manufacturer,
    P_BRAND AS brand,
    P_TYPE AS product_type,
    P_SIZE AS product_size,
    P_CONTAINER AS container_type,
    P_RETAILPRICE AS retail_price
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;

-- Verify
SELECT * FROM DIM_PRODUCT LIMIT 10;

SELECT manufacturer, COUNT(*) AS product_count 
FROM DIM_PRODUCT 
GROUP BY manufacturer 
ORDER BY product_count DESC;

/* NOW lets build that fact table

The fact table contains measures (SUM, COUNT, AVG, etc) as well foreign keys
to ALL dimensions 

Line item lets us get granular as to what products are on which order

*/

CREATE OR REPLACE TABLE FCT_ORDER_LINES AS
SELECT
    -- Composite grain: order + line number
    l.L_ORDERKEY AS order_key,
    l.L_LINENUMBER AS line_number,
    
    -- Foreign keys to ALL dimensions
    TO_NUMBER(TO_CHAR(o.O_ORDERDATE, 'YYYYMMDD')) AS date_key,
    o.O_CUSTKEY AS customer_key,
    l.L_PARTKEY AS product_key,
    
    -- Measures (things you aggregate)
    l.L_QUANTITY AS quantity,
    l.L_EXTENDEDPRICE AS extended_price,
    l.L_DISCOUNT AS discount_pct,
    l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) AS net_amount,
    l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT) * (1 + l.L_TAX) AS total_amount,
    1 AS line_count,  -- Useful for COUNT aggregations
    
    -- Semi-additive/descriptive (from order header)
    o.O_ORDERSTATUS AS order_status,
    o.O_ORDERPRIORITY AS order_priority
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM l
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o ON l.L_ORDERKEY = o.O_ORDERKEY;

SELECT * FROM FCT_ORDER_LINES LIMIT 10;

SELECT COUNT(*) AS total_line_items FROM FCT_ORDER_LINES;


-- I want Revenue by region, year, and product type
-- NOT USING OUR STAR SCHEMA

SELECT
    r.R_NAME AS region,
    YEAR(o.O_ORDERDATE) AS order_year,
    p.P_TYPE AS product_type,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM l
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n ON c.C_NATIONKEY = n.N_NATIONKEY
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r ON n.N_REGIONKEY = r.R_REGIONKEY
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART p ON l.L_PARTKEY = p.P_PARTKEY
WHERE YEAR(o.O_ORDERDATE) = 1995
GROUP BY r.R_NAME, YEAR(o.O_ORDERDATE), p.P_TYPE
ORDER BY region, product_type
LIMIT 20;


-- 
SELECT
    d.year AS order_year,
    p.product_type,
    c.market_segment,
    SUM(f.net_amount) AS revenue
FROM FCT_ORDER_LINES f
JOIN DIM_DATE d ON f.date_key = d.date_key
JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
JOIN DIM_PRODUCT p ON f.product_key = p.product_key
WHERE d.year = 1995
GROUP BY d.year, p.product_type, c.market_segment
ORDER BY c.market_segment, p.product_type
LIMIT 20;


-- Revenue by year and quarter
SELECT
    d.year,
    d.quarter,
    SUM(f.net_amount) AS total_revenue,
    SUM(f.quantity) AS total_units,
    COUNT(DISTINCT f.order_key) AS total_orders,
    ROUND(SUM(f.net_amount) / COUNT(DISTINCT f.order_key), 2) AS avg_order_value
FROM FCT_ORDER_LINES f
JOIN DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year, d.quarter;