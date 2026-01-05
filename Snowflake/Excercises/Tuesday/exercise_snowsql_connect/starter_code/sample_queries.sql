-- Sample queries to practice with SnowSQL
-- Source this file with: !source sample_queries.sql

-- Set context
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;

-- Query 1: Simple count
SELECT COUNT(*) AS total_customers FROM CUSTOMER;

-- Query 2: Aggregation with grouping
SELECT 
    C_MKTSEGMENT AS segment,
    COUNT(*) AS count,
    ROUND(AVG(C_ACCTBAL), 2) AS avg_bal
FROM CUSTOMER
GROUP BY C_MKTSEGMENT;

-- Query 3: Date-based analysis
SELECT 
    YEAR(O_ORDERDATE) AS order_year,
    COUNT(*) AS orders,
    ROUND(SUM(O_TOTALPRICE), 2) AS revenue
FROM ORDERS
GROUP BY order_year
ORDER BY order_year;
