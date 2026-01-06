
-- Basic Queries
-- This should be really familiar from SQL, SparkSQL... its almost 1:1 though SnowflakeSQL adds
-- alot of quality of life functionality

-- Losing my sanity for a second...
SELECT 
    CURRENT_ROLE() as current_role;
    
-- Basic select with a limit, same as regular SQL     
SELECT * FROM CUSTOMER LIMIT 10;

-- Aggregation Query
-- Same pattern as SparkSQL or SQL

-- "For each Market Segment, I want the total customers, average account balance
-- of those customers, total account balance segment wide, ordered from largest
-- market segment to smallest (by customer count)"

SELECT
    C_MKTSEGMENT AS market_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(C_ACCTBAL), 2) AS avg_balance,
    ROUND(SUM(C_ACCTBAL), 2) AS total_balance
FROM CUSTOMER
GROUP BY C_MKTSEGMENT
ORDER BY customer_count DESC;


-- Lets do a join - again identical to SQL 

-- "I want to get the name, full address (including the name of the nation), phone number, and Market Segment 
-- for each customer in my DB, for customers in the United States only."
-- Hint, nation name is all caps 
-- 'UNITED STATES'

SELECT 
    c.C_NAME AS customer_name,
    c.C_ADDRESS AS address,
    n.N_NAME AS nation,
    c.C_PHONE as phone_number,
    c.C_MKTSEGMENT AS market_segment
FROM CUSTOMER c
JOIN NATION n ON c.c_nationkey = n.n_nationkey
WHERE n.n_name = 'UNITED STATES'
ORDER BY customer_name DESC;

-- In addition to finding query history in the UI, we can access it via SQL 
-- Its technically stored inside INFORMATION_SCEHMA, though we access it via calling
-- a QUERY_HISTORY() function

SELECT 
    QUERY_TEXT,
    EXECUTION_STATUS
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY());