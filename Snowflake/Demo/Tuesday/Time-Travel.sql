-- Inside of Snowflake we have 3 different table types
-- Permanent (default)
-- Temporary (remains until explicitly dropped)
-- Transient (session scoped, dissapears when session ends)

-- The difference between these tables (aside from Transient not persisting
-- between sessions) is Failsafe and Time Travel

-- Fail safe is disaster recovery for when things go very very wrong
-- Time Travel is like git for your data, we can query against the table
-- as it was in the past for some period of time

-- Time Travel thresholds
-- Permanent table: 90 days of time travel
-- Temporary table: 1 day of time travel
-- Transient table: 1 day of time travel 

CREATE OR REPLACE TABLE ORDERS_PERMANENT (
    order_id STRING,
    customer_id STRING,
    amount DECIMAL(10,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Permanent table, 90 days time travel and 7 days fail safe';

INSERT INTO ORDERS_PERMANENT VALUES 
    ('O001', 'C100', 100.00, CURRENT_TIMESTAMP()),
    ('O002', 'C101', 200.00, CURRENT_TIMESTAMP()),
    ('O003', 'C102', 300.00, CURRENT_TIMESTAMP());

SELECT * FROM ORDERS_PERMANENT;

UPDATE ORDERS_PERMANENT SET amount = 999.99 WHERE order_id = 'O001';

DELETE FROM ORDERS_PERMANENT WHERE order_id = 'O003';

SELECT * FROM ORDERS_PERMANENT AT (OFFSET => -420) ORDER BY order_id;

-- Lets see if we can leverage time travel to fix a mistake
INSERT INTO ORDERS_PERMANENT
SELECT * FROM ORDERS_PERMANENT AT (OFFSET => -380) WHERE order_id = 'O003';

DROP TABLE ORDERS_PERMANENT;

UNDROP TABLE ORDERS_PERMANENT;