-- Streams and Tasks 
-- Stream: Is a snowflake object that tracks changes to a table. Do not get it confused
-- with a Kafka stream. 
-- Why would we use one?
-- Data pipeliesn typically process data as it comes in - whether it's a continouous kafka stream
-- OR a series of batches

-- 
SELECT * FROM RAW_EVENTS;

TRUNCATE TABLE RAW_EVENTS;

-- Fun fact, cant use function calls inside VALUES() area
INSERT INTO RAW_EVENTS (event_id, event_type, payload)
SELECT 'E001', 'click', PARSE_JSON('{"page": "/home", "user": "U100"}')
UNION ALL SELECT 'E002', 'view', PARSE_JSON('{"page": "/products", "user": "U101"}')
UNION ALL SELECT 'E003', 'purchase', PARSE_JSON('{"product": "P001", "amount": 99.99, "user": "U100"}');

-- Lets create a Stream
-- Again, works like a Kafka offset - tracks what rows you've processed

CREATE OR REPLACE STREAM RAW_EVENTS_STREAM ON TABLE RAW_EVENTS
    APPEND_ONLY = FALSE -- Now we track inserts, updates, and deletes
    COMMENT = 'Tracks changes to RAW_EVENTS for incremental processing.';

SELECT * FROM RAW_EVENTS_STREAM;

INSERT INTO RAW_EVENTS (event_id, event_type, payload)
SELECT 'E004', 'click', PARSE_JSON('{"page": "/checkout", "user": "U102"}')
UNION ALL SELECT 'E005', 'purchase', PARSE_JSON('{"product": "P002", "amount": 149.99, "user": "U101"}');

USE SCHEMA SILVER;

SELECT * FROM PROCESSED_EVENTS;

-- Part 2: Tasks 
-- A task is like an Airflow task, but defined in SQL
-- It can run on a schedule OR when it detects new data in a stream

CREATE OR REPLACE TASK PROCESS_EVENTS_TASK 
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE' -- check our stream every minute, if there are things in the stream then run... otherwise just check again in a minute. 
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.RAW_EVENTS_STREAM') -- the stream to check
AS 
INSERT INTO SILVER.PROCESSED_EVENTS (event_id, event_type, user_id, processed_at)
SELECT
    event_id,
    UPPER(event_type) AS event_type, -- make this uppercase
    payload:user::STRING as user_id, --extract from JSON
    CURRENT_TIMESTAMP()
FROM BRONZE.RAW_EVENTS_STREAM
WHERE METADATA$ACTION = 'INSERT'; -- only process insertions

SHOW TASKS;

DESCRIBE TASK PROCESS_EVENTS_TASK;

SELECT * FROM BRONZE.RAW_EVENTS_STREAM;

-- Lets execute our task manually
EXECUTE TASK PROCESS_EVENTS_TASK;

SELECT * FROM SILVER.PROCESSED_EVENTS;

-- We CAN chain tasks together like an Airflow DAG

CREATE OR REPLACE TASK AGGREGATE_EVENTS_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER PROCESS_EVENTS_TASK -- Run directly after this preceding task 
AS
MERGE INTO GOLD.EVENT_SUMMARY tgt
USING (
    SELECT event_type, COUNT(*) AS event_count
    FROM SILVER.PROCESSED_EVENTS
    GROUP BY event_type
) src
ON tgt.event_type = src.event_type
WHEN MATCHED THEN UPDATE SET event_count = src.event_count
WHEN NOT MATCHED THEN INSERT (event_type, event_count) VALUES (src.event_type, src.event_count);

INSERT INTO BRONZE.RAW_EVENTS (event_id, event_type, payload)
SELECT 'E008', 'click', PARSE_JSON('{"page": "/about", "user": "U103"}')
UNION ALL SELECT 'E009', 'logout', PARSE_JSON('{"user": "U100"}');


EXECUTE TASK PROCESS_EVENTS_TASK;

SELECT * FROM SILVER.PROCESSED_EVENTS;
SELECT * FROM GOLD.EVENT_SUMMARY;

-- In order to see our tasks properly chain we need to toggle them to RESUME from SUSPENDED
ALTER TASK AGGREGATE_EVENTS_TASK RESUME; --runs after PROCESS_EVENTS_TASK

ALTER TASK PROCESS_EVENTS_TASK RESUME; -- runs every minute 

SHOW TASKS;

