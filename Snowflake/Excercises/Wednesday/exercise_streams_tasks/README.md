# Exercise: Streams and Tasks Automation

## Overview
**Day:** 3-Wednesday  
**Duration:** 2-3 hours  
**Mode:** Individual (Code Lab)  
**Prerequisites:** Bronze layer populated with RAW_EVENTS data

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Streams and Tasks | [streams-and-tasks.md](../../content/3-Wednesday/streams-and-tasks.md) | Change data capture, task scheduling, automation |
| Snowpipe Overview | [snowpipe-overview.md](../../content/3-Wednesday/snowpipe-overview.md) | Continuous data loading patterns |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Create Streams to track table changes (CDC)
2. Create Tasks for scheduled SQL execution
3. Build automated Bronze-to-Silver data pipelines
4. Monitor and troubleshoot task execution

---

## The Scenario
Your Bronze layer receives raw event data continuously. You need to automate the process of cleansing this data and moving it to the Silver layer. This pipeline should:
- Detect new records automatically
- Transform and load them into Silver
- Run without manual intervention

---

## Core Tasks

### Task 1: Setup Source Data (15 mins)

```sql
USE DATABASE <YOUR_NAME>_DEV_DB;
USE SCHEMA BRONZE;

-- Ensure RAW_EVENTS table exists and has data
CREATE OR REPLACE TABLE RAW_EVENTS (
    event_id STRING,
    event_type STRING,
    payload VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert initial data
INSERT INTO RAW_EVENTS (event_id, event_type, payload) VALUES
    ('E001', 'click', PARSE_JSON('{"page": "/home", "user": "U100"}')),
    ('E002', 'view', PARSE_JSON('{"page": "/products", "user": "U101"}')),
    ('E003', 'purchase', PARSE_JSON('{"product": "P001", "amount": 99.99, "user": "U100"}'));

SELECT * FROM RAW_EVENTS;
```

---

### Task 2: Create a Stream (20 mins)

```sql
-- Create a stream to track changes
CREATE OR REPLACE STREAM RAW_EVENTS_STREAM ON TABLE RAW_EVENTS
    APPEND_ONLY = FALSE
    COMMENT = 'Tracks all changes to RAW_EVENTS';

-- Stream is empty because we created it AFTER the inserts
SELECT * FROM RAW_EVENTS_STREAM;

-- Now insert new data
INSERT INTO RAW_EVENTS (event_id, event_type, payload) VALUES
    ('E004', 'click', PARSE_JSON('{"page": "/checkout", "user": "U102"}')),
    ('E005', 'purchase', PARSE_JSON('{"product": "P002", "amount": 149.99, "user": "U101"}'));

-- Stream now shows the new rows
SELECT * FROM RAW_EVENTS_STREAM;
```

**Observe:**
- METADATA$ACTION column
- METADATA$ISUPDATE column
- METADATA$ROW_ID column

**Your Task:** Insert 3 more events and verify they appear in the stream.

---

### Task 3: Create Target Table (10 mins)

```sql
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE PROCESSED_EVENTS (
    event_id STRING PRIMARY KEY,
    event_type STRING,
    user_id STRING,
    event_data VARIANT,
    source_timestamp TIMESTAMP_NTZ,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

---

### Task 4: Create an Automated Task (30 mins)

```sql
-- Create a task that processes stream data
CREATE OR REPLACE TASK PROCESS_EVENTS_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.RAW_EVENTS_STREAM')
AS
INSERT INTO SILVER.PROCESSED_EVENTS (event_id, event_type, user_id, event_data, source_timestamp)
SELECT 
    event_id,
    UPPER(event_type) AS event_type,
    payload:user::STRING AS user_id,
    payload AS event_data,
    created_at AS source_timestamp
FROM BRONZE.RAW_EVENTS_STREAM
WHERE METADATA$ACTION = 'INSERT';

-- View task definition
SHOW TASKS;
DESCRIBE TASK PROCESS_EVENTS_TASK;
```

**Important:** Tasks are created SUSPENDED by default!

---

### Task 5: Test the Task Manually (20 mins)

```sql
-- Before executing, check stream contents
SELECT 'Stream before:' AS status, COUNT(*) AS rows FROM BRONZE.RAW_EVENTS_STREAM;
SELECT 'Silver before:' AS status, COUNT(*) AS rows FROM SILVER.PROCESSED_EVENTS;

-- Execute task manually
EXECUTE TASK PROCESS_EVENTS_TASK;

-- Check results
SELECT 'Stream after:' AS status, COUNT(*) AS rows FROM BRONZE.RAW_EVENTS_STREAM;
SELECT 'Silver after:' AS status, COUNT(*) AS rows FROM SILVER.PROCESSED_EVENTS;

-- View processed data
SELECT * FROM SILVER.PROCESSED_EVENTS;
```

**Your Task:** 
1. Insert more data into RAW_EVENTS
2. Verify it appears in the stream
3. Execute the task
4. Verify data moved to Silver and stream is empty

---

### Task 6: Create a Downstream Task (30 mins)

Build a task chain (DAG):

```sql
USE SCHEMA GOLD;

-- Create aggregation table
CREATE OR REPLACE TABLE EVENT_METRICS (
    metric_date DATE,
    event_type STRING,
    event_count INTEGER,
    unique_users INTEGER,
    refreshed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (metric_date, event_type)
);

-- Create downstream task (runs AFTER the first task)
CREATE OR REPLACE TASK AGGREGATE_EVENTS_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER <YOUR_NAME>_DEV_DB.SILVER.PROCESS_EVENTS_TASK
AS
MERGE INTO GOLD.EVENT_METRICS tgt
USING (
    SELECT 
        DATE_TRUNC('day', processed_at)::DATE AS metric_date,
        event_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users
    FROM SILVER.PROCESSED_EVENTS
    GROUP BY 1, 2
) src
ON tgt.metric_date = src.metric_date AND tgt.event_type = src.event_type
WHEN MATCHED THEN UPDATE SET 
    event_count = src.event_count,
    unique_users = src.unique_users,
    refreshed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT 
    (metric_date, event_type, event_count, unique_users)
    VALUES (src.metric_date, src.event_type, src.event_count, src.unique_users);
```

---

### Task 7: Monitor Task Execution (20 mins)

```sql
-- View task history
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'PROCESS_EVENTS_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));

-- Check task dependencies
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'PROCESS_EVENTS_TASK',
    RECURSIVE => TRUE
));
```

---

## Deliverables

1. **SQL Script:** `streams_tasks.sql` with all DDL
2. **Test Log:** Document of manual execution results
3. **Architecture Diagram:** Draw the data flow (Bronze -> Stream -> Task -> Silver -> Task -> Gold)

---

## Definition of Done

- [ ] Stream created on RAW_EVENTS
- [ ] PROCESS_EVENTS_TASK created and tested
- [ ] AGGREGATE_EVENTS_TASK created (downstream)
- [ ] Manual execution successful
- [ ] Task history reviewed
- [ ] Architecture diagram completed

---

## Key Commands Reference

| Command | Purpose |
|---------|---------|
| `CREATE STREAM ... ON TABLE` | Track changes to a table |
| `SYSTEM$STREAM_HAS_DATA()` | Check if stream has data |
| `CREATE TASK ... SCHEDULE` | Create scheduled task |
| `CREATE TASK ... AFTER` | Create dependent task |
| `ALTER TASK ... RESUME` | Enable task scheduling |
| `ALTER TASK ... SUSPEND` | Disable task scheduling |
| `EXECUTE TASK` | Run task manually |

---

## Warning

Do NOT resume tasks to run on schedule during this exercise. Manual execution is sufficient for learning. Running tasks consume warehouse credits continuously.
