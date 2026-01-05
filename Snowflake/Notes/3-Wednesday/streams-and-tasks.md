# Streams and Tasks

## Learning Objectives

- Understand Snowflake Streams for change data capture (CDC)
- Create and monitor Streams on tables
- Automate data processing with Tasks
- Build end-to-end automated pipelines using Streams and Tasks together

## Why This Matters

Efficient data pipelines process only what has changed, not entire datasets. Streams and Tasks enable incremental processing patterns that are essential for maintaining your Silver and Gold layers in the Medallion architecture. Instead of reprocessing millions of rows, you process only the delta, reducing compute costs and latency.

## The Concept

### What is a Stream?

A **Stream** is a Snowflake object that tracks changes (inserts, updates, deletes) to a table. Streams provide a change data capture (CDC) mechanism without requiring external tools.

**Key Characteristics:**
- Records changes since the last consumption
- Lightweight; no data duplication
- Supports Standard (default) and Append-Only modes
- Consumable in DML operations (INSERT, MERGE)

### Creating a Stream

**Basic Stream:**
```sql
-- Create a stream on a table
CREATE STREAM orders_stream ON TABLE orders;
```

**Stream Types:**

| Type | Tracks | Use Case |
|------|--------|----------|
| **Standard (Delta)** | INSERT, UPDATE, DELETE | Full CDC |
| **Append-Only** | INSERT only | Event logs, immutable data |

```sql
-- Append-only stream (only inserts)
CREATE STREAM events_stream 
ON TABLE events
APPEND_ONLY = TRUE;
```

### Stream Columns

When you query a stream, it includes metadata columns:

| Column | Description |
|--------|-------------|
| `METADATA$ACTION` | INSERT, DELETE |
| `METADATA$ISUPDATE` | TRUE if part of an UPDATE |
| `METADATA$ROW_ID` | Unique row identifier |

**Understanding Updates:**
Updates appear as a DELETE followed by an INSERT (both with `METADATA$ISUPDATE = TRUE`).

```sql
-- Query the stream
SELECT 
    order_id,
    customer_id,
    METADATA$ACTION AS action,
    METADATA$ISUPDATE AS is_update
FROM orders_stream;
```

**Example Output:**
```
order_id | customer_id | action | is_update
---------|-------------|--------|----------
O001     | C100        | INSERT | FALSE     -- New row
O002     | C101        | DELETE | TRUE      -- Old value (update)
O002     | C101        | INSERT | TRUE      -- New value (update)
O003     | C102        | DELETE | FALSE     -- Deleted row
```

### Consuming a Stream

Streams are consumed when you use them in a DML transaction (INSERT, MERGE, etc.). After commit, the stream resets.

**Insert from Stream:**
```sql
-- Consume stream: insert new/changed rows into target
INSERT INTO orders_archive
SELECT order_id, customer_id, order_date, amount
FROM orders_stream
WHERE METADATA$ACTION = 'INSERT';
```

**Merge Pattern (Most Common):**
```sql
MERGE INTO silver.orders t
USING (
    SELECT 
        order_id,
        customer_id,
        order_date,
        amount
    FROM bronze.orders_stream
    WHERE METADATA$ACTION = 'INSERT'
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN 
    UPDATE SET 
        customer_id = s.customer_id,
        order_date = s.order_date,
        amount = s.amount
WHEN NOT MATCHED THEN 
    INSERT (order_id, customer_id, order_date, amount)
    VALUES (s.order_id, s.customer_id, s.order_date, s.amount);
```

### Stream Staleness

Streams have a staleness period tied to Time Travel retention. If not consumed within the retention period, the stream becomes stale and unusable.

```sql
-- Check stream status
SHOW STREAMS LIKE 'orders_stream';

-- View staleness
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('orders_stream');
```

**Prevention:**
- Consume streams regularly
- Set appropriate data retention on source tables

### What is a Task?

A **Task** is a scheduled object that executes SQL statements on a defined schedule or when triggered.

**Key Characteristics:**
- Cron-based or interval scheduling
- Can depend on other tasks (DAGs)
- Serverless (Snowflake-managed) or warehouse-based compute
- Integrates with Streams for event-driven processing

### Creating a Task

**Basic Scheduled Task:**
```sql
-- Run every hour
CREATE TASK hourly_summary
    WAREHOUSE = compute_wh
    SCHEDULE = '60 MINUTE'  -- or USING CRON '0 * * * *'
AS
    INSERT INTO hourly_metrics
    SELECT DATE_TRUNC('hour', CURRENT_TIMESTAMP()), COUNT(*)
    FROM orders
    WHERE order_date >= DATEADD('hour', -1, CURRENT_TIMESTAMP());
```

**Cron Schedule:**
```sql
CREATE TASK daily_report
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 6 * * * America/New_York'  -- 6 AM ET daily
AS
    CALL generate_daily_report();
```

**Cron Syntax:** `minute hour day month dayOfWeek timezone`

### Serverless Tasks

Tasks can use Snowflake-managed compute instead of a warehouse:

```sql
CREATE TASK lightweight_task
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '5 MINUTE'
AS
    INSERT INTO audit_log VALUES (CURRENT_TIMESTAMP(), 'heartbeat');
```

### Stream-Triggered Tasks

Combine Streams and Tasks for incremental processing:

```sql
-- Task runs only when stream has data
CREATE TASK process_order_changes
    WAREHOUSE = compute_wh
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
    MERGE INTO silver.orders t
    USING orders_stream s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' 
                 AND s.METADATA$ISUPDATE = TRUE
        THEN UPDATE SET ...
    WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT'
        THEN INSERT ...;
```

**The WHEN Clause:**
- `SYSTEM$STREAM_HAS_DATA('stream_name')` returns TRUE if stream has unconsumed changes
- Task only runs when condition is TRUE
- Saves compute costs by skipping empty runs

### Task Dependencies (DAGs)

Create task chains where child tasks run after parent completes:

```sql
-- Parent task
CREATE TASK bronze_to_silver
    WAREHOUSE = compute_wh
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('raw_stream')
AS
    -- Transform bronze to silver
    INSERT INTO silver.data SELECT ... FROM bronze_stream;

-- Child task (runs after parent)
CREATE TASK silver_to_gold
    WAREHOUSE = compute_wh
    AFTER bronze_to_silver
AS
    -- Aggregate silver to gold
    INSERT INTO gold.metrics SELECT ... FROM silver.data;
```

```
bronze_to_silver (scheduled)
          |
          v
    silver_to_gold (triggered after parent)
```

### Managing Tasks

**Tasks are created in suspended state. You must resume them:**

```sql
-- Resume a task (start scheduling)
ALTER TASK process_order_changes RESUME;

-- Suspend a task (stop scheduling)
ALTER TASK process_order_changes SUSPEND;

-- Resume child tasks first, then parent
ALTER TASK silver_to_gold RESUME;
ALTER TASK bronze_to_silver RESUME;
```

**Monitoring Tasks:**
```sql
-- View task history
SELECT *
FROM TABLE(information_schema.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    TASK_NAME => 'process_order_changes'
))
ORDER BY SCHEDULED_TIME DESC;

-- View current task runs
SHOW TASKS;
```

### Complete Pipeline Example

```sql
-- 1. Bronze table (raw data landing)
CREATE TABLE bronze.raw_orders (raw_data VARIANT);

-- 2. Stream on bronze
CREATE STREAM bronze.orders_stream ON TABLE bronze.raw_orders;

-- 3. Silver table (cleansed)
CREATE TABLE silver.orders (
    order_id STRING,
    customer_id STRING,
    amount DECIMAL(12,2),
    processed_at TIMESTAMP_NTZ
);

-- 4. Task to process stream
CREATE TASK bronze_to_silver_task
    WAREHOUSE = etl_wh
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('bronze.orders_stream')
AS
    INSERT INTO silver.orders (order_id, customer_id, amount, processed_at)
    SELECT 
        raw_data:order_id::STRING,
        raw_data:customer_id::STRING,
        raw_data:amount::DECIMAL(12,2),
        CURRENT_TIMESTAMP()
    FROM bronze.orders_stream
    WHERE METADATA$ACTION = 'INSERT';

-- 5. Resume the task
ALTER TASK bronze_to_silver_task RESUME;
```

## Summary

- **Streams** capture changes (INSERT, UPDATE, DELETE) to tables
- Streams enable **incremental processing** without full table scans
- **Tasks** schedule and automate SQL execution
- Use `WHEN SYSTEM$STREAM_HAS_DATA()` for event-driven task execution
- **Task DAGs** create multi-step processing pipelines
- Streams and Tasks together power the Bronze-to-Silver-to-Gold flow

## Additional Resources

- [Snowflake Documentation: Streams](https://docs.snowflake.com/en/user-guide/streams-intro)
- [Snowflake Documentation: Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
- [Snowflake Blog: Building Data Pipelines with Streams and Tasks](https://www.snowflake.com/blog/building-data-pipelines-with-streams-and-tasks/)
