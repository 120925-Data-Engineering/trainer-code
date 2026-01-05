# Snowpipe Overview

## Learning Objectives

- Understand Snowpipe and its role in continuous data loading
- Recognize the difference between batch loading and Snowpipe
- Identify the components of a Snowpipe configuration
- Apply Snowpipe for event-driven data ingestion patterns

## Why This Matters

Modern data platforms require near-real-time data availability. While COPY INTO handles batch loading effectively, many use cases demand continuous ingestion as files arrive. Snowpipe automates this process, loading data within minutes of file arrival without manual intervention. This capability is essential for building responsive Bronze layers that keep pace with source system updates.

## The Concept

### What is Snowpipe?

**Snowpipe** is Snowflake's continuous data ingestion service. It automatically loads data from files as they arrive in a stage, without requiring scheduled batch jobs.

**Key Characteristics:**
- **Event-Driven**: Triggered by file arrival notifications
- **Serverless**: No warehouse required; uses Snowflake-managed compute
- **Near Real-Time**: Typically loads data within 1-2 minutes
- **Cost-Efficient**: Pay only for compute used during loading

### Batch Loading vs. Snowpipe

| Aspect | COPY INTO (Batch) | Snowpipe (Continuous) |
|--------|-------------------|----------------------|
| **Trigger** | Manual or scheduled | Automatic on file arrival |
| **Compute** | Your virtual warehouse | Snowflake-managed |
| **Latency** | Minutes to hours | 1-2 minutes |
| **Billing** | Warehouse credits | Per-file compute credits |
| **Best For** | Large batch jobs | Streaming/continuous feeds |

### Snowpipe Architecture

```
Cloud Storage (S3/Azure/GCS)
         |
         | File arrives
         v
Event Notification (SQS/Event Grid/Pub/Sub)
         |
         | Triggers Snowpipe
         v
Snowpipe (Serverless Compute)
         |
         | Loads data
         v
Target Table
```

### Components of Snowpipe

**1. Stage (External):**
Points to the cloud storage location where files land.

```sql
CREATE STAGE my_s3_stage
    URL = 's3://my-bucket/incoming/'
    STORAGE_INTEGRATION = my_s3_integration;
```

**2. File Format:**
Defines how to parse incoming files.

```sql
CREATE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;
```

**3. Target Table:**
The destination for loaded data.

```sql
CREATE TABLE raw_events (
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT
);
```

**4. Pipe:**
The Snowpipe definition that ties everything together.

```sql
CREATE PIPE my_pipe
    AUTO_INGEST = TRUE  -- Enable automatic triggering
AS
COPY INTO raw_events (source_file, raw_data)
FROM (
    SELECT 
        METADATA$FILENAME,
        $1
    FROM @my_s3_stage
)
FILE_FORMAT = (FORMAT_NAME = json_format);
```

### Auto-Ingest Setup

Auto-ingest uses cloud event notifications to trigger Snowpipe automatically.

**AWS S3 Setup:**

1. Create a storage integration:
```sql
CREATE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789:role/snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/');
```

2. Get the notification channel ARN:
```sql
DESCRIBE PIPE my_pipe;
-- Note the 'notification_channel' value (SQS ARN)
```

3. Configure S3 bucket event notifications to send to the SQS queue.

**Azure Setup:**

1. Create storage integration for Azure.
2. Configure Azure Event Grid to notify Snowpipe.

**GCP Setup:**

1. Create storage integration for GCS.
2. Configure Pub/Sub notifications.

### Manual Snowpipe Triggering

For scenarios without auto-ingest, trigger Snowpipe manually via REST API or SQL:

**SQL (for testing):**
```sql
-- Refresh pipe to load pending files
ALTER PIPE my_pipe REFRESH;
```

**REST API:**
```bash
# Insert files endpoint
POST https://<account>.snowflakecomputing.com/v1/data/pipes/<pipe_name>/insertFiles
```

### Monitoring Snowpipe

**Check Pipe Status:**
```sql
-- View pipe definition and status
SHOW PIPES;
DESCRIBE PIPE my_pipe;

-- Check recent load history
SELECT *
FROM TABLE(information_schema.COPY_HISTORY(
    TABLE_NAME => 'RAW_EVENTS',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

**Pipe Status Function:**
```sql
-- Get current pipe status
SELECT SYSTEM$PIPE_STATUS('my_pipe');
```

**Output includes:**
- `executionState`: RUNNING, PAUSED, STALLED
- `pendingFileCount`: Files waiting to be loaded
- `lastIngestedTimestamp`: Last successful load

### Error Handling

**View Load Errors:**
```sql
SELECT *
FROM TABLE(information_schema.COPY_HISTORY(...))
WHERE STATUS = 'LOAD_FAILED';
```

**Validate Files:**
```sql
SELECT *
FROM TABLE(VALIDATE_PIPE_LOAD(
    PIPE_NAME => 'my_pipe',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
));
```

### Managing Pipes

```sql
-- Pause a pipe (stop loading)
ALTER PIPE my_pipe SET PIPE_EXECUTION_PAUSED = TRUE;

-- Resume a pipe
ALTER PIPE my_pipe SET PIPE_EXECUTION_PAUSED = FALSE;

-- Drop a pipe
DROP PIPE my_pipe;

-- Recreate a pipe (for schema changes)
CREATE OR REPLACE PIPE my_pipe ...;
```

### Snowpipe Billing

Snowpipe uses a **per-file billing model** based on:
- File size and complexity
- Compute time for loading

**Typical costs:**
- First 5 seconds of compute: billed as 1 unit
- Additional compute: billed in 1-second increments

**Optimization Tips:**
- Consolidate small files (avoid many tiny files)
- Use efficient file formats (Parquet over CSV)
- Minimize transformation complexity in COPY

### Use Cases

**1. Real-Time Event Ingestion:**
```
User Events -> Kinesis/Kafka -> S3 -> Snowpipe -> Bronze Table
```

**2. CDC (Change Data Capture):**
```
Database CDC -> Files in S3 -> Snowpipe -> Staging -> Merge
```

**3. IoT Data Streams:**
```
IoT Devices -> Event Hub -> Azure Blob -> Snowpipe -> Analytics
```

**4. Log Aggregation:**
```
Application Logs -> S3 -> Snowpipe -> VARIANT Column -> Analysis
```

## Summary

- **Snowpipe** enables continuous, event-driven data loading
- Uses **cloud event notifications** (SQS, Event Grid, Pub/Sub) for auto-ingest
- **Serverless compute** means no warehouse management
- Typically loads data within **1-2 minutes** of file arrival
- **Billing** is per-file based on compute used
- Ideal for streaming feeds, CDC, IoT, and log ingestion

## Additional Resources

- [Snowflake Documentation: Snowpipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro)
- [Snowflake Documentation: Automating Snowpipe with S3](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3)
- [Snowflake Blog: Snowpipe Best Practices](https://www.snowflake.com/blog/best-practices-for-data-ingestion/)
