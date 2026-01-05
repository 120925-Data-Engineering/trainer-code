# Data Loading Fundamentals

## Learning Objectives

- Understand Snowflake's data loading concepts and staging
- Apply the COPY INTO command for bulk data loading
- Work with different file formats (CSV, JSON, Parquet)
- Recognize best practices for efficient data ingestion

## Why This Matters

Data loading is a fundamental operation in any data pipeline. Whether populating your Bronze layer with raw data or performing batch ETL, understanding Snowflake's loading mechanisms ensures efficient, reliable data ingestion. The techniques you learn here form the foundation for more advanced patterns like Snowpipe (continuous loading), which you will explore on Wednesday.

## The Concept

### Data Loading Overview

Snowflake supports multiple data loading methods:

| Method | Use Case | Latency |
|--------|----------|---------|
| **COPY INTO** | Bulk batch loading | Minutes |
| **Snowpipe** | Continuous streaming | Seconds to minutes |
| **INSERT** | Small row-level inserts | Immediate |
| **Snowpark** | Programmatic loading | Varies |

This reading focuses on **COPY INTO** for bulk loading, the most common method for batch pipelines.

### The Loading Workflow

```
Source Files (S3, Azure, GCS, Local)
          |
          v
       Stage (Internal or External)
          |
          v
     File Format (CSV, JSON, Parquet)
          |
          v
      COPY INTO command
          |
          v
     Target Table
```

### Stages

Stages are storage locations where data files reside before loading.

**Internal Stages (Managed by Snowflake):**

```sql
-- User stage (auto-created for each user)
PUT file://C:/data/orders.csv @~;  -- Windows
PUT file:///home/user/orders.csv @~;  -- Linux/Mac

-- Table stage (auto-created for each table)
PUT file://C:/data/orders.csv @%orders;

-- Named internal stage
CREATE STAGE my_internal_stage;
PUT file://C:/data/orders.csv @my_internal_stage;
```

**External Stages (Cloud Storage):**

```sql
-- Amazon S3
CREATE STAGE s3_stage
    URL = 's3://my-bucket/data/'
    STORAGE_INTEGRATION = my_s3_integration;

-- Azure Blob Storage
CREATE STAGE azure_stage
    URL = 'azure://myaccount.blob.core.windows.net/container/data/'
    STORAGE_INTEGRATION = my_azure_integration;

-- Google Cloud Storage
CREATE STAGE gcs_stage
    URL = 'gcs://my-bucket/data/'
    STORAGE_INTEGRATION = my_gcs_integration;
```

**Listing Stage Contents:**
```sql
LIST @my_stage;
LIST @my_stage/subfolder/;
LIST @s3_stage PATTERN = '.*orders.*[.]csv';
```

### File Formats

File formats define how Snowflake parses data files.

**CSV File Format:**
```sql
CREATE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = 'AUTO';
```

**JSON File Format:**
```sql
CREATE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE  -- If file contains a JSON array
    COMPRESSION = 'AUTO';
```

**Parquet File Format:**
```sql
CREATE FILE FORMAT parquet_format
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO';
```

**Using File Formats:**
```sql
-- Reference by name
COPY INTO orders FROM @my_stage
    FILE_FORMAT = (FORMAT_NAME = csv_format);

-- Inline specification
COPY INTO orders FROM @my_stage
    FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
```

### COPY INTO Command

The primary command for bulk loading data into tables.

**Basic Syntax:**
```sql
COPY INTO <table_name>
FROM @<stage_name>
FILE_FORMAT = (FORMAT_NAME = <format_name>)
[OPTIONS];
```

**Load from Internal Stage:**
```sql
-- Create target table
CREATE TABLE orders (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    order_total DECIMAL(12,2)
);

-- Load CSV data
COPY INTO orders
FROM @my_stage/orders.csv
FILE_FORMAT = (FORMAT_NAME = csv_format);
```

**Load from External Stage (S3):**
```sql
COPY INTO orders
FROM @s3_stage/orders/
FILE_FORMAT = (FORMAT_NAME = csv_format)
PATTERN = '.*orders_2024.*[.]csv';  -- Load files matching pattern
```

**Load with Transformations:**
```sql
-- Select and transform columns during load
COPY INTO orders (order_id, customer_id, order_date, order_total)
FROM (
    SELECT 
        $1,                          -- First column
        $2,                          -- Second column
        TO_DATE($3, 'YYYY-MM-DD'),  -- Parse date
        $4::DECIMAL(12,2)           -- Cast to decimal
    FROM @my_stage/orders.csv
)
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
```

**Load JSON into VARIANT:**
```sql
CREATE TABLE raw_events (
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_data VARIANT
);

COPY INTO raw_events (raw_data)
FROM @my_stage/events/
FILE_FORMAT = (FORMAT_NAME = json_format);
```

### Loading Options

| Option | Description |
|--------|-------------|
| `FILES` | Specific files to load |
| `PATTERN` | Regex pattern for file selection |
| `ON_ERROR` | Error handling: CONTINUE, SKIP_FILE, ABORT_STATEMENT |
| `FORCE` | Reload previously loaded files |
| `PURGE` | Delete files after successful load |
| `VALIDATION_MODE` | Validate without loading |

**Error Handling:**
```sql
-- Continue loading despite errors (log errors)
COPY INTO orders FROM @my_stage
    FILE_FORMAT = (FORMAT_NAME = csv_format)
    ON_ERROR = 'CONTINUE';

-- Skip files with errors
COPY INTO orders FROM @my_stage
    FILE_FORMAT = (FORMAT_NAME = csv_format)
    ON_ERROR = 'SKIP_FILE';

-- Abort on first error (default)
COPY INTO orders FROM @my_stage
    FILE_FORMAT = (FORMAT_NAME = csv_format)
    ON_ERROR = 'ABORT_STATEMENT';
```

**Validate Before Loading:**
```sql
-- Validate syntax without loading
COPY INTO orders FROM @my_stage
    FILE_FORMAT = (FORMAT_NAME = csv_format)
    VALIDATION_MODE = 'RETURN_ERRORS';

-- Return sample rows
COPY INTO orders FROM @my_stage
    FILE_FORMAT = (FORMAT_NAME = csv_format)
    VALIDATION_MODE = 'RETURN_5_ROWS';
```

### Load History and Metadata

Track loading operations:

```sql
-- View load history for a table
SELECT *
FROM TABLE(information_schema.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

**METADATA Columns:**
During load, access metadata about source files:

```sql
COPY INTO orders (order_id, customer_id, order_date, order_total, source_file, load_time)
FROM (
    SELECT 
        $1, $2, $3::DATE, $4::DECIMAL(12,2),
        METADATA$FILENAME,      -- Source filename
        METADATA$FILE_ROW_NUMBER  -- Row number in file
    FROM @my_stage
)
FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
```

### Database Replication

Snowflake supports database replication across regions and cloud providers:

```sql
-- Enable replication (on source account)
ALTER DATABASE my_database ENABLE REPLICATION TO ACCOUNTS org.target_account;

-- Create replica (on target account)
CREATE DATABASE my_database_replica
    AS REPLICA OF org.source_account.my_database;

-- Refresh replica
ALTER DATABASE my_database_replica REFRESH;
```

**Use Cases:**
- Disaster recovery
- Geographic data distribution
- Cross-cloud data sharing

### Loading Best Practices

**1. File Sizing:**
- Aim for compressed file sizes between 100-250 MB
- Avoid very small files (overhead) or very large files (limited parallelism)

**2. File Compression:**
- Use GZIP or ZSTD compression for faster transfers
- Snowflake auto-detects common compression formats

**3. Parallel Loading:**
- Load multiple files simultaneously for maximum throughput
- Use patterns or wildcards to match multiple files

**4. Staging Strategy:**
- Use external stages for ongoing integrations
- Use internal stages for ad-hoc or one-time loads

**5. Incremental Loading:**
- Track loaded files to avoid duplicates
- Use `FORCE = FALSE` (default) to skip already-loaded files

## Summary

- **Stages** are landing zones for data files (internal or external)
- **File Formats** define how to parse CSV, JSON, Parquet, and other file types
- **COPY INTO** is the primary command for bulk data loading
- Use **transformations** during load to clean and type data
- Apply **error handling** options (ON_ERROR) for production pipelines
- **Validate** loads before executing with VALIDATION_MODE
- Follow **best practices** for file sizing and parallel loading

## Additional Resources

- [Snowflake Documentation: Data Loading](https://docs.snowflake.com/en/user-guide/data-load-overview)
- [Snowflake Documentation: COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
- [Snowflake Documentation: Stages](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage)
