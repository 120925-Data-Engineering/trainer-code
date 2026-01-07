-- INSTRUCTIONS:
-- 1. Log into Snowflake with ACCOUNTADMIN role
-- 2. Open a new worksheet
-- 3. Paste and run this entire script
-- 4. Verify all objects were created successfully

-- Set context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

ALTER WAREHOUSE COMPUTE_WH SET 
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS DEV_DB
    COMMENT = 'Development database for Week 5 Snowflake training';

USE DATABASE DEV_DB;

-- CREATE MEDALLION ARCHITECTURE SCHEMAS

-- Bronze: Raw data landing zone
CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Raw data layer - schema-on-read, no transformations';

-- Silver: Cleansed and typed data
CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Cleansed layer - validated, typed, deduplicated';

-- Gold: Business-ready aggregates
CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Business layer - aggregates, metrics, dimensional models';

-- Sandbox for experiments
CREATE SCHEMA IF NOT EXISTS SANDBOX
    COMMENT = 'Personal sandbox for experiments';

-- CREATE FILE FORMATS
USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '', 'N/A')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard CSV format with header';

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON format for event data';

-- CREATE INTERNAL STAGES
CREATE OR REPLACE STAGE INTERNAL_LOAD_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for loading CSV files';

CREATE OR REPLACE STAGE JSON_STAGE
    FILE_FORMAT = JSON_FORMAT
    COMMENT = 'Internal stage for loading JSON files';

-- CREATE BRONZE TABLES

-- Raw orders 
CREATE OR REPLACE TABLE BRONZE.LOADED_ORDERS (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    amount DECIMAL(10,2),
    status STRING,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw events (for Streams & Tasks demo)
CREATE OR REPLACE TABLE BRONZE.RAW_EVENTS (
    event_id STRING,
    event_type STRING,
    payload VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw orders with VARIANT (for Medallion demo)
CREATE OR REPLACE TABLE BRONZE.RAW_ORDERS (
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT
);

-- CREATE SILVER TABLES

CREATE OR REPLACE TABLE SILVER.ORDERS (
    order_id STRING NOT NULL,
    customer_id STRING,
    order_date DATE,
    amount DECIMAL(12,2),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (order_id)
);

CREATE OR REPLACE TABLE SILVER.PROCESSED_EVENTS (
    event_id STRING,
    event_type STRING,
    user_id STRING,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- CREATE GOLD TABLES

CREATE OR REPLACE TABLE GOLD.DAILY_REVENUE (
    report_date DATE NOT NULL,
    total_orders INTEGER,
    total_revenue DECIMAL(14,2),
    avg_order_value DECIMAL(10,2),
    refreshed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (report_date)
);

CREATE OR REPLACE TABLE GOLD.EVENT_SUMMARY (
    event_type STRING,
    event_count INTEGER
);

-- VERIFICATION

-- Show all schemas
SHOW SCHEMAS IN DATABASE DEV_DB;

-- Show all tables
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    COMMENT
FROM DEV_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA IN ('BRONZE', 'SILVER', 'GOLD')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Success message
SELECT 'Week 5 Setup Complete!' AS status,
    (SELECT COUNT(*) FROM DEV_DB.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME IN ('BRONZE','SILVER','GOLD','SANDBOX')) AS schemas_created,
    (SELECT COUNT(*) FROM DEV_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('BRONZE','SILVER','GOLD')) AS tables_created;
