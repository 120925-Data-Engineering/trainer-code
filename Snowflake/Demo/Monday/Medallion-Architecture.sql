-- We want to create our three layers
-- Bronze (Raw/Landing) 
    -- Holds our data exactly how it arrives. 
    -- Within Snowflake, uses Schema-on-read (like reading JSON in spark
        -- without an explicit schema)
    -- Can have duplicates, garbage data, not a concern yet

-- Silver (Cleaned data)
    -- Data is typed, and validated
    -- Like a StructType explicit Spark schema or DB table
    -- Ready for querying/analysis, not combined or aggregated yet

-- Gold (Business intelligence ready)
    -- Aggregated combined datasets for creating dashboards or reports
    -- This what PowerBI will connect to 
    -- Optimized to answer business questions (How many sales per region per        --    part?)
    -- Can be denormalized, use facts and dimensions, etc

CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Raw data layer - schema-on-read, no cleaning or transformations';

CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Cleaned data layer - validated, typed, no duplicates, nulls handled, etc';

CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Business data layer - aggregates, metrics, fact/dimension models';

SHOW SCHEMAS IN DATABASE DEV_DB;

-- Creating Bronze layer table

USE SCHEMA BRONZE;

-- Bronze tables often use a data type within Snowflake SQL called VARIANT
-- This is like spark.read.json() with no schema

CREATE OR REPLACE TABLE RAW_ORDERS (
    -- We probably want some metadata columns. They aren't required,
    -- and some of this info is almost certainly in the raw data - but its nice 
    -- to be able to see things like when/where this arrived from without
    -- unpacking the raw data
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), -- when the data arrived
    source_file STRING, -- where it arrived from 

    -- The actual data is stored below in a VARIANT data type column
    -- This can hold literally anything, JSON, CSV, Parquet, whatever we need
    raw_data VARIANT
)
COMMENT = 'Raw order events - no transformations or cleaning applied yet';

DESCRIBE TABLE RAW_ORDERS;

-- Silver zone table creation

USE SCHEMA SILVER;

CREATE OR REPLACE TABLE ORDERS (

    -- Keys 
    order_id STRING PRIMARY KEY, -- Primary key constraint 
    customer_id STRING NOT NULL,

    -- Typed and validated fields
    order_date DATE NOT NULL,
    order_status STRING,

    -- Monetary amount with a set precision
    amount DECIMAL(12, 2),

    -- Processing metadata
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Cleaned orders - typed/validated - ready for analysis';

DESCRIBE TABLE ORDERS;

-- Gold layer

USE SCHEMA GOLD;

CREATE OR REPLACE TABLE DAILY_REVENUE (
    -- Date dimension key (will explain later)
    report_date DATE PRIMARY KEY,

    -- Computed metrics
    total_orders INTEGER,
    total_revenue DECIMAL(14, 2),
    avg_order_value DECIMAL(10, 2),

    -- We can track refreshes as well if we want to
    refreshed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Daily revenue metrics - ready for dashboards/reporting with PowerBI/Tableu/etc';

DESCRIBE TABLE DAILY_REVENUE;