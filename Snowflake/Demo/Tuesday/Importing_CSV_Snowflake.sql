-- First, lets see what file formats are available for use in our DEV_DB

SHOW FILE FORMATS; -- 0 results

-- I don't see any, so lets go ahead and configure one for CSV and one for JSON

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '', 'N/A')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard CSV format with header';

-- Lets create a JSON format

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON format for event data (later)';

-- Phase 2: Lets create an internal stage

-- In Snowflake, a stage is where files live before they're loaded into our instance
-- Think of it like the S3 bucket we used when working with Spark EMR
-- Internal stages are managed by snowflake, they're just S3 buckets (provided you chose AWS during account creation)
-- In production we'd probably use an external stage, that explicitly points to a managed S3 bucket - managed by our team/org, but that would require linking our AWS accounts and dealing with IAM hell

CREATE OR REPLACE STAGE INTERNAL_LOAD_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Internal stage for loading CSV files';

-- Lets see if we can do a load into our raw (Bronze zone) table 

LIST @INTERNAL_LOAD_STAGE;

DESCRIBE TABLE BRONZE.RAW_ORDERS;

-- Knowing full well we can use the UI to straight load a file into a table, we'll do this the old fashioned way. Atleast once for demo.

-- We gotta do something a little roundabout for CSV formatted data, because Snowflake
-- won't preserve the header row when we copy in via SnowflakeSQL this way

-- First, we create a temp staging table
CREATE OR REPLACE TEMPORARY TABLE STAGING_ORDERS (
    order_id STRING,
    customer_id STRING,
    order_date STRING,
    amount STRING,
    status STRING
);

-- Next we copy into
COPY INTO STAGING_ORDERS
FROM @INTERNAL_LOAD_STAGE
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
ON_ERROR = 'CONTINUE'
PURGE = FALSE; -- Keep files after loading, don't throw them out of the stage
-- Set purge to true once you get your ingestion logic working

SELECT * FROM STAGING_ORDERS;

DESCRIBE TABLE BRONZE.RAW_ORDERS;

-- Now that we have things in STAGING_ORDERS
-- Lets get things into the Bronze.RAW_ORDERS table with Variant. 

INSERT INTO BRONZE.RAW_ORDERS (source_file, raw_data)
SELECT 
    'sample_orders.csv' as source_file, 
    OBJECT_CONSTRUCT(
        'order_id', order_id,
        'customer_id', customer_id,
        'order_date', order_date,
        'amount', amount,
        'status', status
    ) AS raw_data
FROM STAGING_ORDERS;

SELECT * FROM BRONZE.RAW_ORDERS LIMIT 10;