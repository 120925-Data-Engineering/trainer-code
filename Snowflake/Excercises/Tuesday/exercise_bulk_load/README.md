# Exercise: Bulk Data Loading

## Overview
**Day:** 2-Tuesday  
**Duration:** 2-3 hours  
**Mode:** Individual (Code Lab)  
**Prerequisites:** SnowSQL exercise completed, BRONZE schema exists

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Create file formats for CSV and JSON data
2. Set up internal stages for file staging
3. Load data using COPY INTO command
4. Monitor load status and handle errors

---

## The Scenario
The analytics team has provided you with two data files that need to be loaded into Snowflake:
1. A CSV file containing product sales data
2. A JSON file containing customer interaction events

Your task is to load both files into your Bronze layer tables.

---

## Core Tasks

### Task 1: Create File Formats (20 mins)

```sql
USE DATABASE <YOUR_NAME>_DEV_DB;
USE SCHEMA BRONZE;

-- Create CSV file format
CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '', 'N/A')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Standard CSV format for product data';

-- Create JSON file format
CREATE OR REPLACE FILE FORMAT MY_JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'JSON format for event data';

-- Verify formats
SHOW FILE FORMATS;
```

---

### Task 2: Create Internal Stages (15 mins)

```sql
-- Create stage for CSV files
CREATE OR REPLACE STAGE CSV_LOAD_STAGE
    FILE_FORMAT = MY_CSV_FORMAT
    COMMENT = 'Stage for loading CSV product data';

-- Create stage for JSON files
CREATE OR REPLACE STAGE JSON_LOAD_STAGE
    FILE_FORMAT = MY_JSON_FORMAT
    COMMENT = 'Stage for loading JSON event data';

-- Verify stages
SHOW STAGES;
```

---

### Task 3: Create Target Tables (15 mins)

```sql
-- Table for CSV data (products)
CREATE OR REPLACE TABLE RAW_PRODUCTS (
    product_id STRING,
    product_name STRING,
    category STRING,
    price DECIMAL(10,2),
    quantity_sold INTEGER,
    sale_date DATE,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Table for JSON data (events)
CREATE OR REPLACE TABLE RAW_CUSTOMER_EVENTS (
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_data VARIANT
);
```

---

### Task 4: Upload Files (20 mins)

**Option A: Using Snowsight UI (Recommended)**
1. Navigate to Data > Databases > Your Database > BRONZE
2. Click on CSV_LOAD_STAGE
3. Click "Upload Files"
4. Upload `starter_code/products.csv`
5. Repeat for JSON_LOAD_STAGE with `starter_code/events.json`

**Option B: Using SnowSQL PUT command**
```bash
# In SnowSQL
PUT file://./starter_code/products.csv @CSV_LOAD_STAGE;
PUT file://./starter_code/events.json @JSON_LOAD_STAGE;
```

Verify files are staged:
```sql
LIST @CSV_LOAD_STAGE;
LIST @JSON_LOAD_STAGE;
```

---

### Task 5: Load Data with COPY INTO (30 mins)

```sql
-- Load CSV data
COPY INTO RAW_PRODUCTS (product_id, product_name, category, price, quantity_sold, sale_date)
FROM @CSV_LOAD_STAGE
FILE_FORMAT = (FORMAT_NAME = MY_CSV_FORMAT)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Check results
SELECT * FROM RAW_PRODUCTS;
SELECT COUNT(*) AS rows_loaded FROM RAW_PRODUCTS;

-- Load JSON data
COPY INTO RAW_CUSTOMER_EVENTS (raw_data)
FROM @JSON_LOAD_STAGE
FILE_FORMAT = (FORMAT_NAME = MY_JSON_FORMAT)
ON_ERROR = 'CONTINUE';

-- Check results
SELECT * FROM RAW_CUSTOMER_EVENTS;
```

---

### Task 6: Verify Load History (20 mins)

```sql
-- Check CSV load history
SELECT 
    TABLE_NAME,
    FILE_NAME,
    STATUS,
    ROWS_PARSED,
    ROWS_LOADED,
    ERRORS_SEEN,
    FIRST_ERROR
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_PRODUCTS',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));

-- Check JSON load history
SELECT 
    TABLE_NAME,
    FILE_NAME,
    STATUS,
    ROWS_PARSED,
    ROWS_LOADED,
    ERRORS_SEEN
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW_CUSTOMER_EVENTS',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));
```

Document any errors encountered and how you resolved them.

---

### Task 7: Query Loaded Data (30 mins)

Write queries to answer:

1. **Products:** What is the total revenue (price * quantity) by category?

2. **Events:** Extract the event_type from the JSON and count occurrences:
```sql
SELECT 
    raw_data:type::STRING AS event_type,
    COUNT(*) AS event_count
FROM RAW_CUSTOMER_EVENTS
GROUP BY event_type;
```

3. Create your own analysis query for each table.

---

## Deliverables

1. **SQL Script:** `bulk_load.sql` containing all DDL and COPY commands
2. **Screenshot:** COPY_HISTORY output showing successful loads
3. **Query Results:** Output from Task 7 queries
4. **Error Log:** Any errors encountered and resolution steps

---

## Definition of Done

- [ ] CSV file format created
- [ ] JSON file format created
- [ ] Both stages created and verified
- [ ] Target tables created
- [ ] Files uploaded to stages
- [ ] COPY INTO successful for both files
- [ ] Load history verified
- [ ] Analysis queries completed

---

## Starter Code

The `starter_code/` directory contains:
- `products.csv` - Sample product sales data (10 rows)
- `events.json` - Sample customer events (5 events)

---

## Error Handling Challenge

Intentionally modify the CSV to have a bad row:
1. Add a row with incorrect date format
2. Attempt to load with ON_ERROR = 'ABORT_STATEMENT'
3. Note the error message
4. Change to ON_ERROR = 'CONTINUE' and reload
5. Document the difference in behavior
