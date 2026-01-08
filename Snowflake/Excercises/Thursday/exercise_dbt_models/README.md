# Lab: Building dbt Models with ref() and source()

## Overview
**Day:** 4-Thursday  
**Duration:** 2-2.5 hours  
**Mode:** Individual (Code Lab)  
**Prerequisites:** dbt Setup Lab completed, dbt connected to Snowflake

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Define sources in dbt to reference raw data
2. Create staging models using `source()`
3. Build intermediate models using `ref()`
4. Understand the dbt DAG (dependency graph)
5. Apply proper model organization and naming conventions

---

## The Scenario

You have raw event data landing in your Bronze layer (BRONZE.RAW_EVENTS). Your task is to build a dbt transformation pipeline that:

1. **Stages** the raw data with proper typing and field extraction
2. **Aggregates** events at the daily level
3. **Creates** a business-ready metrics table

This mirrors the Bronze -> Silver -> Gold medallion architecture.

---

## Part 1: Understand the Source Data

Before building models, explore the source data in Snowflake:

```sql
-- Run in Snowflake to understand your source
USE DATABASE DEV_DB;
USE SCHEMA BRONZE;

-- View sample records
SELECT * FROM RAW_EVENTS LIMIT 10;

-- Understand the structure
DESCRIBE TABLE RAW_EVENTS;

-- Check event types
SELECT DISTINCT event_type, COUNT(*) 
FROM RAW_EVENTS 
GROUP BY event_type;
```

Take note of:
- Column names and types
- The `payload` column (is it JSON/VARIANT?)
- Date range of the data

---

## Part 2: Define Sources

Sources tell dbt about tables that exist outside of dbt (your raw data).

### Task 2.1: Create sources.yml

Create the file `models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: bronze
    description: "Raw data landing zone from streaming pipeline"
    database: DEV_DB
    schema: BRONZE
    tables:
      - name: raw_events
        description: "Raw event data from Kafka ingestion"
        columns:
          - name: event_id
            description: "Unique identifier for each event"
          - name: event_type
            description: "Type of event (CLICK, VIEW, PURCHASE, etc.)"
          - name: payload
            description: "JSON payload containing event details"
          - name: created_at
            description: "Timestamp when event was created"
```

### Task 2.2: Verify Source

```bash
# Compile to check for errors
dbt compile

# Check that dbt can find the source
dbt ls --resource-type source
```

**Question:** What happens if you misspell the database or schema name? Try it and note the error.

---

## Part 3: Create Staging Model

Staging models are the first transformation layer. They:
- Reference sources using `source()`
- Apply light transformations (typing, renaming, filtering nulls)
- Are typically materialized as views

### Task 3.1: Create stg_events.sql

Create the file `models/staging/stg_events.sql`:

```sql
{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for raw events.
    - Extracts fields from JSON payload
    - Normalizes event_type to uppercase
    - Filters out null event_ids
*/

WITH source_data AS (
    -- source() references tables defined in sources.yml
    SELECT * FROM {{ source('bronze', 'raw_events') }}
),

transformed AS (
    SELECT
        -- Primary key
        event_id,
        
        -- Normalized event type
        UPPER(event_type) AS event_type,
        
        -- Extracted payload fields (adjust based on your data!)
        -- If payload is VARIANT/JSON:
        payload:user::STRING AS user_id,
        payload:page::STRING AS page_url,
        payload:product::STRING AS product_id,
        payload:amount::DECIMAL(10,2) AS amount,
        
        -- Timestamp handling
        created_at AS event_timestamp,
        DATE(created_at) AS event_date,
        
        -- dbt metadata
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
        
    FROM source_data
    WHERE event_id IS NOT NULL
)

SELECT * FROM transformed
```

### Task 3.2: Run the Staging Model

```bash
# Run just this model
dbt run --select stg_events
```

### Task 3.3: Verify in Snowflake

```sql
-- Check your dbt schema
USE SCHEMA DBT_<YOUR_NAME>;

-- View the created view
SELECT * FROM STG_EVENTS LIMIT 10;

-- Check row counts match
SELECT COUNT(*) FROM STG_EVENTS;
SELECT COUNT(*) FROM BRONZE.RAW_EVENTS WHERE event_id IS NOT NULL;
```

---

## Part 4: Create Intermediate Model

Intermediate models aggregate or join staging models. They use `ref()` to reference other dbt models.

### Task 4.1: Create int_events_daily.sql

Create the directory and file `models/intermediate/int_events_daily.sql`:

```sql
{{
    config(
        materialized='view'
    )
}}

/*
    Intermediate model: Daily event aggregates
    - Groups events by date and type
    - Calculates daily metrics
*/

WITH events AS (
    -- ref() creates a dependency on stg_events
    -- dbt will run stg_events BEFORE this model
    SELECT * FROM {{ ref('stg_events') }}
),

daily_aggregates AS (
    SELECT
        event_date,
        event_type,
        
        -- Counts
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users,
        
        -- Amounts (only for events with amounts)
        SUM(COALESCE(amount, 0)) AS total_amount,
        AVG(COALESCE(amount, 0)) AS avg_amount,
        
        -- Time boundaries
        MIN(event_timestamp) AS first_event_at,
        MAX(event_timestamp) AS last_event_at
        
    FROM events
    GROUP BY event_date, event_type
)

SELECT * FROM daily_aggregates
```

### Task 4.2: Run with Dependencies

```bash
# Run just this model (dbt will also run stg_events if needed)
dbt run --select int_events_daily

# Or run with explicit upstream
dbt run --select +int_events_daily
```

**Question:** What does the `+` prefix mean in dbt selectors?

---

## Part 5: Create Mart Model

Mart models are business-ready tables, typically materialized as tables for query performance.

### Task 5.1: Create fct_daily_metrics.sql

Create `models/marts/fct_daily_metrics.sql`:

```sql
{{
    config(
        materialized='table'
    )
}}

/*
    Mart model: Daily metrics fact table
    - Business-ready aggregates
    - Includes calculated KPIs
    - Materialized as table for BI tool performance
*/

WITH daily_events AS (
    SELECT * FROM {{ ref('int_events_daily') }}
),

enriched AS (
    SELECT
        -- Dimensions
        event_date,
        event_type,
        
        -- Measures
        event_count,
        unique_users,
        total_amount,
        
        -- Calculated KPIs
        ROUND(total_amount / NULLIF(event_count, 0), 2) AS revenue_per_event,
        ROUND(total_amount / NULLIF(unique_users, 0), 2) AS revenue_per_user,
        
        -- Time metadata
        first_event_at,
        last_event_at,
        DATEDIFF('minute', first_event_at, last_event_at) AS active_minutes,
        
        -- dbt metadata
        CURRENT_TIMESTAMP() AS _dbt_refreshed_at
        
    FROM daily_events
)

SELECT * FROM enriched
ORDER BY event_date DESC, event_type
```

### Task 5.2: Run Full Pipeline

```bash
# Run all models
dbt run

# Check the output
dbt run --select fct_daily_metrics
```

---

## Part 6: Visualize the DAG

dbt automatically builds a dependency graph from your `source()` and `ref()` calls.

### Task 6.1: Generate Documentation

```bash
dbt docs generate
dbt docs serve --port 8081
```

### Task 6.2: Explore the Lineage Graph

1. Open http://localhost:8081
2. Click the graph icon (lineage)
3. Find your models and trace the flow:

```
BRONZE.RAW_EVENTS (source)
         |
         | source('bronze', 'raw_events')
         v
    stg_events (view)
         |
         | ref('stg_events')
         v
  int_events_daily (view)
         |
         | ref('int_events_daily')
         v
  fct_daily_metrics (table)
```

**Screenshot:** Capture the lineage graph for your deliverables.

---

## Part 7: Experiment with ref()

### Task 7.1: What Happens with Circular References?

Create a file `models/marts/test_circular.sql`:

```sql
-- DON'T ACTUALLY RUN THIS - just to understand the error
SELECT * FROM {{ ref('test_circular') }}
```

```bash
# This will fail - why?
dbt compile --select test_circular
```

Delete the file after observing the error.

### Task 7.2: Multiple refs()

Create a new model that references multiple upstream models:

`models/marts/dim_event_summary.sql`:

```sql
{{
    config(
        materialized='table'
    )
}}

-- Join staging with aggregates for a denormalized view
WITH events AS (
    SELECT DISTINCT event_type FROM {{ ref('stg_events') }}
),

metrics AS (
    SELECT 
        event_type,
        SUM(event_count) AS total_events,
        SUM(unique_users) AS total_unique_users,
        SUM(total_amount) AS grand_total_amount
    FROM {{ ref('int_events_daily') }}
    GROUP BY event_type
)

SELECT
    e.event_type,
    COALESCE(m.total_events, 0) AS total_events,
    COALESCE(m.total_unique_users, 0) AS total_unique_users,
    COALESCE(m.grand_total_amount, 0) AS grand_total_amount
FROM events e
LEFT JOIN metrics m ON e.event_type = m.event_type
```

```bash
dbt run --select dim_event_summary
```

Check the lineage graph again - how does this model connect?

---

## Deliverables

Submit the following:

1. **dbt Project Folder:** Zipped or link to your models directory
2. **Lineage Graph Screenshot:** Showing your full DAG
3. **Query Results:** Screenshot of `SELECT * FROM fct_daily_metrics LIMIT 10` in Snowflake
4. **Written Answers:**
   - What is the difference between `source()` and `ref()`?
   - Why are staging models typically views and marts typically tables?
   - What error message did the circular reference produce?

---

## Definition of Done

- [ ] sources.yml created with bronze source defined
- [ ] stg_events.sql created and runs successfully
- [ ] int_events_daily.sql created using ref()
- [ ] fct_daily_metrics.sql created as materialized table
- [ ] All models run without errors (`dbt run`)
- [ ] Lineage graph shows correct dependencies
- [ ] dim_event_summary.sql (bonus) uses multiple refs

---

## Bonus Challenges

1. **Freshness:** Add `freshness` configuration to your source and run `dbt source freshness`
2. **Tags:** Add tags to your models and run `dbt run --select tag:staging`
3. **Custom Schema:** Configure staging models to write to a different schema than marts
