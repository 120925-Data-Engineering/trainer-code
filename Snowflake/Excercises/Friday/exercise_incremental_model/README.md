# Exercise: Incremental Models

## Overview
**Day:** 5-Friday  
**Duration:** 2-3 hours  
**Mode:** Individual (Code Lab)  
**Prerequisites:** Thursday dbt project completed

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| dbt Transformations | [dbt-transformations.md](../../content/5-Friday/dbt-transformations.md) | Incremental models, materializations, Jinja |
| dbt Use Cases | [dbt-use-cases.md](../../content/5-Friday/dbt-use-cases.md) | Production pipelines, real-world patterns |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Understand when to use incremental models
2. Implement is_incremental() logic correctly
3. Use different incremental strategies (merge, delete+insert, append)
4. Test and troubleshoot incremental loads

---

## The Scenario

Your event data grows by millions of rows daily. Rebuilding the entire table each run is:
- Expensive (compute costs)
- Slow (hours instead of minutes)
- Risky (full table locks)

You need to implement incremental processing that only handles new data.

---

## Core Tasks

### Task 1: Understand the Problem (15 mins)

Current approach (full refresh):
```sql
-- Every run processes ALL data
CREATE OR REPLACE TABLE fct_events AS
SELECT * FROM stg_events;
-- 10 million rows = slow and expensive
```

Better approach (incremental):
```sql
-- First run: process all data
-- Subsequent runs: only new data since last run
-- 100,000 new rows = fast and cheap
```

---

### Task 2: Create Incremental Model (45 mins)

Create `models/marts/fct_events_incremental.sql`:

```sql
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

WITH new_events AS (
    SELECT
        event_id,
        event_type,
        user_id,
        page_url,
        product_id,
        amount,
        event_timestamp,
        _loaded_at
    FROM {{ ref('stg_events') }}
    
    {% if is_incremental() %}
    -- Only get new records since last run
    WHERE event_timestamp > (
        SELECT COALESCE(MAX(event_timestamp), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS _processed_at
FROM new_events
```

---

### Task 3: Test First Run (20 mins)

```bash
# First run - processes all data
dbt run --select fct_events_incremental

# Check row count
dbt run-operation get_row_count --args '{model_name: fct_events_incremental}'
```

Or query Snowflake directly:
```sql
SELECT COUNT(*) FROM DBT_<YOUR_SCHEMA>.FCT_EVENTS_INCREMENTAL;
```

---

### Task 4: Add New Data and Test Incremental (30 mins)

1. Add new events to source:

```sql
INSERT INTO <YOUR_NAME>_DEV_DB.BRONZE.RAW_EVENTS (event_id, event_type, payload, created_at) 
VALUES
    ('E100', 'click', PARSE_JSON('{"user": "U200", "page": "/new-page"}'), DATEADD('day', 1, CURRENT_TIMESTAMP())),
    ('E101', 'purchase', PARSE_JSON('{"user": "U201", "amount": 299.99}'), DATEADD('day', 1, CURRENT_TIMESTAMP())),
    ('E102', 'view', PARSE_JSON('{"user": "U200", "page": "/checkout"}'), DATEADD('day', 1, CURRENT_TIMESTAMP()));
```

2. Run incremental:

```bash
dbt run --select fct_events_incremental
```

3. Check that only new rows were processed:
   - Look at the dbt output for rows affected
   - Query the table to verify new records exist

---

### Task 5: Compare with Full Refresh (20 mins)

```bash
# Force full refresh
dbt run --select fct_events_incremental --full-refresh

# Compare query IDs in Snowflake Query History
# Note the difference in rows processed and execution time
```

Document:
- Incremental run: rows processed, execution time
- Full refresh: rows processed, execution time
- Difference in performance

---

### Task 6: Try Different Strategies (30 mins)

Modify your model to try different strategies:

**Strategy 1: Append (no dedup)**
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}
-- No unique_key, just appends rows
-- Fastest, but may have duplicates
```

**Strategy 2: Delete+Insert**
```sql
{{
    config(
        materialized='incremental',
        unique_key='event_date',
        incremental_strategy='delete+insert'
    )
}}
-- Deletes all rows for matching event_date, then inserts
-- Good for partitioned data
```

Document when you would use each strategy.

---

### Task 7: Handle Late-Arriving Data (20 mins)

Modify your incremental logic to handle late data:

```sql
{% if is_incremental() %}
-- Look back 3 days to catch late arrivals
WHERE event_timestamp > (
    SELECT DATEADD('day', -3, COALESCE(MAX(event_timestamp), '1900-01-01'::TIMESTAMP))
    FROM {{ this }}
)
{% endif %}
```

Discuss: What are the tradeoffs of this approach?

---

## Deliverables

1. **Incremental Model:** `fct_events_incremental.sql`
2. **Performance Comparison:** Document comparing incremental vs full refresh
3. **Strategy Analysis:** When to use merge vs append vs delete+insert

---

## Definition of Done

- [ ] Incremental model created with is_incremental() logic
- [ ] First run (full load) successful
- [ ] Incremental run (new data only) successful
- [ ] Full refresh tested and compared
- [ ] At least one alternative strategy tested
- [ ] Performance documented

---

## Key Concepts

| Concept | Meaning |
|---------|---------|
| `is_incremental()` | Returns TRUE on runs after the first |
| `{{ this }}` | References the current model's table |
| `unique_key` | Column(s) used for merge matching |
| `--full-refresh` | Forces complete rebuild |
| `incremental_strategy` | How to handle existing rows |

---

## Best Practices

1. **Always have a reliable timestamp** for filtering new records
2. **Handle late-arriving data** with lookback window
3. **Use merge** when you need upsert behavior
4. **Use append** for immutable event logs
5. **Test with --full-refresh** periodically to ensure consistency
