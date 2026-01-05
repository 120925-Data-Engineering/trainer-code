# Exercise: Semi-Structured Data Querying

## Overview
**Day:** 2-Tuesday  
**Duration:** 2-3 hours  
**Mode:** Individual (Code Lab)  
**Prerequisites:** Bulk Load exercise completed

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Query semi-structured JSON data using VARIANT type
2. Extract nested fields using dot notation and bracket notation
3. Use LATERAL FLATTEN to unnest arrays
4. Transform JSON into relational table structures

---

## The Scenario
Your RAW_CUSTOMER_EVENTS table contains complex JSON event data. The analytics team needs you to extract specific fields and create structured views for their dashboards.

---

## Core Tasks

### Task 1: Understanding the JSON Structure (20 mins)

First, examine the raw data:
```sql
USE DATABASE <YOUR_NAME>_DEV_DB;
USE SCHEMA BRONZE;

-- View raw JSON
SELECT raw_data FROM RAW_CUSTOMER_EVENTS LIMIT 5;

-- View formatted JSON (Snowsight will format it nicely)
SELECT raw_data, TYPEOF(raw_data) AS data_type FROM RAW_CUSTOMER_EVENTS;
```

Load additional sample data for this exercise:
```sql
-- Insert more complex events with arrays
INSERT INTO RAW_CUSTOMER_EVENTS (raw_data)
SELECT PARSE_JSON(column1) FROM VALUES
('{"event_id": "E006", "type": "cart_update", "user_id": "U102", "items": [{"product_id": "P001", "qty": 1}, {"product_id": "P002", "qty": 2}]}'),
('{"event_id": "E007", "type": "cart_update", "user_id": "U103", "items": [{"product_id": "P003", "qty": 1}]}'),
('{"event_id": "E008", "type": "order", "user_id": "U102", "items": [{"product_id": "P001", "qty": 1, "price": 1299.99}, {"product_id": "P002", "qty": 2, "price": 29.99}], "total": 1359.97}');
```

---

### Task 2: Field Extraction with Dot Notation (30 mins)

```sql
-- Basic field extraction
SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:type::STRING AS event_type,
    raw_data:user_id::STRING AS user_id,
    raw_data:timestamp::TIMESTAMP AS event_time
FROM RAW_CUSTOMER_EVENTS;

-- Extract nested properties
SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:type::STRING AS event_type,
    raw_data:properties:page::STRING AS page_viewed,
    raw_data:properties:product_id::STRING AS product_id,
    raw_data:properties:query::STRING AS search_query
FROM RAW_CUSTOMER_EVENTS
WHERE raw_data:properties IS NOT NULL;
```

**Exercise:** Write a query that extracts:
- event_id
- user_id  
- The order_id from properties (if it exists)
- The total from properties (if it exists)

---

### Task 3: Handling Arrays with LATERAL FLATTEN (45 mins)

```sql
-- View events with items array
SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:items AS items_array,
    ARRAY_SIZE(raw_data:items) AS item_count
FROM RAW_CUSTOMER_EVENTS
WHERE raw_data:items IS NOT NULL;

-- FLATTEN the items array
SELECT 
    e.raw_data:event_id::STRING AS event_id,
    e.raw_data:user_id::STRING AS user_id,
    f.value:product_id::STRING AS product_id,
    f.value:qty::INTEGER AS quantity,
    f.value:price::DECIMAL(10,2) AS unit_price
FROM RAW_CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.raw_data:items) f
WHERE e.raw_data:items IS NOT NULL;
```

**Key Concept:** LATERAL FLATTEN turns each array element into a separate row.

**Exercise:** Calculate the total value of items per event (quantity * price, summed).

---

### Task 4: Conditional Extraction (30 mins)

Handle different event types with different structures:

```sql
SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:type::STRING AS event_type,
    
    -- Conditional extraction based on event type
    CASE 
        WHEN raw_data:type = 'page_view' THEN raw_data:properties:page::STRING
        WHEN raw_data:type = 'search' THEN raw_data:properties:query::STRING
        WHEN raw_data:type = 'purchase' THEN raw_data:properties:order_id::STRING
        ELSE NULL
    END AS context_value,
    
    -- Safe extraction with COALESCE
    COALESCE(
        raw_data:properties:product_id::STRING,
        raw_data:items[0]:product_id::STRING,
        'N/A'
    ) AS primary_product
    
FROM RAW_CUSTOMER_EVENTS;
```

---

### Task 5: Create Structured Views (30 mins)

Create views that transform JSON into relational structures:

```sql
-- View 1: All events with extracted fields
CREATE OR REPLACE VIEW V_EVENTS_EXTRACTED AS
SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:type::STRING AS event_type,
    raw_data:user_id::STRING AS user_id,
    raw_data:timestamp::TIMESTAMP AS event_timestamp,
    raw_data:properties AS properties,
    _loaded_at
FROM RAW_CUSTOMER_EVENTS;

-- View 2: Cart items flattened
CREATE OR REPLACE VIEW V_CART_ITEMS AS
SELECT 
    e.raw_data:event_id::STRING AS event_id,
    e.raw_data:user_id::STRING AS user_id,
    f.index AS item_index,
    f.value:product_id::STRING AS product_id,
    f.value:qty::INTEGER AS quantity,
    f.value:price::DECIMAL(10,2) AS unit_price
FROM RAW_CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.raw_data:items) f
WHERE e.raw_data:items IS NOT NULL;

-- Test the views
SELECT * FROM V_EVENTS_EXTRACTED;
SELECT * FROM V_CART_ITEMS;
```

---

### Task 6: Analysis Queries (30 mins)

Write queries to answer:

1. **Event Distribution:** How many events of each type?

2. **User Activity:** Which user has the most events?

3. **Cart Analysis:** What is the average number of items per cart?

4. **Search Analysis:** What are the most common search queries?

5. **Purchase Patterns:** What is the average order total for purchase events?

---

## Deliverables

1. **SQL Script:** `semistructured_queries.sql` with all your queries
2. **Views Created:** Names and purposes of views you created
3. **Analysis Answers:** Written answers to Task 6 questions
4. **Challenge Solution:** (See stretch goals)

---

## Definition of Done

- [ ] Sample data loaded successfully
- [ ] Dot notation extraction working
- [ ] LATERAL FLATTEN queries working
- [ ] Conditional extraction implemented
- [ ] At least 2 views created
- [ ] All analysis questions answered

---

## Key Syntax Reference

| Operation | Syntax | Example |
|-----------|--------|---------|
| Extract field | `col:field` | `raw_data:event_id` |
| Extract nested | `col:parent:child` | `raw_data:properties:page` |
| Array element | `col:array[n]` | `raw_data:items[0]` |
| Cast to type | `::TYPE` | `::STRING`, `::INTEGER` |
| Flatten array | `LATERAL FLATTEN(input => col:array)` | See Task 3 |
| Array size | `ARRAY_SIZE(col:array)` | `ARRAY_SIZE(raw_data:items)` |

---

## Stretch Goals

1. Create a stored procedure that processes raw events into a Silver layer table
2. Handle events with missing or malformed JSON gracefully
3. Build a query that tracks user journey (sequence of events per user)
