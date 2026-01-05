# Silver Table Design Template

## Table: [TABLE_NAME]

### Source Information
| Attribute | Value |
|-----------|-------|
| Source Bronze Table | |
| Primary Key | |
| Deduplication Strategy | |

### Column Definitions

| Column Name | Data Type | Source Expression | Transformation |
|-------------|-----------|-------------------|----------------|
| | | | |
| | | | |
| | | | |

### Transformations Applied

1. **Data Typing:**
   - 

2. **Standardization:**
   - 

3. **Null Handling:**
   - 

4. **Deduplication:**
   - 

### DDL Statement

```sql
CREATE OR REPLACE TABLE SILVER.[TABLE_NAME] (
    -- Primary key
    
    -- Business columns
    
    -- Metadata
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY ()
)
COMMENT = '[Description]';
```

### Sample Transformation Query

```sql
SELECT
    -- Extract and type cast from Bronze
FROM BRONZE.[SOURCE_TABLE]
WHERE [deduplication logic]
```
