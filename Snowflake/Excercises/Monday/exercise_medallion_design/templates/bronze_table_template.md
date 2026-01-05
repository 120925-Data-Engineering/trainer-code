# Bronze Table Design Template

## Table: [TABLE_NAME]

### Source Information
| Attribute | Value |
|-----------|-------|
| Source System | |
| Data Format | |
| Update Frequency | |
| Load Method | |

### Column Definitions

| Column Name | Data Type | Description | Sample Value |
|-------------|-----------|-------------|--------------|
| ingestion_ts | TIMESTAMP_NTZ | When data was loaded | 2024-01-15 10:30:00 |
| source_file | STRING | Source file name | orders_20240115.csv |
| raw_data | VARIANT | Raw JSON payload | {...} |
| | | | |
| | | | |

### DDL Statement

```sql
CREATE OR REPLACE TABLE BRONZE.[TABLE_NAME] (
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT
)
COMMENT = '[Description of what this table contains]';
```

### Notes
- 
- 
