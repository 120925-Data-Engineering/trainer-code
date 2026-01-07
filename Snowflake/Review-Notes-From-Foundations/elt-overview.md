# ELT Overview

## Learning Objectives

- Understand what ELT is and how it differs from ETL
- Learn the advantages of ELT for cloud data warehouses
- Know when ELT is preferred over ETL
- Apply ELT patterns using BigQuery

## Why This Matters

Cloud data warehouses like BigQuery have changed how we build data pipelines. With powerful compute built into the warehouse, we can load raw data first and transform it there (ELT). This modern approach simplifies architecture and improves agility.

## The Concept

### What is ELT?

ELT (Extract, Load, Transform) is a data integration approach where data is loaded into the target system first, then transformed there.

**ETL (Traditional):**
```
Source --> Extract --> Transform (ETL Server) --> Load --> Warehouse
```

**ELT (Modern):**
```
Source --> Extract --> Load --> Transform (In Warehouse) --> Warehouse
```

### The Key Difference

| Aspect | ETL | ELT |
|--------|-----|-----|
| Transform location | Separate ETL server | Inside data warehouse |
| Transform engine | Informatica, SSIS, etc. | SQL in BigQuery, Redshift |
| Raw data storage | Not stored in warehouse | Stored in staging tables |
| Flexibility | Transformations fixed | Can re-transform raw data |

### Why ELT Emerged

Cloud data warehouses changed the game:

**Traditional (ETL Era):**
- Warehouse compute was expensive and limited
- Made sense to transform on cheaper ETL servers
- Transform, then load clean data

**Modern (ELT Era):**
- Cloud warehouses have massive, elastic compute
- Storage is cheap
- Load everything, transform with SQL
- Leverage warehouse optimization

### ELT Architecture

```
+----------+     +--------+     +----------+     +------------+     +----------+
| Source   | --> | Extract| --> | Load to  | --> | Transform  | --> | Analytics|
| Systems  |     +--------+     | Staging  |     | (SQL in DW)|     +----------+
+----------+                    +----------+     +------------+
                                     |                |
                            Raw data stored    Results to final tables
```

### ELT Stages

**1. Extract:**
Same as ETL - pull data from sources.

**2. Load:**
Load raw data directly into the warehouse (staging area).

```sql
-- Load raw data to staging
LOAD DATA INTO staging.raw_orders
FROM FILES (
  format = 'JSON',
  uris = ['gs://bucket/orders/*.json']
);
```

**3. Transform:**
Use SQL to transform data within the warehouse.

```sql
-- Transform in warehouse using SQL
CREATE OR REPLACE TABLE warehouse.fact_orders AS
SELECT
    ROW_NUMBER() OVER() AS order_key,
    JSON_VALUE(data, '$.order_id') AS order_id,
    CAST(JSON_VALUE(data, '$.total') AS NUMERIC) AS total,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.date')) AS order_date,
    CURRENT_TIMESTAMP() AS loaded_at
FROM staging.raw_orders;
```

### Benefits of ELT

| Benefit | Description |
|---------|-------------|
| Simpler architecture | No separate ETL servers needed |
| Faster ingestion | Load raw, no transform delay |
| Flexibility | Can re-transform historical data |
| Raw data preserved | Original data always available |
| SQL-based | Analysts can build transformations |
| Scalable | Leverage warehouse compute power |
| Lower latency | Data available faster |

### ELT Patterns

**Pattern 1: Staging to Core**
```sql
-- Raw landing
CREATE TABLE staging.raw_customers AS
SELECT * FROM EXTERNAL_SOURCE;

-- Transform to dimension
CREATE TABLE warehouse.dim_customers AS
SELECT
    customer_id,
    UPPER(TRIM(name)) AS name,
    LOWER(email) AS email,
    CURRENT_DATE() AS effective_date
FROM staging.raw_customers;
```

**Pattern 2: Incremental ELT**
```sql
-- Load new raw data
INSERT INTO staging.raw_orders
SELECT * FROM EXTERNAL_SOURCE
WHERE created_at > (SELECT MAX(created_at) FROM staging.raw_orders);

-- Transform incrementally
INSERT INTO warehouse.fact_orders
SELECT transform_logic(*)
FROM staging.raw_orders
WHERE NOT EXISTS (
    SELECT 1 FROM warehouse.fact_orders f 
    WHERE f.order_id = raw_orders.order_id
);
```

**Pattern 3: Views for Transformation**
```sql
-- Transformation as view (always fresh)
CREATE VIEW warehouse.v_customer_orders AS
SELECT
    c.name,
    COUNT(o.order_id) AS order_count,
    SUM(o.total) AS total_spent
FROM staging.raw_orders o
JOIN staging.raw_customers c ON o.customer_id = c.customer_id
GROUP BY c.name;
```

**Pattern 4: Materialized Views**
```sql
-- Pre-computed for performance
CREATE MATERIALIZED VIEW warehouse.mv_daily_sales AS
SELECT
    DATE(order_date) AS date,
    SUM(total) AS daily_total,
    COUNT(*) AS order_count
FROM staging.raw_orders
GROUP BY DATE(order_date);
```

### ELT in BigQuery

BigQuery is designed for ELT:

```sql
-- 1. Load raw data from Cloud Storage
LOAD DATA INTO staging.raw_events
FROM FILES (
  format = 'PARQUET',
  uris = ['gs://data-lake/events/*.parquet']
);

-- 2. Transform with SQL
CREATE OR REPLACE TABLE analytics.user_activity AS
SELECT
    user_id,
    DATE(event_timestamp) AS event_date,
    event_type,
    COUNT(*) AS event_count
FROM staging.raw_events
WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY user_id, DATE(event_timestamp), event_type;

-- 3. Create views for different consumers
CREATE VIEW reports.daily_active_users AS
SELECT event_date, COUNT(DISTINCT user_id) AS dau
FROM analytics.user_activity
GROUP BY event_date;
```

### ELT Orchestration

ELT pipelines need orchestration:

```
Cloud Scheduler / Airflow
         |
         v
+--------+--------+
|  1. Load raw    |  (Cloud Storage -> BigQuery)
+--------+--------+
         |
         v
+--------+--------+
|  2. Transform   |  (SQL script / dbt)
+--------+--------+
         |
         v
+--------+--------+
|  3. Build marts |  (Materialized views)
+--------+--------+
         |
         v
+--------+--------+
|  4. Validate    |  (Data quality checks)
+--------+--------+
```

**dbt (Data Build Tool):**
A popular tool for ELT transformations:
- SQL-based transformations
- Version controlled
- Automated testing
- Documentation generation

### ELT Challenges

| Challenge | Mitigation |
|-----------|------------|
| Compute costs | Partition, cluster, limit scans |
| Schema evolution | Use flexible JSON, STRUCT types |
| Data quality | Add validation in transform stage |
| Complex logic | Use SQL stored procedures or dbt |
| Real-time | Combine with streaming ingestion |

## Summary

- ELT loads raw data first, then transforms in the data warehouse
- Modern cloud warehouses (BigQuery) make ELT practical
- Benefits: simpler architecture, preserved raw data, flexibility, scalability
- Transform using SQL queries, views, and materialized views
- ELT requires orchestration (Airflow, Cloud Scheduler)
- dbt is popular for managing ELT transformations

## Additional Resources

- [BigQuery ELT Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-patterns)
- [What is ELT? (Fivetran)](https://www.fivetran.com/blog/what-is-elt)
- [dbt Documentation](https://docs.getdbt.com/)
- [ELT Architecture (Databricks)](https://www.databricks.com/glossary/elt)
