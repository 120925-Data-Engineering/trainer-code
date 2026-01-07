# ETL Overview

## Learning Objectives

- Understand what ETL is and its role in data pipelines
- Learn the three stages: Extract, Transform, Load
- Know common ETL patterns and tools
- Recognize ETL architecture in traditional data warehouses

## Why This Matters

ETL (Extract, Transform, Load) is the backbone of data integration. It moves data from source systems into data warehouses, applying transformations along the way. Understanding ETL is essential for building data pipelines and maintaining data quality.

## The Concept

### What is ETL?

ETL is a three-stage process for moving data from source systems to a target data warehouse:

```
+----------+     +-----------+     +--------+     +---------------+
| Source   | --> | Extract   | --> |Transform| --> | Load          |
| Systems  |     +-----------+     +--------+     | (Warehouse)   |
+----------+                                       +---------------+
```

### The Three Stages

**Extract:**
Pull data from source systems.

**Transform:**
Clean, validate, and restructure data for the target.

**Load:**
Insert data into the target data warehouse.

### Stage 1: Extract

Extract retrieves data from various source systems.

**Source Types:**
| Source | Examples |
|--------|----------|
| Databases | PostgreSQL, MySQL, Oracle |
| Files | CSV, JSON, XML, Excel |
| APIs | REST, SOAP, GraphQL |
| Applications | ERP, CRM, SaaS platforms |
| Streaming | Kafka, Pub/Sub, Kinesis |

**Extraction Methods:**
```
Full Extraction:
+------------+                    +------------+
| All Data   | -----------------> | Staging    |
+------------+                    +------------+
Every row, every time (simple but slow)

Incremental Extraction:
+------------+                    +------------+
| New/Changed| -----------------> | Staging    |
| Data Only  |                    +------------+
+------------+
Only changes since last extract (faster, more complex)

CDC (Change Data Capture):
+------------+     +--------+     +------------+
| Database   | --> | Change | --> | Staging    |
| Logs       |     | Events |     +------------+
+------------+     +--------+
Real-time capture of database changes
```

**Extract Considerations:**
- Source system impact (don't overload production)
- Network bandwidth
- Incremental vs. full load trade-offs
- Data format compatibility

### Stage 2: Transform

Transform applies business logic to prepare data for the target.

**Common Transformations:**

| Transformation | Description | Example |
|----------------|-------------|---------|
| Cleansing | Fix data quality issues | Remove duplicates, fix NULLs |
| Standardization | Consistent formats | Date formats, case normalization |
| Validation | Ensure data integrity | Check referential integrity |
| Filtering | Remove unwanted data | Exclude test records |
| Aggregation | Summarize data | Daily totals from transactions |
| Enrichment | Add derived values | Calculate age from birth date |
| Deduplication | Remove duplicates | Single customer record |
| Mapping | Convert codes/values | Map status codes to labels |
| Joining | Combine data sources | Merge customer + address |
| Splitting | Separate fields | Full name to first/last |

**Transformation Example:**
```sql
-- Raw source data
| id | name      | phone        | created      |
|----|-----------|--------------|--------------|
| 1  | ALICE     | 555-123-4567 | 6/15/2024    |
| 2  | bob smith | (555)9876543 | 2024-06-15   |

-- After transformation
| id | first_name | last_name | phone      | created_date |
|----|------------|-----------|------------|--------------|
| 1  | Alice      | NULL      | 5551234567 | 2024-06-15   |
| 2  | Bob        | Smith     | 5559876543 | 2024-06-15   |
```

**Transformation Location:**

Traditional ETL transforms data **before** loading:

```
Source --> Extract --> Transform (ETL Server) --> Load --> Warehouse
```

This requires dedicated ETL infrastructure.

### Stage 3: Load

Load inserts transformed data into the target data warehouse.

**Loading Strategies:**

| Strategy | Description | Use Case |
|----------|-------------|----------|
| Full Load | Truncate and reload all | Small dimensions, corrections |
| Incremental | Append new/changed only | Large fact tables |
| Upsert | Insert or update (MERGE) | Slowly changing dimensions |
| Bulk Load | High-volume batch insert | Initial loads, migrations |

**Loading Example:**
```sql
-- Full load (truncate and insert)
TRUNCATE TABLE dim_product;
INSERT INTO dim_product SELECT * FROM staging_product;

-- Incremental load (append new)
INSERT INTO fact_sales
SELECT * FROM staging_sales
WHERE sale_date > (SELECT MAX(sale_date) FROM fact_sales);

-- Upsert (merge)
MERGE INTO dim_customer AS target
USING staging_customer AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

### ETL Architecture

Traditional ETL architecture:

```
+-------------+     +-------------+     +-------------+     +-------------+
| Source      |     | ETL Server  |     | Staging     |     | Data        |
| Systems     | --> | (Transform) | --> | Area        | --> | Warehouse   |
+-------------+     +-------------+     +-------------+     +-------------+
                          |
                    +-----+-----+
                    | ETL Tool  |
                    | (Informatica,
                    |  SSIS,
                    |  Talend)
                    +-----------+
```

**ETL Pipeline Example:**
```
1. Source: PostgreSQL orders table
2. Extract: Connect via JDBC, query orders WHERE order_date = yesterday
3. Transform:
   - Clean NULL values
   - Convert date formats
   - Lookup customer_key from dim_customer
   - Calculate derived metrics
4. Load: Insert into fact_sales in data warehouse
```

### Common ETL Tools

| Tool | Type | Notes |
|------|------|-------|
| Informatica | Commercial | Enterprise standard |
| SSIS | Commercial | Microsoft ecosystem |
| Talend | Open Source/Commercial | Java-based |
| Pentaho | Open Source | Apache project |
| AWS Glue | Cloud Native | Serverless on AWS |
| Azure Data Factory | Cloud Native | Azure ecosystem |
| GCP Dataflow | Cloud Native | Beam-based, GCP |
| Apache Airflow | Open Source | Orchestration-focused |

### ETL Best Practices

**1. Idempotent Processing**
Running the same job multiple times produces the same result.

**2. Error Handling**
Log failures, retry transient errors, alert on critical failures.

**3. Metadata Tracking**
Track row counts, timestamps, source/target details.

```sql
-- ETL audit table
CREATE TABLE etl_audit (
    job_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rows_extracted INT64,
    rows_loaded INT64,
    status STRING
);
```

**4. Incremental Processing**
Avoid full loads when incremental is possible.

**5. Testing**
Validate data at each stage.

**6. Scheduling**
Run during off-peak hours, manage dependencies.

### ETL Challenges

| Challenge | Description |
|-----------|-------------|
| Performance | Transforming large datasets takes time |
| Complexity | Many sources, many transformations |
| Latency | Data not available until ETL completes |
| Scalability | Traditional ETL servers have limits |
| Maintenance | Schema changes break pipelines |

These challenges led to the rise of ELT (covered next).

## Summary

- ETL stands for Extract, Transform, Load
- **Extract**: Pull data from source systems
- **Transform**: Clean, validate, and restructure data
- **Load**: Insert into the target warehouse
- Traditional ETL transforms data on dedicated servers
- Common patterns: full load, incremental, upsert
- Challenges include performance, latency, and maintainability

## Additional Resources

- [What is ETL? (IBM)](https://www.ibm.com/topics/etl)
- [ETL Best Practices](https://www.guru99.com/etl-extract-load-process.html)
- [ETL vs ELT (AWS)](https://aws.amazon.com/compare/the-difference-between-etl-and-elt/)
