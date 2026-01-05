# Data Loading Best Practices

## Learning Objectives

- Apply optimal file sizing strategies for Snowflake loading
- Implement parallel loading techniques for maximum throughput
- Handle errors gracefully in production pipelines
- Optimize loading performance through format and compression choices

## Why This Matters

Efficient data loading directly impacts pipeline reliability, cost, and latency. Poorly designed loading processes can result in slow Bronze layer updates, excessive compute costs, and fragile pipelines that fail under load. Mastering these best practices ensures your data platform scales effectively as data volumes grow.

## The Concept

### File Sizing

File size significantly impacts loading performance. Snowflake processes files in parallel, but there are overhead costs per file.

**Optimal File Sizes:**

| File Size | Impact |
|-----------|--------|
| Under 10 MB | High overhead per file, slow overall |
| 10-100 MB | Acceptable, may not fully utilize parallelism |
| **100-250 MB** | **Optimal for most workloads** |
| Over 250 MB | Reduced parallelism within single file |

**Why 100-250 MB?**
- Large enough to amortize per-file overhead
- Small enough for effective parallel processing
- Balances between too many small files and too few large files

**Recommendations:**
```
- Compress files before upload (GZIP, ZSTD, Snappy)
- Aggregate small files into larger batches if possible
- Split very large files into 100-250 MB chunks
- Consider file splitting in upstream ETL/data producers
```

### Parallel Loading

Snowflake automatically parallelizes loading across multiple files. Maximize throughput by:

**1. Load Multiple Files Simultaneously:**
```sql
-- Load all matching files in parallel
COPY INTO orders
FROM @my_stage/orders/
FILE_FORMAT = (FORMAT_NAME = csv_format)
PATTERN = '.*[.]csv';
```

**2. Size Your Warehouse Appropriately:**

| Warehouse Size | Parallel Threads |
|----------------|------------------|
| X-Small | 1 |
| Small | 2 |
| Medium | 4 |
| Large | 8 |
| X-Large | 16 |

For large batch loads, use a larger warehouse to maximize parallelism.

**3. Avoid Sequential Single-File Loads:**
```sql
-- AVOID: Sequential single-file loads
COPY INTO orders FROM @stage/file1.csv ...;
COPY INTO orders FROM @stage/file2.csv ...;
COPY INTO orders FROM @stage/file3.csv ...;

-- BETTER: Single parallel load
COPY INTO orders 
FROM @stage/
PATTERN = 'file[0-9]+[.]csv';
```

### File Formats and Compression

**Recommended Formats:**

| Format | Compression | Use Case |
|--------|-------------|----------|
| **Parquet** | Built-in | Large datasets, analytics |
| **CSV (GZIP)** | Excellent | Universal compatibility |
| **JSON (GZIP)** | Good | Semi-structured data |
| **ORC** | Built-in | Hive ecosystem integration |

**Parquet Advantages:**
- Columnar storage (efficient for analytics)
- Built-in compression and encoding
- Schema embedded in file
- Predicate pushdown support

**Compression Recommendations:**
```sql
-- Specify compression in file format
CREATE FILE FORMAT compressed_csv
    TYPE = 'CSV'
    COMPRESSION = 'GZIP'  -- or AUTO for detection
    SKIP_HEADER = 1;

-- AUTO compression detects: GZIP, BZ2, BROTLI, ZSTD, DEFLATE, RAW_DEFLATE
```

**Compression Comparison:**

| Compression | Speed | Ratio | CPU |
|-------------|-------|-------|-----|
| GZIP | Medium | High | Medium |
| ZSTD | Fast | High | Low |
| Snappy | Very Fast | Medium | Very Low |

For most cases, GZIP provides the best balance.

### Error Handling

Production pipelines must handle errors gracefully without blocking entire loads.

**ON_ERROR Options:**

```sql
-- ABORT_STATEMENT (default): Stop on first error
COPY INTO orders FROM @stage
    ON_ERROR = 'ABORT_STATEMENT';

-- CONTINUE: Load valid rows, skip errors, log issues
COPY INTO orders FROM @stage
    ON_ERROR = 'CONTINUE';

-- SKIP_FILE: Skip entire file if any error
COPY INTO orders FROM @stage
    ON_ERROR = 'SKIP_FILE';

-- SKIP_FILE_<n>: Skip file after n errors
COPY INTO orders FROM @stage
    ON_ERROR = 'SKIP_FILE_3';  -- Skip after 3 errors in file

-- SKIP_FILE_<n>%: Skip file after n% errors
COPY INTO orders FROM @stage
    ON_ERROR = 'SKIP_FILE_5%';  -- Skip if 5% of rows fail
```

**Production Pattern:**
```sql
-- Log errors but continue loading
COPY INTO orders FROM @stage
    FILE_FORMAT = (FORMAT_NAME = csv_format)
    ON_ERROR = 'CONTINUE'
    VALIDATION_MODE = 'RETURN_ERRORS';  -- First pass: validate

-- If validation passes
COPY INTO orders FROM @stage
    FILE_FORMAT = (FORMAT_NAME = csv_format)
    ON_ERROR = 'SKIP_FILE_1%';  -- Allow up to 1% error rate
```

**Accessing Error Details:**
```sql
-- Query error records
SELECT *
FROM TABLE(VALIDATE(orders, JOB_ID => '_last'));

-- View copy history with errors
SELECT *
FROM TABLE(information_schema.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
WHERE ERROR_COUNT > 0;
```

### Deduplication Strategies

Source files may contain duplicates. Handle them during or after loading.

**Option 1: Dedupe After Load:**
```sql
-- Load to staging
COPY INTO staging_orders FROM @stage ...;

-- Merge with deduplication
MERGE INTO orders t
USING (
    SELECT DISTINCT order_id, customer_id, order_date, amount
    FROM staging_orders
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

**Option 2: Load with Row Number:**
```sql
COPY INTO orders (order_id, customer_id, order_date, amount, row_num)
FROM (
    SELECT 
        $1, $2, $3::DATE, $4::DECIMAL,
        METADATA$FILE_ROW_NUMBER
    FROM @stage
)
FILE_FORMAT = (TYPE = CSV);
```

### Incremental Loading

Track what has been loaded to avoid duplicates and enable restartability.

**File Tracking (Automatic):**
Snowflake tracks loaded files for 64 days. By default, files are not reloaded.

```sql
-- Force reload of previously loaded files
COPY INTO orders FROM @stage
    FORCE = TRUE;  -- WARNING: May cause duplicates
```

**Custom Tracking Table:**
```sql
CREATE TABLE load_tracking (
    file_name STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    row_count INTEGER,
    status STRING
);

-- Record successful loads
INSERT INTO load_tracking (file_name, row_count, status)
SELECT 'orders_2024_01_15.csv', 50000, 'SUCCESS';
```

### Staging Tables Pattern

Use staging tables for validation and transformation before final insert.

```sql
-- 1. Load to transient staging table (low cost)
CREATE TRANSIENT TABLE stg_orders (raw_data VARIANT);

COPY INTO stg_orders FROM @stage
    FILE_FORMAT = (FORMAT_NAME = json_format);

-- 2. Transform and validate
CREATE TEMPORARY TABLE validated_orders AS
SELECT 
    raw_data:order_id::STRING AS order_id,
    raw_data:amount::DECIMAL(12,2) AS amount
FROM stg_orders
WHERE raw_data:order_id IS NOT NULL;

-- 3. Merge to production
MERGE INTO orders t
USING validated_orders s
ON t.order_id = s.order_id
WHEN NOT MATCHED THEN INSERT (order_id, amount) VALUES (s.order_id, s.amount);

-- 4. Cleanup
DROP TABLE stg_orders;
```

### Performance Optimization Checklist

| Area | Best Practice |
|------|---------------|
| **File Size** | Target 100-250 MB compressed |
| **File Count** | Many files enable parallelism |
| **Compression** | Use GZIP or ZSTD |
| **Format** | Prefer Parquet for analytics |
| **Warehouse** | Size appropriately for batch size |
| **Errors** | Use SKIP_FILE with thresholds |
| **Validation** | Validate before production load |
| **Staging** | Use transient tables for staging |

## Summary

- **Optimal file size** is 100-250 MB compressed for maximum throughput
- **Parallel loading** across multiple files leverages warehouse capacity
- Use **GZIP or Parquet** for efficient transfer and storage
- Implement **robust error handling** with ON_ERROR options
- Apply **staging patterns** for validation before production insert
- Track loads to enable **incremental, restartable pipelines**

## Additional Resources

- [Snowflake Documentation: Data Loading Best Practices](https://docs.snowflake.com/en/user-guide/data-load-considerations)
- [Snowflake Documentation: COPY INTO Options](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
- [Snowflake Blog: Optimizing Data Loading](https://www.snowflake.com/blog/optimizing-data-load-in-snowflake/)
