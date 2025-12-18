# Bucketing

## Learning Objectives
- Understand what bucketing is and how it differs from partitioning
- Learn when to use bucketing for query optimization
- Master the syntax for creating and using bucketed tables

## Why This Matters

While partitioning organizes data by column values into directories, **bucketing** organizes data by hash values into fixed-size buckets within files. This optimization technique is particularly powerful for:

- Speeding up joins between large tables
- Optimizing aggregations on specific columns
- Reducing shuffle operations during query execution

Bucketing is an advanced optimization that becomes crucial when working with large-scale data, directly supporting our Weekly Epic of *Mastering Spark SQL and DataFrames*.

## The Concept

### Partitioning vs. Bucketing

| Aspect | Partitioning | Bucketing |
|--------|-------------|-----------|
| Organization | Separate directories | Hash-based file distribution |
| Best for | Low-cardinality columns (date, region) | High-cardinality columns (user_id) |
| Number of divisions | Based on distinct values | Fixed number of buckets |
| Query optimization | Partition pruning | Sort-merge join, aggregation |
| Data location | `column=value/` directories | Within files |

### How Bucketing Works

```
Data: user_id, action, timestamp
Bucketing by user_id into 4 buckets:

hash(user_id) % 4 = bucket_number

Bucket 0: users where hash(id) % 4 = 0
Bucket 1: users where hash(id) % 4 = 1
Bucket 2: users where hash(id) % 4 = 2
Bucket 3: users where hash(id) % 4 = 3
```

### Benefits of Bucketing

**1. Optimized Joins**
When two tables are bucketed on the same column with the same number of buckets:
- Matching buckets can be joined locally
- No shuffle required (sort-merge join)

**2. Optimized Aggregations**
When aggregating by the bucket column:
- Each bucket can be aggregated independently
- Reduced shuffle for groupBy operations

**3. Consistent File Sizes**
Unlike partitioning, bucket sizes are more predictable.

### Bucketing Requirements

- Bucketing is only supported for **persistent tables** (not temporary views)
- Tables must be written using `saveAsTable()` or Hive DDL
- Both tables in a join must have the **same number of buckets** to avoid shuffle

## Code Example

### Creating Bucketed Tables

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bucketing") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Sample data
users_data = [
    (1, "Alice", "NY"),
    (2, "Bob", "CA"),
    (3, "Charlie", "TX"),
    (4, "Diana", "NY"),
    (5, "Eve", "CA")
]

users_df = spark.createDataFrame(users_data, ["user_id", "name", "state"])

# Create bucketed table
# bucketBy(numBuckets, column) + sortBy (optional) + saveAsTable
users_df.write \
    .bucketBy(4, "user_id") \
    .sortBy("user_id") \
    .mode("overwrite") \
    .saveAsTable("users_bucketed")

print("Bucketed table created:")
spark.sql("DESCRIBE EXTENDED users_bucketed").show(truncate=False)
```

### Joining Bucketed Tables

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bucketed Joins") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Create two related datasets
users_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Diana")]
orders_data = [
    (101, 1, 100.00), (102, 2, 200.00), 
    (103, 1, 150.00), (104, 3, 75.00)
]

users_df = spark.createDataFrame(users_data, ["user_id", "name"])
orders_df = spark.createDataFrame(orders_data, ["order_id", "user_id", "amount"])

# Bucket both tables on user_id with same number of buckets
users_df.write \
    .bucketBy(4, "user_id") \
    .sortBy("user_id") \
    .mode("overwrite") \
    .saveAsTable("users_bucketed")

orders_df.write \
    .bucketBy(4, "user_id") \
    .sortBy("user_id") \
    .mode("overwrite") \
    .saveAsTable("orders_bucketed")

# Enable bucketed join optimization
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

# Read bucketed tables
users = spark.table("users_bucketed")
orders = spark.table("orders_bucketed")

# This join can use sort-merge join without shuffle
result = users.join(orders, "user_id")
result.show()

# Check the query plan
result.explain()
# Look for "SortMergeJoin" without "Exchange" (shuffle)
```

### Combining Partitioning and Bucketing

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Partition and Bucket") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Sales data with date and customer
sales_data = [
    ("2023-01-15", 1, "Electronics", 500.00),
    ("2023-01-15", 2, "Clothing", 100.00),
    ("2023-01-16", 1, "Electronics", 750.00),
    ("2023-02-01", 3, "Electronics", 300.00)
]

sales_df = spark.createDataFrame(
    sales_data, 
    ["sale_date", "customer_id", "category", "amount"]
)

# Partition by date (low cardinality), bucket by customer_id (high cardinality)
sales_df.write \
    .partitionBy("sale_date") \
    .bucketBy(10, "customer_id") \
    .sortBy("customer_id") \
    .mode("overwrite") \
    .saveAsTable("sales_partitioned_bucketed")

# Query benefits:
# - Partition pruning for date filters
# - Bucket optimization for customer joins/aggregations

spark.sql("""
    SELECT customer_id, SUM(amount) as total
    FROM sales_partitioned_bucketed
    WHERE sale_date = '2023-01-15'
    GROUP BY customer_id
""").show()
```

### Checking Bucketing Information

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bucket Info") \
    .enableHiveSupport() \
    .getOrCreate()

# Describe table to see bucketing info
print("Table Properties:")
spark.sql("DESCRIBE EXTENDED users_bucketed").show(100, truncate=False)

# Check specific table properties
spark.sql("""
    SHOW CREATE TABLE users_bucketed
""").show(truncate=False)

# Verify bucket columns and count
print("\nCatalog Information:")
for table in spark.catalog.listTables():
    if "bucketed" in table.name:
        print(f"Table: {table.name}")
```

### Best Practices

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Bucket Best Practices").getOrCreate()

# Best Practice 1: Choose appropriate bucket count
# Rule of thumb: bucket_count = total_data_size / target_bucket_size
# Target bucket size: 100MB - 200MB

# Example: 10GB of data, target 100MB buckets
# bucket_count = 10000MB / 100MB = 100 buckets

# Best Practice 2: Use power of 2 for bucket counts
# 64, 128, 256, 512 - helps with join optimization

# Best Practice 3: Match bucket counts for joined tables
# Both tables must have same bucket count for shuffle-free join

# Best Practice 4: Include sortBy for sort-merge joins
# Sorting within buckets enables efficient merge joins

# Best Practice 5: Monitor with explain()
df = spark.table("users_bucketed")
df.join(spark.table("orders_bucketed"), "user_id").explain(True)
# Look for:
# - "SortMergeJoin" (good)
# - No "Exchange" before join (good, no shuffle)
# - "BroadcastHashJoin" may override bucketing for small tables
```

## Summary

- **Bucketing** organizes data by hash value into a fixed number of buckets
- Unlike partitioning (directories), bucketing distributes data within files
- Bucketed tables must be persisted with `saveAsTable()`
- **Optimized joins**: Two tables bucketed on the same column with same bucket count can join without shuffle
- **Optimized aggregations**: GroupBy on bucket column reduces shuffle
- Combine partitioning (low-cardinality) with bucketing (high-cardinality) for best results
- Use `explain()` to verify that bucket optimizations are being applied

## Additional Resources

- [Spark SQL Bucketing](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html#bucketed-tables)
- [Bucketing in Apache Spark](https://www.databricks.com/blog/2020/03/27/best-practices-for-bucketing-in-apache-spark-sql.html)
- [Optimizing Spark SQL Joins](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
