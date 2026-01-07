# Snowflake Schema

## Learning Objectives

- Understand the snowflake schema structure
- Know how snowflake differs from star schema
- Recognize the trade-offs between star and snowflake
- Apply snowflake design where appropriate

## Why This Matters

While star schemas are more common, snowflake schemas are useful in certain scenarios. Understanding the snowflake schema helps you make informed design decisions and work with various data warehouse implementations.

## The Concept

### What is a Snowflake Schema?

A snowflake schema is a variation of the star schema where dimension tables are normalized into multiple related tables, creating a snowflake-like shape.

```
Star Schema:                      Snowflake Schema:

          +-------------+                       +-------------+
          | dim_product |                       | dim_brand   |
          +------+------+                       +------+------+
                 |                                     |
                 |                              +------v------+
                 |                              | dim_product |
                 v                              +------+------+
          +------+------+                              |
          | fact_sales  |                       +------v------+
          +------+------+                       | fact_sales  |
                 |                              +------+------+
                 |                                     |
          +------v------+                       +------v------+
          | dim_customer|                       |dim_geography|
          +-------------+                       +------+------+
                                                       |
                                                +------v------+
                                                | dim_customer|
                                                +-------------+
```

### Normalized Dimensions

In a snowflake schema, dimensions are normalized into sub-dimensions:

**Star Schema (Denormalized):**
```sql
dim_product
+-----------+----------+----------+-------------+------------+
| prod_key  | name     | category | category_mgr| brand_name |
+-----------+----------+----------+-------------+------------+
| 1         | Widget A | Tools    | Smith       | BrandX     |
| 2         | Widget B | Tools    | Smith       | BrandX     |
| 3         | Gadget C | Tech     | Jones       | BrandY     |
+-----------+----------+----------+-------------+------------+
Redundancy: Category and brand info repeated for each product
```

**Snowflake Schema (Normalized):**
```sql
dim_product                     dim_category
+-----------+----------+-----+  +-----+----------+-------------+
| prod_key  | name     | cat |  | key | category | category_mgr|
+-----------+----------+-----+  +-----+----------+-------------+
| 1         | Widget A | 1   |  | 1   | Tools    | Smith       |
| 2         | Widget B | 1   |  | 2   | Tech     | Jones       |
| 3         | Gadget C | 2   |  +-----+----------+-------------+
+-----------+----------+-----+
                               dim_brand
                               +-----+-----------+
                               | key | brand_name|
                               +-----+-----------+
                               | 1   | BrandX    |
                               | 2   | BrandY    |
                               +-----+-----------+
No redundancy: Category and brand are separate lookup tables
```

### Snowflake Schema Example

**Fact Table:**
```sql
CREATE TABLE fact_sales (
    date_key INT64,
    customer_key INT64,
    product_key INT64,
    store_key INT64,
    quantity INT64,
    total_amount NUMERIC(10,2)
);
```

**Normalized Product Dimension:**
```sql
-- Main product dimension
CREATE TABLE dim_product (
    product_key INT64,
    product_id STRING,
    name STRING,
    category_key INT64,    -- FK to category
    brand_key INT64        -- FK to brand
);

-- Category sub-dimension
CREATE TABLE dim_category (
    category_key INT64,
    category_name STRING,
    department STRING,
    department_manager STRING
);

-- Brand sub-dimension
CREATE TABLE dim_brand (
    brand_key INT64,
    brand_name STRING,
    manufacturer STRING,
    country_of_origin STRING
);
```

**Normalized Location/Geography:**
```sql
-- Store dimension
CREATE TABLE dim_store (
    store_key INT64,
    store_name STRING,
    city_key INT64         -- FK to city
);

-- City sub-dimension
CREATE TABLE dim_city (
    city_key INT64,
    city_name STRING,
    state_key INT64        -- FK to state
);

-- State sub-dimension
CREATE TABLE dim_state (
    state_key INT64,
    state_name STRING,
    region STRING,
    country STRING
);
```

### Querying Snowflake Schema

Queries require more joins:

**Star Schema Query:**
```sql
SELECT
    p.category,
    SUM(f.total_amount) AS sales
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category;
```

**Snowflake Schema Query:**
```sql
SELECT
    c.category_name,
    SUM(f.total_amount) AS sales
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_category c ON p.category_key = c.category_key
GROUP BY c.category_name;
```

### Star vs. Snowflake Comparison

| Aspect | Star Schema | Snowflake Schema |
|--------|-------------|------------------|
| Dimension structure | Denormalized | Normalized |
| Query complexity | Simpler (fewer joins) | More complex |
| Query performance | Generally faster | Generally slower |
| Storage | More (redundancy) | Less (no redundancy) |
| Maintenance | Updates to many rows | Updates to few rows |
| ETL complexity | Simpler | More complex |
| BI tool compatibility | Better | May require more config |

### When to Use Snowflake Schema

**Use Snowflake When:**
- Storage costs are a significant concern
- Dimension attributes change frequently
- Dimension hierarchies are deep and stable
- Data quality of hierarchies needs enforcement
- Regulatory requirements demand data minimization

**Use Star When:**
- Query performance is priority
- Simplicity is valued
- BI tools are primary consumers
- Storage is cheap (cloud)
- Dimensions are relatively stable

### Hybrid Approach

In practice, many warehouses use a hybrid:
- Keep some dimensions as star (flat)
- Normalize only where it makes sense (deep hierarchies)

```
Hybrid Example:
- fact_sales --> dim_date (star - flat)
- fact_sales --> dim_customer (star - flat)
- fact_sales --> dim_product --> dim_category (snowflake)
- fact_sales --> dim_store --> dim_city --> dim_state (snowflake)
```

### Performance Considerations

**Snowflake Performance Challenges:**

1. **More Joins:**
```sql
-- Each level of normalization adds a join
fact -> product -> category -> department
      -> brand -> manufacturer
```

2. **Query Optimizer Complexity:**
More tables means more optimization paths.

3. **Index/Clustering:**
Need proper indexes on all join keys.

**Mitigation Strategies:**

1. **Materialized Views:**
```sql
-- Pre-join frequently accessed dimensions
CREATE MATERIALIZED VIEW dim_product_full AS
SELECT p.*, c.category_name, b.brand_name
FROM dim_product p
JOIN dim_category c ON p.category_key = c.category_key
JOIN dim_brand b ON p.brand_key = b.brand_key;
```

2. **Aggressive Caching:**
Let BigQuery/Redshift cache common joins.

3. **Denormalize for Performance:**
If snowflake is too slow, flatten the most-used dimensions.

### Modern Perspective

**Traditional View (1990s-2000s):**
Storage was expensive, so snowflake helped reduce costs.

**Modern Cloud View:**
- Storage is cheap
- Compute (joins) costs more than storage
- Star schema is generally preferred
- Use snowflake only where genuinely needed

**BigQuery/Cloud Guidance:**
Star schemas typically perform better in columnar cloud warehouses. Snowflake patterns are less common in modern implementations.

## Summary

- Snowflake schema normalizes dimension tables into sub-tables
- Creates more tables and requires more joins than star schema
- Benefits: reduced storage, easier dimension maintenance, data integrity
- Drawbacks: slower queries, more complex SQL, ETL complexity
- Hybrid approaches use snowflake selectively for deep hierarchies
- Modern cloud warehouses often favor star due to cheap storage
- Choose based on specific requirements (storage vs. performance)

## Additional Resources

- [Star vs Snowflake Schema (Guru99)](https://www.guru99.com/star-snowflake-data-warehousing.html)
- [Kimball on Snowflake](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [When Snowflake Makes Sense](https://www.holistics.io/blog/snowflake-schema-vs-star-schema/)
