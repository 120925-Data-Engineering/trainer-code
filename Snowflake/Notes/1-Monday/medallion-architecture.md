# Medallion Architecture

## Learning Objectives

- Understand the Medallion architecture pattern (Bronze, Silver, Gold layers)
- Recognize how data quality progresses through each layer
- Apply the Medallion pattern to real-world data pipeline scenarios
- Connect this architecture to your upcoming work in Snowflake

## Why This Matters

As part of your journey *From Data Lakes to Data Warehouses*, the Medallion architecture provides a structured approach to organizing data transformations. Rather than an unorganized data swamp, this pattern creates a clear, maintainable progression from raw ingestion to business-ready analytics.

This architecture has become a standard pattern in modern data platforms, particularly in lakehouse implementations. Understanding it will help you design pipelines that are debuggable, auditable, and scalable. When you work with dbt later this week, you will see how this layered approach maps directly to dbt's model organization.

## The Concept

The **Medallion architecture** organizes data into three distinct layers, each with a specific purpose and quality level. Data flows from raw ingestion through progressive refinement, much like refining raw ore into polished metal.

### Bronze Layer (Raw)

The **Bronze layer** is the landing zone for raw data. Data arrives here exactly as it was received from source systems, with minimal or no transformation.

**Characteristics:**
- **Raw and Unprocessed**: Data is stored in its original format
- **Append-Only**: New data is appended, never overwritten or deleted
- **Full Fidelity**: Preserves all source data, including errors and duplicates
- **Auditable**: Serves as the single source of truth for what was received
- **Schema Flexibility**: May use flexible schemas or semi-structured formats

**Purpose:**
- Enable reprocessing if downstream logic changes
- Support debugging and data lineage tracing
- Provide a historical record of all ingested data

**Example:**
```sql
-- Bronze table: raw JSON events as received
CREATE TABLE bronze.raw_orders (
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT  -- Stores raw JSON as-is
);
```

### Silver Layer (Cleansed)

The **Silver layer** contains cleansed, validated, and conformed data. This is where you apply data quality rules, deduplication, and standardization.

**Characteristics:**
- **Cleaned Data**: Nulls handled, data types enforced, formats standardized
- **Deduplicated**: Duplicate records removed or flagged
- **Validated**: Business rules applied, invalid records quarantined
- **Conformed**: Consistent naming conventions and time zones
- **Relational Structure**: Often normalized or semi-normalized tables

**Purpose:**
- Provide a reliable foundation for diverse analytical use cases
- Enable self-service analytics with trusted data
- Support multiple downstream consumers (Gold tables, ML models, ad-hoc queries)

**Example:**
```sql
-- Silver table: cleansed and typed order data
CREATE TABLE silver.orders (
    order_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    order_date DATE NOT NULL,
    order_total DECIMAL(12, 2),
    currency_code STRING,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (order_id)
);
```

### Gold Layer (Business-Ready)

The **Gold layer** contains aggregated, business-ready data optimized for specific use cases. These are the tables that power dashboards, reports, and business applications.

**Characteristics:**
- **Aggregated**: Pre-computed metrics, summaries, and KPIs
- **Denormalized**: Optimized for query performance, not storage efficiency
- **Business-Aligned**: Tables organized by business domain or use case
- **Consumption-Ready**: Directly queryable by BI tools and end users
- **SLA-Driven**: Often subject to refresh schedules and quality SLAs

**Purpose:**
- Deliver fast, reliable data to business users
- Reduce query complexity for common analytical patterns
- Enforce consistent metric definitions across the organization

**Example:**
```sql
-- Gold table: daily order metrics for dashboards
CREATE TABLE gold.daily_order_metrics (
    order_date DATE NOT NULL,
    total_orders INTEGER,
    total_revenue DECIMAL(14, 2),
    average_order_value DECIMAL(10, 2),
    unique_customers INTEGER,
    refreshed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (order_date)
);
```

## Data Quality Progression

| Layer | Data Quality | Schema | Consumers |
|-------|-------------|--------|-----------|
| **Bronze** | Raw, unvalidated | Flexible/schema-on-read | Data Engineers, Debugging |
| **Silver** | Cleansed, validated | Defined, enforced | Analysts, Data Scientists |
| **Gold** | Aggregated, trusted | Denormalized, optimized | Business Users, Dashboards |

## Real-World Implementation Patterns

### Pattern 1: Incremental Processing

Each layer processes only new or changed data, reducing compute costs and latency.

```
Bronze (append-only) -> Silver (merge/upsert) -> Gold (incremental aggregation)
```

### Pattern 2: Quarantine and Reprocess

Invalid records are quarantined at the Silver layer instead of blocking the pipeline.

```
Bronze -> Silver (valid) + Silver_Quarantine (invalid)
                      -> Gold
```

### Pattern 3: Multiple Gold Tables

A single Silver source feeds multiple Gold tables for different business domains.

```
Silver.orders -> Gold.sales_metrics
             -> Gold.customer_360
             -> Gold.finance_reconciliation
```

## Naming Conventions

Consistent naming helps teams navigate the architecture:

| Convention | Example |
|------------|---------|
| **Database per layer** | `BRONZE_DB`, `SILVER_DB`, `GOLD_DB` |
| **Schema per layer** | `RAW`, `CLEANSED`, `ANALYTICS` |
| **Prefix per layer** | `bronze_orders`, `silver_orders`, `gold_order_metrics` |

In Snowflake, you will often see a combination: databases for major boundaries and schemas for domain organization within each layer.

## Summary

- The **Medallion architecture** organizes data into Bronze (raw), Silver (cleansed), and Gold (business-ready) layers
- Data quality and structure increase as data moves through each layer
- **Bronze** preserves raw data for auditability and reprocessing
- **Silver** applies cleaning, validation, and standardization
- **Gold** delivers aggregated, performance-optimized data for business consumption
- This pattern enables maintainable, debuggable, and scalable data pipelines

## Additional Resources

- [Databricks: Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Microsoft: Medallion Lakehouse Architecture](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)
- [Snowflake: Building a Lakehouse with Snowflake](https://www.snowflake.com/guides/what-data-lakehouse)
