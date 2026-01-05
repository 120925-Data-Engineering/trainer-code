# Introduction to dbt

## Learning Objectives

- Understand what dbt is and its role in the modern data stack
- Recognize the analytics engineering workflow that dbt enables
- Identify where dbt fits alongside extraction, loading, and orchestration tools
- Prepare for hands-on dbt project work later today

## Why This Matters

As you transition from loading data into Snowflake to transforming it for analytics, dbt (data build tool) becomes an essential part of your toolkit. dbt brings software engineering best practices to data transformation, enabling version control, testing, documentation, and modular design. In your journey *From Data Lakes to Data Warehouses*, dbt is the tool that transforms your Silver layer into polished Gold layer analytics.

## The Concept

### What is dbt?

**dbt (data build tool)** is an open-source command-line tool that enables data analysts and engineers to transform data in their warehouse using SQL. dbt handles the "T" in ELT (Extract, Load, Transform).

**Key Characteristics:**
- Transforms data using SQL SELECT statements
- Manages dependencies between transformations
- Enables testing and documentation
- Integrates with version control (Git)
- Supports multiple data warehouses (Snowflake, BigQuery, Redshift, Databricks)

### ELT vs. ETL

Traditional ETL (Extract, Transform, Load) transforms data before loading it into the warehouse. Modern ELT (Extract, Load, Transform) loads raw data first, then transforms it inside the warehouse.

```
Traditional ETL:
Source -> [Transform outside warehouse] -> Load -> Warehouse

Modern ELT:
Source -> Extract -> Load -> Warehouse -> [Transform inside with dbt]
```

**Why ELT?**
- Leverage warehouse compute power for transformations
- Preserve raw data for auditability (Bronze layer)
- Simplify extraction and loading pipelines
- Enable iterative transformation development

### The Analytics Engineering Workflow

dbt introduced the concept of **analytics engineering**, bridging data engineering and data analysis.

**Traditional Roles:**
| Role | Focus |
|------|-------|
| Data Engineer | Pipelines, infrastructure, data loading |
| Data Analyst | Reports, dashboards, ad-hoc queries |

**Analytics Engineer (Bridge Role):**
- Writes production-quality SQL transformations
- Applies software engineering practices to analytics
- Builds and maintains the transformation layer
- Creates tested, documented, reusable data models

### How dbt Works

dbt operates on a simple principle: **write SELECT statements, and dbt handles the rest**.

**1. You Write a Model (SQL SELECT):**
```sql
-- models/staging/stg_orders.sql
SELECT
    order_id,
    customer_id,
    order_date,
    order_total
FROM {{ source('raw', 'orders') }}
WHERE order_id IS NOT NULL
```

**2. dbt Compiles and Materializes:**
```sql
-- dbt generates and runs:
CREATE TABLE analytics.stg_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_total
    FROM raw.orders
    WHERE order_id IS NOT NULL
);
```

**3. dbt Manages Dependencies:**
If another model references `stg_orders`, dbt ensures it runs first.

### Core dbt Concepts

**Models:**
SQL files that define transformations. Each model is a SELECT statement that dbt materializes as a table or view.

**Sources:**
Declarations of raw data tables that exist in your warehouse (your Bronze layer).

**ref() and source():**
Functions that create dependencies between models and sources.

```sql
-- Reference a source (raw table)
SELECT * FROM {{ source('raw', 'orders') }}

-- Reference another model
SELECT * FROM {{ ref('stg_orders') }}
```

**Materializations:**
How dbt creates objects in the warehouse.

| Materialization | Description |
|-----------------|-------------|
| `view` | Creates a SQL view |
| `table` | Creates a table (full refresh) |
| `incremental` | Appends/merges new data |
| `ephemeral` | CTE, not materialized |

**Tests:**
Assertions about your data (unique, not_null, accepted_values, relationships).

**Documentation:**
Descriptions of models, columns, and sources that generate a documentation site.

### dbt in the Modern Data Stack

dbt integrates with the broader data ecosystem:

```
[Extraction]      [Loading]        [Transformation]    [BI/Analytics]
Fivetran     ->   Snowflake   ->   dbt            ->   Tableau
Airbyte           BigQuery         (models, tests)     Looker
Stitch            Redshift                             Power BI
```

**Orchestration (from Week 4):**
Airflow, Dagster, or Prefect schedules and monitors dbt runs.

```
Airflow DAG:
  extract_task >> load_task >> dbt_run_task >> dbt_test_task >> notify_task
```

### dbt Core vs. dbt Cloud

| Feature | dbt Core | dbt Cloud |
|---------|----------|-----------|
| **Deployment** | Self-hosted CLI | Managed SaaS |
| **Cost** | Free, open-source | Paid subscription |
| **Scheduling** | External (Airflow) | Built-in scheduler |
| **IDE** | Local editor | Web-based IDE |
| **Best For** | Existing infra, custom workflows | Quick start, managed service |

For this training, we focus on **dbt Core** with Snowflake.

### Why dbt Matters

**1. Version Control:**
SQL transformations live in Git, enabling code review, branching, and history.

**2. Modularity:**
Models reference other models, creating reusable building blocks.

**3. Testing:**
Built-in and custom tests catch data quality issues before they reach dashboards.

**4. Documentation:**
Auto-generated docs with lineage graphs help teams understand data flow.

**5. Environment Management:**
Develop in a personal schema, test in staging, deploy to production.

### dbt and the Medallion Architecture

dbt aligns naturally with the Medallion architecture you learned earlier:

| Layer | dbt Concept | Description |
|-------|-------------|-------------|
| **Bronze** | Sources | Raw data declared as `source()` |
| **Silver** | Staging Models | Cleansed, typed data using `stg_` prefix |
| **Gold** | Mart Models | Aggregated, business-ready using domain prefixes |

```
sources (Bronze)
    |
    v
staging models (Silver)
    |
    v
intermediate models (optional)
    |
    v
mart models (Gold)
```

### Getting Started

To work with dbt, you will need:

1. **Python** (for dbt Core installation)
2. **dbt-snowflake adapter** (pip install dbt-snowflake)
3. **Snowflake account** (with credentials)
4. **A dbt project** (initialized with `dbt init`)

You will set up your first dbt project in the hands-on exercises later today.

## Summary

- **dbt** is the transformation layer in modern ELT pipelines
- It uses **SQL SELECT statements** to define transformations
- **Models** are materialized as tables or views in your warehouse
- **ref()** and **source()** create dependency graphs
- dbt brings **version control, testing, and documentation** to SQL transformations
- dbt aligns with the **Medallion architecture** (sources -> staging -> marts)
- You will explore dbt project structure and modeling in the next readings

## Additional Resources

- [dbt Documentation: What is dbt?](https://docs.getdbt.com/docs/introduction)
- [dbt Learn: Free Courses](https://courses.getdbt.com/)
- [The Analytics Engineering Guide](https://www.getdbt.com/analytics-engineering/)
