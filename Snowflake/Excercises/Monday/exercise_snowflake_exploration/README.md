# Exercise: Snowflake Exploration

## Overview
**Day:** 1-Monday  
**Duration:** 2-3 hours  
**Mode:** Individual (Hybrid - Exploration + Documentation)  
**Prerequisites:** Snowflake Free Trial account created

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Data Warehouse Fundamentals | [data-warehouse-fundamentals.md](../../content/1-Monday/data-warehouse-fundamentals.md) | Data warehouse vs data lake concepts |
| Snowflake Introduction | [snowflake-introduction.md](../../content/1-Monday/snowflake-introduction.md) | What makes Snowflake unique |
| Snowflake Architecture | [snowflake-architecture.md](../../content/1-Monday/snowflake-architecture.md) | Three-layer architecture, virtual warehouses |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Navigate the Snowsight user interface confidently
2. Create databases, schemas, and tables using SQL commands
3. Identify and explain the three-layer Snowflake architecture
4. Configure cost-saving settings for your warehouse

---

## The Scenario
You have just joined a data engineering team that uses Snowflake as their cloud data warehouse. Your manager has asked you to set up your personal development environment and document your observations about the platform architecture.

---

## Core Tasks

### Task 1: Account Setup and Navigation (30 mins)

1. Log into your Snowflake Free Trial account
2. Explore the Snowsight UI and locate these sections:
   - Data > Databases (object browser)
   - Activity > Query History
   - Admin > Warehouses
   - Admin > Users & Roles
3. Take note of the default objects that exist (SNOWFLAKE_SAMPLE_DATA, etc.)

**Checkpoint:** Answer these questions in your notes:
- What databases exist by default?
- What is the name of the default virtual warehouse?
- What role are you currently using?

---

### Task 2: Create Your Development Environment (45 mins)

Execute the following SQL commands (modify as needed):

```sql
-- Set your context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- CRITICAL: Set cost-saving auto-suspend
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;

-- Create your personal database
CREATE DATABASE IF NOT EXISTS <YOUR_NAME>_DEV_DB
    COMMENT = 'Personal development database';

-- Create schemas for the Medallion architecture
CREATE SCHEMA IF NOT EXISTS <YOUR_NAME>_DEV_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS <YOUR_NAME>_DEV_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS <YOUR_NAME>_DEV_DB.GOLD;

-- Verify creation
SHOW SCHEMAS IN DATABASE <YOUR_NAME>_DEV_DB;
```

**Checkpoint:** Take a screenshot showing your database and three schemas in the object browser.

---

### Task 3: Explore Sample Data (30 mins)

1. Switch to the sample database:
```sql
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;
```

2. Run these exploration queries:
```sql
-- List all tables
SHOW TABLES;

-- Count rows in a large table
SELECT COUNT(*) FROM ORDERS;

-- Basic aggregation
SELECT 
    O_ORDERSTATUS,
    COUNT(*) AS order_count
FROM ORDERS
GROUP BY O_ORDERSTATUS;
```

3. Experiment with at least 3 additional queries of your own design.

**Checkpoint:** Record the execution time for your COUNT(*) query.

---

### Task 4: Architecture Documentation (45 mins)

Based on your exploration and the written content, create a documentation file that includes:

1. **Architecture Diagram**: Draw (hand-drawn is fine) or describe the three layers of Snowflake architecture:
   - Cloud Services Layer
   - Query Processing Layer (Virtual Warehouses)
   - Database Storage Layer

2. **Observations Table**: Complete this table based on your exploration:

| Component | What You Observed | Purpose |
|-----------|-------------------|---------|
| Virtual Warehouse | | |
| Database | | |
| Schema | | |
| Table | | |
| Role | | |

3. **Cost Control Settings**: Document the AUTO_SUSPEND setting and explain why it matters.

---

## Deliverables

Submit the following:

1. **Screenshot 1:** Your database and schemas in the Snowflake object browser
2. **Screenshot 2:** Query History showing your executed queries
3. **Documentation File:** Your architecture observations (markdown or text format)
4. **SQL Script:** All SQL commands you executed, saved as `exploration.sql`

---

## Definition of Done

- [ ] Personal database created with your name prefix
- [ ] Three Medallion schemas created (BRONZE, SILVER, GOLD)
- [ ] AUTO_SUSPEND set to 60 seconds on your warehouse
- [ ] At least 5 queries executed against sample data
- [ ] Architecture diagram completed
- [ ] All deliverables submitted

---

## Stretch Goals (Optional)

1. Create a simple table in your BRONZE schema and insert sample data
2. Compare query performance between X-SMALL and SMALL warehouse sizes
3. Explore the INFORMATION_SCHEMA to find metadata about your objects

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Permission denied | Ensure you are using ACCOUNTADMIN role |
| Warehouse not running | Run: `ALTER WAREHOUSE COMPUTE_WH RESUME;` |
| Database not visible | Refresh the object browser |
| Queries hang | Check that USE WAREHOUSE was executed |
