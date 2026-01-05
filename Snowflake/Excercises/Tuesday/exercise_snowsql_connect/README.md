# Exercise: SnowSQL Connection and Queries

## Overview
**Day:** 2-Tuesday  
**Duration:** 2-3 hours  
**Mode:** Individual (Code Lab)  
**Prerequisites:** Monday exercises completed, personal database exists

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Install and configure SnowSQL CLI
2. Connect to Snowflake from the command line
3. Execute queries and manage output formats
4. Use SnowSQL meta-commands effectively

---

## The Scenario
Your team needs to automate Snowflake operations using scripts. The first step is mastering the SnowSQL command-line interface, which enables batch processing and CI/CD integration.

---

## Core Tasks

### Task 1: SnowSQL Installation (30 mins)

1. Download SnowSQL from the Snowflake downloads page
2. Install following the instructions for your operating system
3. Verify installation:
```bash
snowsql --version
```

**Alternative:** If installation is blocked, use Snowsight worksheets. Document the limitation.

---

### Task 2: Connection Configuration (30 mins)

1. Create or edit your SnowSQL config file:
   - **Windows:** `%USERPROFILE%\.snowsql\config`
   - **Mac/Linux:** `~/.snowsql/config`

2. Add a connection profile:
```ini
[connections.training]
accountname = <your_account>
username = <your_username>
password = <your_password>
dbname = <YOUR_NAME>_DEV_DB
schemaname = BRONZE
warehousename = COMPUTE_WH
rolename = ACCOUNTADMIN
```

3. Test the connection:
```bash
snowsql -c training
```

4. Verify context:
```sql
SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA();
```

**Security Note:** In production, use SSO or key-pair authentication instead of passwords in config files.

---

### Task 3: Query Execution (45 mins)

Execute the following queries and save the output:

1. **Basic Context Query:**
```sql
SELECT 
    CURRENT_WAREHOUSE() AS warehouse,
    CURRENT_DATABASE() AS database,
    CURRENT_SCHEMA() AS schema,
    CURRENT_ROLE() AS role;
```

2. **Sample Data Exploration:**
```sql
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;

-- Customer distribution by market segment
SELECT 
    C_MKTSEGMENT,
    COUNT(*) AS customer_count,
    ROUND(AVG(C_ACCTBAL), 2) AS avg_balance
FROM CUSTOMER
GROUP BY C_MKTSEGMENT
ORDER BY customer_count DESC;
```

3. **Multi-Table Join:**
```sql
-- Top 10 customers by order volume
SELECT 
    c.C_NAME AS customer_name,
    n.N_NAME AS nation,
    COUNT(o.O_ORDERKEY) AS order_count,
    SUM(o.O_TOTALPRICE) AS total_spent
FROM CUSTOMER c
JOIN ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY
GROUP BY c.C_NAME, n.N_NAME
ORDER BY total_spent DESC
LIMIT 10;
```

4. **Write your own query** that answers: "What is the average order value by year?"

---

### Task 4: SnowSQL Meta-Commands (30 mins)

Practice these meta-commands:

```bash
# In SnowSQL session:

# Change output format to CSV
!set output_format=csv

# Run a query
SELECT * FROM CUSTOMER LIMIT 5;

# Change output format to JSON
!set output_format=json

# Show current settings
!set

# Run a SQL file
!source starter_code/sample_queries.sql

# Save output to file
!spool /tmp/output.csv
SELECT * FROM NATION;
!spool off

# Exit
!exit
```

Document which commands you found most useful and why.

---

### Task 5: Query History Analysis (30 mins)

Analyze your query history:

```sql
SELECT 
    QUERY_TEXT,
    EXECUTION_STATUS,
    TOTAL_ELAPSED_TIME/1000 AS seconds,
    BYTES_SCANNED/1000000 AS mb_scanned,
    ROWS_PRODUCED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TYPE = 'SELECT'
ORDER BY START_TIME DESC
LIMIT 10;
```

Answer these questions:
1. Which query took the longest?
2. Which query scanned the most data?
3. What optimization might reduce scan time?

---

## Deliverables

1. **Screenshot:** SnowSQL version output from terminal
2. **Config File:** Your sanitized connection profile (password removed)
3. **SQL File:** `my_queries.sql` containing all queries you executed
4. **Query Analysis:** Answers to the Task 5 questions

---

## Definition of Done

- [ ] SnowSQL installed and version verified
- [ ] Connection profile configured
- [ ] Successfully connected from command line
- [ ] All Task 3 queries executed
- [ ] Meta-commands practiced
- [ ] Query history analyzed

---

## Starter Code

The `starter_code/` directory contains:
- `sample_queries.sql` - Example queries to source
- `config_template.ini` - Template for SnowSQL config

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Check account name format (abc12345.us-east-1) |
| Authentication failed | Verify username/password, check role exists |
| Warehouse not found | Ensure COMPUTE_WH exists and you have access |
| Command not found | Add SnowSQL to your PATH |
