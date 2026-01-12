# Exercise: Data Import and Connectivity Modes

## Overview
**Day:** 1-Monday
**Duration:** 2-3 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Power BI Desktop installed, Snowflake account from Week 5

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Connecting to Data Sources | [connecting-to-data-sources.md](../../content/1-Monday/connecting-to-data-sources.md) | Snowflake connector, authentication |
| Importing Data | [importing-data.md](../../content/1-Monday/importing-data.md) | Import vs DirectQuery modes |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Connect Power BI to CSV and Excel files
2. Connect Power BI to your Week 5 Snowflake database
3. Understand the difference between Import and DirectQuery modes
4. Choose the appropriate connectivity mode for different scenarios

---

## The Scenario
Your data engineering team has prepared data in multiple formats: local CSV files for ad-hoc analysis and a Snowflake data warehouse for production reporting. You need to demonstrate proficiency in connecting Power BI to both source types.

---

## Core Tasks

### Task 1: Connect to CSV File (30 mins)

1. Open Power BI Desktop
2. Click **Home** > **Get Data** > **Text/CSV**
3. Navigate to `starter_code/sample_orders.csv`
4. Preview the data and verify:
   - Column names are detected correctly
   - Data types are appropriate
5. Click **Load** (Import mode)

**Verify in Data View:**
- Row count matches expected
- Column data types are correct

**Checkpoint:** Screenshot of loaded CSV data in Data View.

---

### Task 2: Connect to Excel File (30 mins)

1. Click **Home** > **Get Data** > **Excel Workbook**
2. Navigate to `starter_code/sample_customers.xlsx`
3. In the Navigator:
   - Check the relevant sheet(s)
   - Preview the data
4. Click **Load**

5. Switch to Model View and note:
   - Both tables now visible
   - Any auto-detected relationships?

**Checkpoint:** Screenshot showing both tables in Model View.

---

### Task 3: Connect to Snowflake GOLD Zone (45 mins)

This connects to the Week 5 star schema you built!

1. Click **Home** > **Get Data** > **More...**
2. Search for "Snowflake" and select it
3. Enter connection details:
   ```
   Server: <your_account>.snowflakecomputing.com
   Warehouse: COMPUTE_WH
   ```
4. Choose **Import** mode (we'll explore DirectQuery later)
5. Click **OK** and authenticate with your Snowflake credentials

6. In the Navigator, expand:
   - DEV_DB > GOLD schema
   - Select these tables:
     - DIM_DATE
     - DIM_CUSTOMER
     - DIM_PRODUCT
     - FCT_ORDER_LINES

7. Click **Load** and wait for import

**Checkpoint:** Screenshot showing all four GOLD zone tables in Model View.

---

### Task 4: Explore DirectQuery Mode (30 mins)

1. Add a new Snowflake connection (separate from Import)
2. This time, select **DirectQuery** mode
3. Load just DIM_DATE table

4. **Compare behaviors:**

| Aspect | Import (FCT_ORDER_LINES) | DirectQuery (DIM_DATE) |
|--------|--------------------------|------------------------|
| File size impact | | |
| Query speed | | |
| Data freshness | | |

5. Create a simple visual using each table and note performance differences

**Checkpoint:** Document the differences you observed.

---

### Task 5: Decision Analysis (30 mins)

For each scenario below, determine which mode you would recommend:

| Scenario | Recommended Mode | Justification |
|----------|------------------|---------------|
| Dashboard refreshed weekly, 50K rows | | |
| Real-time operational dashboard, 1M rows | | |
| CFO report with 5 years of history, 100M rows | | |
| Ad-hoc analysis on rapidly changing data | | |
| Mobile dashboard with offline access needs | | |

---

## Deliverables

Submit the following:

1. **Power BI File (.pbix):** Containing all loaded tables
2. **Screenshot 1:** CSV and Excel tables in Model View
3. **Screenshot 2:** Snowflake GOLD zone tables loaded
4. **Analysis Document:** Import vs DirectQuery comparison and scenario recommendations

---

## Definition of Done

- [ ] CSV file imported successfully
- [ ] Excel file imported successfully
- [ ] Snowflake GOLD zone tables imported (all 4)
- [ ] DirectQuery connection tested
- [ ] Comparison analysis documented
- [ ] Scenario recommendations provided with justification

---

## Stretch Goals (Optional)

1. Connect to the SILVER layer and compare to GOLD data
2. Use parameters to make the Snowflake connection configurable
3. Practice switching a table between Import and DirectQuery

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Snowflake connection timeout | Check account URL format, verify internet |
| "Driver not found" | Install Snowflake ODBC driver |
| Authentication fails | Verify username/password in Snowflake directly |
| DirectQuery not available | Some connectors don't support DirectQuery |
| Large import takes too long | Filter data during import in Power Query |
