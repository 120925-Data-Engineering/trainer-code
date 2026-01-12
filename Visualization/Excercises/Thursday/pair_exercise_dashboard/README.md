# Pair Programming Exercise: Complete Analytics Solution

## Overview
**Day:** 4-Thursday
**Duration:** 4-5 hours
**Mode:** Collaborative (Pair Programming)
**Prerequisites:** All Monday-Wednesday exercises completed

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Data Alerts and Dashboards | [data-alerts-dashboard.md](../../content/4-Thursday/data-alerts-dashboard.md) | Dashboard creation, alerts |
| Dataset Refresh | [dataset-refresh.md](../../content/4-Thursday/dataset-refresh.md) | Refresh strategies |
| Power BI Service | [power-bi-service.md](../../content/4-Thursday/power-bi-service.md) | Publishing, sharing, RLS |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Build a complete Power BI solution from data import to dashboard
2. Practice pair programming with clear role definitions
3. Publish to Power BI Service and create a dashboard
4. Configure data alerts for key business thresholds
5. Collaborate effectively on a technical project

---

## Pair Programming Protocol

### Role Definitions

**Driver:**
- Controls the keyboard and mouse
- Writes code and configures settings
- Explains decisions out loud
- Asks navigator for input on key decisions

**Navigator:**
- Reviews everything the driver does
- References documentation and requirements
- Thinks ahead about next steps
- Catches errors and suggests improvements
- Keeps track of time and checkpoints

### Rotation Schedule

| Phase | Duration | Driver Focus |
|-------|----------|--------------|
| 1 | 45 mins | Data import and modeling |
| 2 | 45 mins | DAX measures |
| 3 | 45 mins | Report design |
| 4 | 45 mins | Publishing and dashboard |
| 5 | 30 mins | Final review and documentation |

**CRITICAL: Switch roles after each phase!**

---

## The Scenario

A fictional retail company, "DataMart Inc.", needs a complete analytics solution. You and your partner will build everything from scratch, simulating a real-world engagement.

**Business Requirements from the "client":**
1. Connect to their Snowflake data warehouse
2. Build a model following best practices
3. Create key performance measures
4. Design an executive report
5. Publish to Power BI Service with a dashboard
6. Set up an alert for when revenue drops below threshold

---

## Phase 1: Data Import and Modeling (45 mins)

**Driver Task:** Set up the data foundation

### Step 1.1: Create New Power BI File
1. Open Power BI Desktop
2. Save as `DataMart_Solution_[Partner1]_[Partner2].pbix`

### Step 1.2: Connect to Snowflake
1. **Get Data** > **Snowflake**
2. Connect to DEV_DB.GOLD schema
3. Import:
   - DIM_DATE
   - DIM_CUSTOMER
   - DIM_PRODUCT
   - FCT_ORDER_LINES

### Step 1.3: Configure Relationships
1. Switch to Model View
2. Verify/create relationships:
   - DIM_DATE -> FCT_ORDER_LINES (date_key)
   - DIM_CUSTOMER -> FCT_ORDER_LINES (customer_key)  
   - DIM_PRODUCT -> FCT_ORDER_LINES (product_key)
3. All should be 1:* with single-direction filtering

### Step 1.4: Mark Date Table
1. Select DIM_DATE
2. **Table tools** > **Mark as date table**
3. Select the date column

**Navigator Responsibilities:**
- Verify correct tables selected
- Check relationship configuration
- Document any data issues observed

**Checkpoint:** Model view screenshot showing relationships

**SWITCH ROLES!**

---

## Phase 2: DAX Measures (45 mins)

**Driver Task:** Build the measures library

### Step 2.1: Create Measures Table
1. **Enter Data** > Create `_Measures` table
2. Hide helper column

### Step 2.2: Create Base Measures
```dax
Total Revenue = SUM(FCT_ORDER_LINES[net_amount])
Order Count = DISTINCTCOUNT(FCT_ORDER_LINES[order_key])
Customer Count = DISTINCTCOUNT(FCT_ORDER_LINES[customer_key])
Avg Order Value = DIVIDE([Total Revenue], [Order Count], 0)
```

### Step 2.3: Create Time Intelligence Measures
```dax
Revenue YTD = TOTALYTD([Total Revenue], DIM_DATE[full_date])
Revenue PY = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(DIM_DATE[full_date]))
YoY Growth = DIVIDE([Total Revenue] - [Revenue PY], [Revenue PY], 0)
```

### Step 2.4: Create Comparison Measures
```dax
Total Revenue All = CALCULATE([Total Revenue], ALL(DIM_DATE))
% of Total = DIVIDE([Total Revenue], [Total Revenue All], 0)
```

### Step 2.5: Test Measures
- Create quick matrix to verify calculations
- Delete test matrix after verification

**Navigator Responsibilities:**
- Verify DAX syntax before execution
- Suggest additional useful measures
- Check measure formatting

**Checkpoint:** Measures table with at least 8 measures

**SWITCH ROLES!**

---

## Phase 3: Report Design (45 mins)

**Driver Task:** Create the executive report

### Step 3.1: Page 1 - Executive Overview
Layout:
```
+----------------------------------------+
| DataMart Sales Dashboard    [Slicers] |
+----------------------------------------+
| Revenue | Orders | Customers | Growth  |
+----------------------------------------+
|   Revenue by Year    |  Segment Pie   |
+----------------------------------------+
```

1. Add title text box
2. Add 4 KPI cards
3. Add revenue bar chart
4. Add segment pie chart
5. Add year slicer (tile format)

### Step 3.2: Page 2 - Detailed Analysis
1. Add matrix: Manufacturer x Year with Revenue
2. Add table: Top 10 products by revenue
3. Sync slicers from page 1

### Step 3.3: Apply Formatting
1. Choose a theme
2. Apply consistent colors
3. Add conditional formatting to matrix
4. Ensure proper data labels

**Navigator Responsibilities:**
- Reference design principles
- Suggest layout improvements
- Check visual alignment

**Checkpoint:** 2-page report with consistent styling

**SWITCH ROLES!**

---

## Phase 4: Publishing and Dashboard (45 mins)

**Driver Task:** Deploy to Power BI Service

### Step 4.1: Publish Report
1. Save the file
2. **Home** > **Publish**
3. Sign in to Power BI Service
4. Select workspace (create "Training - [Names]" if needed)
5. Confirm publish success

### Step 4.2: Create Dashboard in Service
1. Open Power BI Service (app.powerbi.com)
2. Navigate to your workspace
3. Open the published report
4. Pin key visuals to a new dashboard:
   - Revenue KPI card
   - Revenue by Year bar chart
   - YoY Growth card
5. Name dashboard "DataMart Executive Dashboard"

### Step 4.3: Configure Data Alert
1. On the dashboard, click the Revenue card
2. **...** > **Manage alerts**
3. Create alert:
   - Condition: Below
   - Threshold: 5,000,000 (or appropriate value)
   - Notification: Email

### Step 4.4: Test Dashboard
1. Verify all tiles display correctly
2. Click a tile to navigate to report
3. Confirm real-time card is updating

**Navigator Responsibilities:**
- Document workspace URL
- Verify dashboard layout
- Check alert configuration

**Checkpoint:** Dashboard live in Power BI Service with alert configured

**SWITCH ROLES (for final review)!**

---

## Phase 5: Review and Documentation (30 mins)

**Both Partners Together:**

### Step 5.1: Solution Review
Walk through the entire solution:
- Data model correct?
- Measures calculating properly?
- Report visually appealing?
- Dashboard functional?

### Step 5.2: Create Documentation
Prepare a brief solution document:
1. Data sources used
2. List of measures created
3. Report pages and purpose
4. Dashboard URL
5. Alert configuration

### Step 5.3: Pair Reflection
Discuss and document:
- What went well in pair programming?
- What was challenging?
- How did you resolve disagreements?
- What would you do differently?

---

## Deliverables

As a pair, submit:

1. **Power BI File (.pbix):** Complete solution
2. **Dashboard Screenshot:** From Power BI Service
3. **Solution Documentation:** Technical summary
4. **Pair Reflection:** 1-page collaboration reflection

---

## Definition of Done

- [ ] Data imported from Snowflake
- [ ] Star schema relationships configured
- [ ] At least 8 DAX measures created
- [ ] 2-page report with consistent styling
- [ ] Report published to Power BI Service
- [ ] Dashboard created with pinned visuals
- [ ] Data alert configured
- [ ] Both partners drove at least twice
- [ ] Documentation completed
- [ ] Pair reflection submitted

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Publish fails | Check Power BI account, workspace permissions |
| Dashboard tiles blank | Verify dataset loaded, refresh if needed |
| Alert not triggering | Check threshold, may need to test with lower value |
| Partner conflict | Take a break, revisit requirement together |
| Time running out | Prioritize publishing over polish |
