# Exercise: Advanced DAX with CALCULATE and Time Intelligence

## Overview
**Day:** 2-Tuesday
**Duration:** 2-3 hours
**Mode:** Individual (Implementation)
**Prerequisites:** DAX measures exercise completed, base measures created

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| CALCULATE Function | [dax-calculate-function.md](../../content/2-Tuesday/dax-calculate-function.md) | Filter modification, context transition |
| DAX Studio Introduction | [dax-studio-introduction.md](../../content/2-Tuesday/dax-studio-introduction.md) | Debugging and performance |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Use CALCULATE to modify filter context
2. Create time intelligence measures (YTD, Prior Year)
3. Build comparison and percentage measures
4. Debug DAX using DAX Studio (optional)

---

## The Scenario
Management wants to see not just current metrics, but comparisons: year-over-year growth, year-to-date performance, and percentage breakdowns. You will extend your measures library with advanced CALCULATE patterns.

---

## Core Tasks

### Task 1: Prepare the Date Table (20 mins)

Time intelligence requires a proper date table marked in Power BI.

1. Verify `DIM_DATE` contains:
   - Continuous dates (no gaps)
   - A column with Date data type (`full_date`)

2. Mark as Date Table:
   - Select `DIM_DATE` in Model View
   - **Table tools** > **Mark as date table**
   - Select the `full_date` column

**Checkpoint:** Date table marked (no warning icon).

---

### Task 2: Create Fixed Filter Measures (30 mins)

**Revenue for a Specific Year:**
```dax
Revenue 2023 = 
CALCULATE(
    [Total Revenue],
    DIM_DATE[year] = 2023
)
```

**Revenue for a Specific Segment:**
```dax
Revenue Automobile = 
CALCULATE(
    [Total Revenue],
    DIM_CUSTOMER[market_segment] = "AUTOMOBILE"
)
```

**Revenue Excluding a Status:**
```dax
Revenue Completed Only = 
CALCULATE(
    [Total Revenue],
    FCT_ORDER_LINES[status] = "completed"
)
```

**Test:** Create a matrix and verify these return the same value regardless of year slicer selection for the first measure.

**Checkpoint:** Fixed filter measures work correctly.

---

### Task 3: Create ALL Measures (30 mins)

ALL removes filters from specified columns/tables.

**Total Revenue All Years (for % calculations):**
```dax
Total Revenue All Years = 
CALCULATE(
    [Total Revenue],
    ALL(DIM_DATE)
)
```

**Percentage of Total:**
```dax
% of Total Revenue = 
DIVIDE(
    [Total Revenue],
    [Total Revenue All Years],
    0
)
```
Format as Percentage.

**Percentage of Segment:**
```dax
% of Segment = 
DIVIDE(
    [Total Revenue],
    CALCULATE(
        [Total Revenue],
        ALL(DIM_PRODUCT)
    ),
    0
)
```

**Test:** Add to matrix by year - percentages should sum to 100%.

**Checkpoint:** Percentage measures work correctly.

---

### Task 4: Time Intelligence Measures (45 mins)

**Year-to-Date Revenue:**
```dax
Revenue YTD = 
TOTALYTD(
    [Total Revenue],
    DIM_DATE[full_date]
)
```

**Prior Year Revenue:**
```dax
Revenue PY = 
CALCULATE(
    [Total Revenue],
    SAMEPERIODLASTYEAR(DIM_DATE[full_date])
)
```

**Year-over-Year Growth:**
```dax
YoY Growth = 
DIVIDE(
    [Total Revenue] - [Revenue PY],
    [Revenue PY],
    0
)
```
Format as Percentage.

**Year-over-Year Growth Amount:**
```dax
YoY Growth Amount = 
[Total Revenue] - [Revenue PY]
```
Format as Currency.

**Quarter-to-Date:**
```dax
Revenue QTD = 
TOTALQTD(
    [Total Revenue],
    DIM_DATE[full_date]
)
```

**Test:** Create a matrix with months and verify YTD accumulates correctly.

**Checkpoint:** Time intelligence measures calculate correctly.

---

### Task 5: Context Transition Understanding (30 mins)

Create a visual that demonstrates context transition:

1. Create a table visual:
   - Rows: Individual customer names from `DIM_CUSTOMER[customer_name]`
   - Values: `[Total Revenue]`

2. Observe: For each customer row, CALCULATE creates a filter context automatically

3. Create a calculated column in `DIM_CUSTOMER` (to show the difference):
   ```dax
   Customer Total = CALCULATE([Total Revenue])
   ```
   (This uses context transition - CALCULATE converts row context to filter context)

**Document your understanding:**
- What is row context?
- What is filter context?
- How does CALCULATE bridge them?

**Checkpoint:** Explain context transition in your own words.

---

### Task 6: Create a KPI Summary Page (30 mins)

Build a report page with:

1. **KPI Cards Row:**
   - Total Revenue
   - Revenue YTD
   - YoY Growth %

2. **Comparison Table:**
   - Matrix with Year on rows
   - Show: Revenue, Revenue PY, YoY Growth, YoY Growth Amount

3. **Segment Analysis:**
   - Bar chart by market segment
   - Show: % of Total Revenue

**Checkpoint:** KPI page displays all advanced measures correctly.

---

## Deliverables

Submit the following:

1. **Power BI File (.pbix):** With all CALCULATE measures
2. **Screenshot 1:** Matrix showing time intelligence measures
3. **Screenshot 2:** KPI summary page
4. **Explanation:** 1 paragraph explaining context transition

---

## Definition of Done

- [ ] Date table marked properly
- [ ] 3 fixed filter CALCULATE measures created
- [ ] 3 percentage/ALL measures created
- [ ] 5 time intelligence measures created
- [ ] Context transition documented
- [ ] KPI summary page built
- [ ] All measures formatted correctly

---

## Stretch Goals (Optional)

1. Install DAX Studio and connect to Power BI
2. Run a query to see actual DAX generated
3. Create a rolling 3-month average measure
4. Create a measure for same period prior year QTD

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Time intelligence error | Ensure date table is marked, use date column not key |
| SAMEPERIODLASTYEAR returns blank | Need prior year data in dataset |
| YoY shows unexpected values | Check base measures work before combining |
| % shows > 100% | Using wrong ALL scope |
| CALCULATE nested warning | Restructure to avoid nested CALCULATE when possible |
