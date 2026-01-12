# Exercise: Creating DAX Measures for Business Metrics

## Overview
**Day:** 2-Tuesday
**Duration:** 2-3 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Data modeling exercise completed, star schema configured

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| DAX Introduction | [dax-introduction.md](../../content/2-Tuesday/dax-introduction.md) | Syntax, measures vs columns, context |
| DAX Aggregation and Statistics | [dax-aggregation-statistics.md](../../content/2-Tuesday/dax-aggregation-statistics.md) | SUM, AVERAGE, COUNT, iterators |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Create basic DAX measures using aggregation functions
2. Organize measures in a dedicated measures table
3. Format measures for business presentation
4. Understand the difference between calculated columns and measures

---

## The Scenario
The business team needs key performance indicators (KPIs) to track sales performance. You will create a set of reusable DAX measures that can be used across multiple reports and dashboards.

---

## Core Tasks

### Task 1: Create a Measures Table (10 mins)

**Best Practice:** Store measures in a dedicated table for organization.

1. Click **Home** > **Enter Data**
2. Create a table with one column named "Helper"
3. Add one row with value "Measures Table"
4. Name the table `_Measures` (underscore puts it at top of list)
5. Click **Load**
6. Right-click the Helper column > **Hide in Report View**

**Checkpoint:** `_Measures` table visible in Fields pane.

---

### Task 2: Create Basic Aggregation Measures (45 mins)

Create each measure in the `_Measures` table:

**Total Revenue:**
```dax
Total Revenue = SUM(FCT_ORDER_LINES[net_amount])
```
Format as Currency.

**Total Quantity:**
```dax
Total Quantity = SUM(FCT_ORDER_LINES[quantity])
```
Format as Whole Number with thousands separator.

**Order Count:**
```dax
Order Count = DISTINCTCOUNT(FCT_ORDER_LINES[order_key])
```
Format as Whole Number.

**Customer Count:**
```dax
Customer Count = DISTINCTCOUNT(FCT_ORDER_LINES[customer_key])
```
Format as Whole Number.

**Product Count:**
```dax
Product Count = DISTINCTCOUNT(FCT_ORDER_LINES[product_key])
```
Format as Whole Number.

**Test each measure:** Add to a Card visual and verify values.

**Checkpoint:** All 5 basic measures created and formatted.

---

### Task 3: Create Calculated Metrics (45 mins)

Build measures that reference other measures:

**Average Order Value:**
```dax
Avg Order Value = DIVIDE([Total Revenue], [Order Count], 0)
```
Format as Currency.

**Revenue per Customer:**
```dax
Revenue per Customer = DIVIDE([Total Revenue], [Customer Count], 0)
```
Format as Currency.

**Units per Order:**
```dax
Units per Order = DIVIDE([Total Quantity], [Order Count], 0)
```
Format as Decimal with 2 places.

**Average Unit Price:**
```dax
Avg Unit Price = DIVIDE([Total Revenue], [Total Quantity], 0)
```
Format as Currency.

**Why DIVIDE instead of `/`?**
- Handles division by zero gracefully
- Returns alternate result (0) instead of error
- More robust for dynamic filtering

**Checkpoint:** All calculated metrics work correctly in visuals.

---

### Task 4: Create Statistical Measures (30 mins)

Add measures for deeper analysis:

**Min Order Value:**
```dax
Min Order Value = MIN(FCT_ORDER_LINES[net_amount])
```

**Max Order Value:**
```dax
Max Order Value = MAX(FCT_ORDER_LINES[net_amount])
```

**Order Line Count:**
```dax
Order Line Count = COUNTROWS(FCT_ORDER_LINES)
```

**Distinct Dates with Sales:**
```dax
Sales Days = DISTINCTCOUNT(FCT_ORDER_LINES[date_key])
```

**Checkpoint:** Statistical measures display correctly.

---

### Task 5: Test with Filters (30 mins)

Verify measures respond to filter context:

1. Create a matrix visual:
   - Rows: `DIM_DATE[year]`
   - Values: `[Total Revenue]`, `[Order Count]`, `[Avg Order Value]`

2. Add a slicer for `DIM_CUSTOMER[market_segment]`

3. Select different segments and observe:
   - Do values change?
   - Do derived measures recalculate correctly?

4. Document observations:

| Segment | Total Revenue | Order Count | Avg Order Value |
|---------|---------------|-------------|-----------------|
| All Segments | | | |
| AUTOMOBILE | | | |
| MACHINERY | | | |

**Checkpoint:** Measures respond correctly to slicer selections.

---

### Task 6: Organize and Document (20 mins)

1. **Create measure folders** (if using display folders):
   - Right-click measure > Properties > Display Folder
   - Suggested folders: "Base Metrics", "Derived Metrics", "Statistics"

2. **Add descriptions** to each measure:
   - Right-click measure > Properties > Description
   - Explain what each measure calculates

3. **Create a Measures Reference Sheet:**

| Measure | Formula | Purpose | Format |
|---------|---------|---------|--------|
| Total Revenue | SUM(FCT_ORDER_LINES[net_amount]) | Total sales amount | Currency |
| | | | |

---

## Deliverables

Submit the following:

1. **Power BI File (.pbix):** With all measures created
2. **Screenshot:** Measures table expanded in Fields pane
3. **Test Visual Screenshot:** Matrix showing measures by year
4. **Measures Reference:** Documented list of all measures

---

## Definition of Done

- [ ] _Measures table created
- [ ] 5 basic aggregation measures created
- [ ] 4 calculated metric measures created
- [ ] 4 statistical measures created
- [ ] All measures formatted appropriately
- [ ] Measures respond to filter context
- [ ] Documentation completed

---

## Stretch Goals (Optional)

1. Create a % of Total measure using DIVIDE and ALL
2. Create a ranking measure using RANKX
3. Use SUMX for a calculated aggregation
4. Experiment with CALCULATE to force specific filters

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Measure returns blank | Check column references, table may be filtered to empty |
| DIVIDE returns error | Ensure third argument provided for alternate result |
| Wrong aggregation | Verify correct function (SUM vs COUNT vs DISTINCTCOUNT) |
| Measure not visible | Check if hidden or in wrong table |
| Format not applying | Re-apply format in Modeling tab after selecting measure |
