# Exercise: Multi-Page Report Design

## Overview
**Day:** 3-Wednesday
**Duration:** 3-4 hours
**Mode:** Individual (Implementation + Design)
**Prerequisites:** Tuesday exercises completed, DAX measures created

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Report Generation | [report-generation.md](../../content/3-Wednesday/report-generation.md) | Report structure, design principles |
| Creating Reports | [creating-reports.md](../../content/3-Wednesday/creating-reports.md) | Visual types, when to use each |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Design a multi-page report with consistent styling
2. Choose appropriate visual types for different data stories
3. Create effective layouts that guide the user's eye
4. Apply consistent formatting across all pages

---

## The Scenario
The executive team needs a comprehensive sales report. You will design a 3-page report that tells a complete story: Overview (big picture), Details (drill-down), and Trends (time analysis).

---

## Core Tasks

### Task 1: Plan Your Report (30 mins)

Before building, sketch your plan on paper or in a document:

**Page 1: Executive Overview**
- Purpose: High-level KPIs at a glance
- Target audience: C-suite executives
- Key metrics to display: _____________

**Page 2: Product/Customer Analysis**
- Purpose: Detailed breakdown by dimension
- Target audience: Sales managers
- Key comparisons: _____________

**Page 3: Trend Analysis**
- Purpose: Time-based patterns
- Target audience: Analysts
- Key trends: _____________

**Checkpoint:** Report plan documented with page purposes and target metrics.

---

### Task 2: Create Page 1 - Executive Overview (45 mins)

**Header Section (top):**
1. Add a text box with report title: "Sales Performance Dashboard"
2. Add date/time stamp (optional)

**KPI Cards Row:**
Add 4 Card visuals showing:
- Total Revenue
- Order Count
- Customer Count
- YoY Growth %

**Main Visual:**
Add a Clustered Bar Chart:
- Axis: `DIM_DATE[year]`
- Values: `[Total Revenue]`

**Supporting Visual:**
Add a Donut/Pie Chart:
- Legend: `DIM_CUSTOMER[market_segment]`
- Values: `[Total Revenue]`

**Layout Guidelines:**
```
+----------------------------------------+
| Title                          Filters |
+----------------------------------------+
| Card 1 | Card 2 | Card 3 | Card 4     |
+----------------------------------------+
|                        |              |
|   Bar Chart            | Pie/Donut   |
|   (Revenue by Year)    | (Segments)  |
|                        |              |
+----------------------------------------+
```

**Checkpoint:** Page 1 complete with proper layout.

---

### Task 3: Create Page 2 - Analysis (45 mins)

**Focus:** Allow users to explore data by different dimensions.

**Top Section:**
Add a matrix visual:
- Rows: `DIM_PRODUCT[manufacturer]`
- Columns: `DIM_DATE[year]`
- Values: `[Total Revenue]`, `[Order Count]`

**Bottom Section:**
Add two visualizations side by side:

1. **Customer Analysis (left):**
   - Stacked Bar Chart or Treemap
   - Show revenue by customer segment and year

2. **Product Analysis (right):**
   - Table or sorted bar chart
   - Show top 10 products by revenue

**Layout Guidelines:**
```
+----------------------------------------+
| Title: Detailed Analysis              |
+----------------------------------------+
|                                        |
|        Matrix (Mfr x Year)            |
|                                        |
+----------------------------------------+
| Customer Breakdown  | Top Products    |
| (Stacked Bar)       | (Table)         |
+----------------------------------------+
```

**Checkpoint:** Page 2 complete with matrix and breakdowns.

---

### Task 4: Create Page 3 - Trends (45 mins)

**Focus:** Time-based analysis for pattern recognition.

**Main Visual:**
Line Chart showing revenue over time:
- Axis: `DIM_DATE[full_date]` or `DIM_DATE[month_name]`
- Values: `[Total Revenue]`
- Legend: `DIM_DATE[year]` (for year-over-year comparison)

**Supporting Visuals:**

1. **Monthly Trend:**
   Area Chart:
   - Axis: Month
   - Values: `[Revenue YTD]`

2. **Growth Analysis:**
   Clustered Column Chart:
   - Axis: `DIM_DATE[quarter]`
   - Values: `[Total Revenue]`, `[Revenue PY]`

**Optional - Sparklines:**
If using table, add sparklines for quick trend visualization.

**Layout Guidelines:**
```
+----------------------------------------+
| Title: Trend Analysis                 |
+----------------------------------------+
|                                        |
|     Line Chart (Multi-Year Trend)     |
|                                        |
+----------------------------------------+
| YTD Area Chart   | Growth Column Chart|
+----------------------------------------+
```

**Checkpoint:** Page 3 complete with trend visuals.

---

### Task 5: Apply Consistent Styling (30 mins)

**Theme Selection:**
1. **View** > **Themes** > Choose or customize
2. OR import a custom theme JSON

**Color Consistency:**
- Use the same colors for same dimensions across pages
- Example: Blue for 2024, Gray for 2023 everywhere

**Font Consistency:**
- Title: Large, bold
- Subtitle: Medium
- Data labels: Small

**Visual Formatting:**
For each visual:
1. Remove unnecessary elements (gridlines if cluttered)
2. Add data labels where useful
3. Ensure proper axis titles

**Checkpoint:** All three pages use consistent styling.

---

### Task 6: Add Navigation (20 mins)

**Option 1: Bookmarks (simpler)**
1. Create a bookmark for each page
2. Add buttons that navigate to bookmarks

**Option 2: Page Navigation Buttons**
1. Add button shapes
2. Configure Action > Page navigation

**Add to each page:**
- "Back to Overview" button
- Page indicator (which page am I on?)

**Checkpoint:** Navigation works between all pages.

---

## Deliverables

Submit the following:

1. **Power BI File (.pbix):** Complete 3-page report
2. **Screenshots:** One screenshot per page
3. **Design Document:** Your initial planning sketch
4. **Self-Assessment:** Rate your report on:
   - Clarity (1-5)
   - Visual appeal (1-5)
   - Information density (1-5)

---

## Definition of Done

- [ ] 3 pages created with distinct purposes
- [ ] Page 1 has KPI cards and summary visuals
- [ ] Page 2 has detailed analysis visuals
- [ ] Page 3 has time-based trend visuals
- [ ] Consistent styling applied across all pages
- [ ] Navigation between pages working
- [ ] Report tells a coherent story

---

## Stretch Goals (Optional)

1. Create a mobile layout for the report
2. Add a "landing page" with report description
3. Implement bookmarks for different filter states
4. Export the theme as a JSON file for reuse

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Visuals misaligned | Use Format > Align and Distribute |
| Theme not applying | Check theme is saved and selected |
| Navigation not working | Verify Action is enabled on button |
| Colors inconsistent | Set specific colors in visual format |
| Page cluttered | Reduce number of visuals, increase whitespace |
