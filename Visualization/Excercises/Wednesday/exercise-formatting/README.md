# Exercise: Conditional Formatting for KPI Indicators

## Overview
**Day:** 3-Wednesday
**Duration:** 2 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Report design and interactivity exercises completed

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Conditional Formatting | [conditional-formatting.md](../../content/3-Wednesday/conditional-formatting.md) | Data bars, color scales, icons, DAX formatting |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Apply data bars to show relative values
2. Configure color scales based on value ranges
3. Use icon sets for status indicators
4. Create DAX-driven dynamic formatting

---

## The Scenario
Management wants visual cues to quickly identify performance levels without reading numbers. You will apply conditional formatting to make key metrics instantly recognizable: green for good, red for concerning.

---

## Core Tasks

### Task 1: Add Data Bars (20 mins)

**Apply to a table column:**
1. Create or use existing table visual with:
   - Row: `DIM_PRODUCT[manufacturer]`
   - Values: `[Total Revenue]`, `[Order Count]`

2. Select the `[Total Revenue]` column
3. **Format** > **Cell elements** > **Data bars** = ON
4. Configure:
   - Positive bar color: Blue
   - Show bar only: Off (show number too)
   - Minimum/Maximum: Automatic

5. Repeat for `[Order Count]` with a different color

**Checkpoint:** Data bars visible in table.

---

### Task 2: Apply Color Scales (25 mins)

**Background color gradient:**
1. In same table, add `[Avg Order Value]` column
2. **Format** > **Cell elements** > **Background color** = ON
3. Click **fx** (conditional formatting)
4. Configure:
   - Format style: Gradient
   - What field: `[Avg Order Value]`
   - Minimum: Light color (e.g., yellow)
   - Maximum: Dark color (e.g., green)

**Font color scale:**
1. Add `[YoY Growth]` column
2. **Format** > **Cell elements** > **Font color** = ON
3. Configure:
   - Format style: Gradient
   - Minimum (negative): Red
   - Center (zero): Black
   - Maximum (positive): Green

**Checkpoint:** Both color scales working correctly.

---

### Task 3: Implement Icon Sets (30 mins)

**Add status icons based on rules:**
1. In table, ensure `[YoY Growth]` column visible
2. **Format** > **Cell elements** > **Icons** = ON
3. Click **fx** to configure
4. Set format by: **Rules**

5. Configure rules:

| If Value | Icon |
|----------|------|
| >= 0.10 (10%+) | Green Up Arrow |
| >= 0 and < 0.10 | Yellow Right Arrow |
| < 0 (negative) | Red Down Arrow |

6. Position: Left of data

**Alternative: Status Column**
Create a separate status column with only icons (no numbers):
- Duplicate the measure
- Configure icons
- Show icon only, hide values

**Checkpoint:** Icons display based on growth thresholds.

---

### Task 4: Rules-Based Formatting (25 mins)

**Highlight entire rows based on condition:**
1. Create a matrix:
   - Rows: `DIM_CUSTOMER[market_segment]`
   - Values: `[Total Revenue]`, `[Customer Count]`

2. Apply background color with rules:
   - If `[Total Revenue]` > 5,000,000 then Green
   - If `[Total Revenue]` between 2,000,000 and 5,000,000 then Yellow
   - Else Red

3. Configure at the cell level (not row)

**Test:** Verify colors match the rules correctly.

**Checkpoint:** Rules-based formatting applied.

---

### Task 5: DAX-Driven Formatting (30 mins)

**Create a formatting measure:**
```dax
Growth Color = 
SWITCH(
    TRUE(),
    [YoY Growth] >= 0.10, "#2E7D32",  // Dark green
    [YoY Growth] >= 0, "#FFC107",     // Yellow
    "#D32F2F"                          // Red
)
```

**Apply the measure:**
1. In a card or table, select a field
2. **Format** > **Cell elements** > **Background color** = ON
3. Format by: **Field value**
4. Select `[Growth Color]` measure

**Create text status measure:**
```dax
Performance Status = 
SWITCH(
    TRUE(),
    [YoY Growth] >= 0.10, "Exceeding",
    [YoY Growth] >= 0, "Meeting",
    "Below Target"
)
```

**Add to table as additional column.**

**Checkpoint:** DAX measures drive formatting dynamically.

---

### Task 6: Format Cards with Conditional Colors (20 mins)

**Dynamic card colors:**
1. Select a KPI card (e.g., YoY Growth)
2. **Format** > **Callout value** > **Color**
3. Click **fx** for conditional formatting
4. Configure rules similar to icons

**Card border or background:**
1. **Format** > **Visual** > **Background** > **fx**
2. Configure to change based on measure value

**Checkpoint:** Cards change color based on performance.

---

## Deliverables

Submit the following:

1. **Power BI File (.pbix):** With conditional formatting applied
2. **Screenshot 1:** Table with data bars and color scales
3. **Screenshot 2:** Table with icon indicators
4. **Screenshot 3:** Card with conditional coloring
5. **Measure List:** DAX measures created for formatting

---

## Definition of Done

- [ ] Data bars applied to at least 2 columns
- [ ] Color scale (gradient) applied to at least 1 column
- [ ] Icon set with 3+ levels configured
- [ ] Rules-based formatting implemented
- [ ] At least 1 DAX-driven formatting measure created
- [ ] Cards formatted conditionally
- [ ] All formatting supports the business narrative

---

## Stretch Goals (Optional)

1. Create a "traffic light" gauge visual
2. Implement sparklines in a table (if supported)
3. Create a tooltip page with conditional formatting
4. Use conditional formatting on a bar chart (different colors per bar)

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Color scale not showing gradient | Check min/max values aren't too close |
| Icons not appearing | Verify rules cover all value ranges |
| DAX measure returns blank | Add format validation in SWITCH |
| Colors don't update with filter | Ensure measure references filter context |
| Too many colors | Simplify to 3 levels max for clarity |
