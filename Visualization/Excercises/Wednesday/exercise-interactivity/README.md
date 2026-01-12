# Exercise: Interactivity - Slicers and Drill-Through

## Overview
**Day:** 3-Wednesday
**Duration:** 2-3 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Report design exercise completed

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Slicing and Filtering | [slicing-and-filtering.md](../../content/3-Wednesday/slicing-and-filtering.md) | Filter types, interactions |
| Analyze Feature | [analyze-feature.md](../../content/3-Wednesday/analyze-feature.md) | AI insights, Q&A |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Add and configure different slicer types
2. Control cross-filtering between visuals
3. Create drill-through pages for detailed analysis
4. Implement sync slicers across pages

---

## The Scenario
Users want interactive control over their data views. You will enhance your report with filtering capabilities that allow exploration without building custom reports for each scenario.

---

## Core Tasks

### Task 1: Add Standard Slicers (30 mins)

**Year Slicer:**
1. Add a Slicer visual
2. Field: `DIM_DATE[year]`
3. Format as **Tile** (horizontal buttons)
4. Enable "Single select" or "Multi-select" as appropriate

**Segment Slicer:**
1. Add another Slicer
2. Field: `DIM_CUSTOMER[market_segment]`
3. Format as **Dropdown** (saves space)
4. Set header title

**Product Type Slicer:**
1. Add a Slicer for product dimension
2. Field: `DIM_PRODUCT[manufacturer]`
3. Format as **List** with search enabled

**Position slicers:**
- Place in consistent location (top or sidebar)
- Ensure they don't obscure main visuals

**Test:** Make selections and verify all visuals filter correctly.

**Checkpoint:** 3 slicers working on main page.

---

### Task 2: Configure Cross-Filtering (30 mins)

**Default behavior:** Clicking a visual filters other visuals.

**Customize interactions:**
1. Select a visual (e.g., the bar chart)
2. **Format** > **Edit interactions**
3. For each other visual, choose:
   - **Filter** (cross-filter): Filters the other visual
   - **Highlight**: Highlights matching values
   - **None**: No interaction

**Scenario configurations:**

| Source Visual | Target Visual | Interaction |
|---------------|---------------|-------------|
| Year Bar Chart | Segment Pie | Filter |
| Year Bar Chart | Summary Cards | None |
| Segment Pie | Year Bar Chart | Highlight |

**Why different interactions?**
- Cards should show filtered totals, not be excluded
- Some visuals work better with highlighting than filtering

**Test:** Click around and verify interactions work as configured.

**Checkpoint:** Document your interaction configuration.

---

### Task 3: Create a Drill-Through Page (45 mins)

**Purpose:** Allow users to right-click a data point and see details.

**Step 1: Create the detail page**
1. Add a new page named "Customer Details"
2. Design for showing individual customer information

**Step 2: Add drill-through field**
1. In the Filters pane on "Customer Details" page
2. Find "Drill through" section
3. Drag `DIM_CUSTOMER[customer_key]` into drill through

**Step 3: Build detail visuals**
Add visuals that make sense for a single customer:
- Card: Customer Name
- Card: Total Revenue for this customer
- Table: Recent orders for this customer
- Line chart: Customer's order history over time

**Step 4: Add back button**
1. The back button is auto-added (look for arrow)
2. OR add custom button with Action > Back

**Test drill-through:**
1. Go to main page
2. Right-click a customer data point in any visual
3. Select **Drill through** > **Customer Details**
4. Verify the detail page shows correct customer data

**Checkpoint:** Drill-through navigation working.

---

### Task 4: Sync Slicers Across Pages (30 mins)

**Problem:** Selecting a year on page 1 doesn't affect page 2.

**Solution: Sync slicers**
1. Go to **View** > **Sync slicers** (opens pane)
2. Select your Year slicer
3. In the sync pane, check which pages to sync
4. Also choose whether slicer is visible on each page

**Configuration options:**
| Page | Sync (applies filter) | Visible (shows slicer) |
|------|----------------------|------------------------|
| Overview | Yes | Yes |
| Analysis | Yes | Yes |
| Trends | Yes | Yes |
| Customer Details | No | No |

**Repeat for other slicers as needed.**

**Test:** Make selection on page 1, navigate to page 2, verify filter applied.

**Checkpoint:** Slicers sync across pages.

---

### Task 5: Advanced Filtering Options (30 mins)

**Top N Filter:**
1. Select a visual showing products or customers
2. In Filters pane, expand the field filter
3. Change from "Basic" to "Top N"
4. Configure: Top 10 by [Total Revenue]

**Relative Date Filter:**
1. Add a date field to Filters pane
2. Change to "Relative date"
3. Configure: Last 3 months, last year, etc.

**Include/Exclude:**
1. Create a filter that excludes specific values
2. Example: Exclude "cancelled" status orders

**Document your filters:**

| Filter | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| | | | |

**Checkpoint:** At least 2 advanced filters configured.

---

### Task 6: Test User Experience (20 mins)

**Walkthrough test:**
Pretend you are an end user. Navigate through:

1. Start at Overview page
2. Filter to a specific year and segment
3. Drill through to see a specific customer
4. Navigate back
5. Check another page - are filters maintained?

**Document issues found:**

| Issue | Page | Resolution |
|-------|------|------------|
| | | |

**Checkpoint:** User experience validated and documented.

---

## Deliverables

Submit the following:

1. **Power BI File (.pbix):** With all interactivity features
2. **Screenshot 1:** Slicers in action (filtered state)
3. **Screenshot 2:** Drill-through page with customer detail
4. **Configuration Document:** Interaction settings and sync configuration

---

## Definition of Done

- [ ] At least 3 slicers added with appropriate types
- [ ] Cross-filtering configured intentionally
- [ ] Drill-through page created and working
- [ ] Slicers sync across appropriate pages
- [ ] Advanced filters (Top N or Relative Date) implemented
- [ ] User experience tested and documented

---

## Stretch Goals (Optional)

1. Create a second drill-through for products
2. Add Q&A visual for natural language queries
3. Create bookmarks for common filter combinations
4. Add "Clear all filters" button

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Drill-through option missing | Ensure drill-through field is configured on target page |
| Back button missing | Add manually or check it wasn't deleted |
| Sync not working | Verify sync pane settings, may need same slicer |
| Filters conflict | Check filter hierarchy (report > page > visual) |
| Too many slicers | Consider using fewer with dropdown style |
