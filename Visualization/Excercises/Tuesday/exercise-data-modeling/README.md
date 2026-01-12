# Exercise: Building a Star Schema Data Model

## Overview
**Day:** 2-Tuesday
**Duration:** 2-3 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Monday exercises completed, data imported into Power BI

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Designing Schemas | [designing-schemas.md](../../content/2-Tuesday/designing-schemas.md) | Star schema, cardinality, cross-filter direction |
| Writing Queries | [writing-queries.md](../../content/2-Tuesday/writing-queries.md) | How relationships affect queries |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Identify fact and dimension tables in a dataset
2. Create relationships between tables in Power BI Model View
3. Configure cardinality and cross-filter direction correctly
4. Validate that your model matches star schema principles

---

## The Scenario
You have imported the Snowflake GOLD zone tables from Week 5. Now you need to verify and configure the relationships in Power BI to create a proper star schema that enables efficient DAX calculations and visual filtering.

---

## Core Tasks

### Task 1: Load Tables from Snowflake (if not done) (20 mins)

Ensure you have these tables loaded:
- `DIM_DATE`
- `DIM_CUSTOMER`
- `DIM_PRODUCT`
- `FCT_ORDER_LINES`

If not loaded, connect to Snowflake and import them.

---

### Task 2: Identify Fact vs Dimension (20 mins)

Open Model View and analyze each table:

| Table | Type (Fact/Dim) | Reason |
|-------|-----------------|--------|
| DIM_DATE | | |
| DIM_CUSTOMER | | |
| DIM_PRODUCT | | |
| FCT_ORDER_LINES | | |

**Indicators of Fact Tables:**
- Contains numeric measures (quantities, amounts)
- Has foreign keys to dimensions
- Usually the largest table
- Records transactions/events

**Indicators of Dimension Tables:**
- Contains descriptive attributes
- Has a primary key (often surrogate)
- Relatively static data
- Smaller row count

**Checkpoint:** Document your classification with reasoning.

---

### Task 3: Review Auto-Detected Relationships (15 mins)

1. In Model View, examine existing relationship lines
2. For each relationship, double-click to see properties:
   - Which columns are connected?
   - What is the detected cardinality?
   - Is it active?

3. Document what Power BI detected:

| From Table | From Column | To Table | To Column | Cardinality |
|------------|-------------|----------|-----------|-------------|
| | | | | |
| | | | | |

**Note any missing or incorrect relationships.**

---

### Task 4: Create/Fix Relationships (45 mins)

**Delete incorrect relationships:**
1. Right-click relationship line > Delete

**Create proper star schema relationships:**

1. **Date to Fact:**
   - Drag `DIM_DATE.date_key` to `FCT_ORDER_LINES.date_key`
   - Verify: One-to-Many (1:*)
   - Cross-filter: Single (Dim to Fact)
   - Active: Yes

2. **Customer to Fact:**
   - Drag `DIM_CUSTOMER.customer_key` to `FCT_ORDER_LINES.customer_key`
   - Verify: One-to-Many (1:*)
   - Cross-filter: Single
   - Active: Yes

3. **Product to Fact:**
   - Drag `DIM_PRODUCT.product_key` to `FCT_ORDER_LINES.product_key`
   - Verify: One-to-Many (1:*)
   - Cross-filter: Single
   - Active: Yes

**Checkpoint:** Screenshot of Model View with all relationships configured.

---

### Task 5: Validate the Model (30 mins)

**Create test visuals to verify relationships work:**

1. **Test 1: Sales by Year**
   - Bar chart
   - Axis: `DIM_DATE[year]`
   - Values: `Sum of FCT_ORDER_LINES[net_amount]`
   - Does it aggregate correctly?

2. **Test 2: Sales by Customer Segment**
   - Bar chart
   - Axis: `DIM_CUSTOMER[market_segment]`
   - Values: `Sum of FCT_ORDER_LINES[net_amount]`
   - Does filtering work?

3. **Test 3: Cross-filtering**
   - Add both visuals to same page
   - Click on a year bar
   - Does the customer chart filter?

**Checkpoint:** All test visuals work correctly.

---

### Task 6: Document Your Model (30 mins)

Create a model documentation file:

1. **Star Schema Diagram:**
   Draw or describe the final model:
   ```
                [DIM_DATE]
                    |
                    v
   [DIM_CUSTOMER] --> [FCT_ORDER_LINES] <-- [DIM_PRODUCT]
   ```

2. **Relationship Details Table:**

| Relationship | Cardinality | Filter Direction | Active | Purpose |
|--------------|-------------|------------------|--------|---------|
| DIM_DATE -> FCT | 1:* | Single | Yes | Time analysis |
| DIM_CUSTOMER -> FCT | | | | |
| DIM_PRODUCT -> FCT | | | | |

3. **Design Decisions:**
   - Why single direction filtering?
   - What would happen with bidirectional?

---

## Deliverables

Submit the following:

1. **Power BI File (.pbix):** With configured star schema
2. **Screenshot:** Model View showing relationships
3. **Documentation:** Model documentation with diagram and rationale
4. **Test Results:** Screenshots of working test visuals

---

## Definition of Done

- [ ] All four tables loaded in Power BI
- [ ] Tables classified as Fact or Dimension
- [ ] Three 1:* relationships created
- [ ] All relationships use single-direction filtering
- [ ] Test visuals aggregate correctly
- [ ] Cross-filtering works between visuals
- [ ] Model documentation completed

---

## Stretch Goals (Optional)

1. Create a Date table using DAX CALENDAR function
2. Mark the date table as a date table
3. Add a table with no relationships and observe behavior
4. Experiment with bidirectional filtering and document issues

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Cannot create relationship | Check data types match between columns |
| "Ambiguous path" warning | Multiple active relationships; deactivate one |
| Measures show wrong totals | Verify relationship direction and cardinality |
| Cross-filter not working | Check all relationships are active |
| Circular dependency | Remove bidirectional filtering |
