# Dimension Types

## Learning Objectives

- Understand different types of dimension tables
- Learn conformed, degenerate, junk, and role-playing dimensions
- Know when to apply each dimension type
- Apply dimension design patterns to real scenarios

## Why This Matters

Not all dimensions are simple lookup tables. Understanding dimension types helps you handle complex design scenarios like shared dimensions across fact tables, dimensions without tables, and multiple uses of the same dimension. These patterns appear in real-world data warehouse designs.

## The Concept

### Overview of Dimension Types

| Type | Description |
|------|-------------|
| Conformed | Shared across multiple fact tables |
| Degenerate | Dimension value in fact table with no dimension table |
| Junk | Combines miscellaneous flags/attributes |
| Role-Playing | Same dimension used multiple times in a fact |
| Outrigger | Connected to another dimension, not fact |

### Conformed Dimensions

Conformed dimensions are shared and consistent across multiple fact tables.

**Purpose:**
- Enable cross-process analysis (compare sales to inventory)
- Ensure consistent definitions enterprise-wide
- Support drill-across queries

**Example:**
```
fact_sales --------+
                   |
                   +--> dim_date (conformed)
                   |
fact_inventory ----+

fact_sales --------+
                   |
                   +--> dim_product (conformed)
                   |
fact_shipments ----+
```

**Benefits:**
- "Quantity sold" and "Quantity shipped" use same product definition
- Report on sales and inventory together meaningfully
- Single source of truth for shared concepts

**Implementation:**
```sql
-- One dim_date table used by all fact tables
CREATE TABLE warehouse.dim_date (
    date_key INT64,
    full_date DATE,
    year INT64,
    month INT64,
    day_of_week STRING,
    -- ... same structure used everywhere
);

-- Both facts reference the same dimension
SELECT
    d.year,
    SUM(s.revenue) AS sales_revenue,
    SUM(i.quantity) AS inventory_level
FROM warehouse.fact_sales s
JOIN warehouse.dim_date d ON s.date_key = d.date_key
JOIN warehouse.fact_inventory i ON i.date_key = d.date_key
GROUP BY d.year;
```

### Degenerate Dimensions

Degenerate dimensions are dimension values stored directly in the fact table without a corresponding dimension table.

**Purpose:**
- Handle unique identifiers (invoice number, order number)
- Values with no additional attributes worth storing

**Example:**
```sql
-- No dim_invoice table - invoice_number is degenerate
CREATE TABLE fact_sales (
    date_key INT64,
    customer_key INT64,
    product_key INT64,
    invoice_number STRING,  -- Degenerate dimension
    quantity INT64,
    amount NUMERIC
);
```

**When to Use:**
- Transaction numbers (invoice, order, receipt)
- Line item identifiers
- Values with no attributes beyond the ID itself

**Why Not Create a Table?**
```sql
-- This would be wasteful:
dim_invoice
+----------------+
| invoice_number |  <- PK with no other columns!
+----------------+
| INV-001        |
| INV-002        |
+----------------+
```

### Junk Dimensions

Junk dimensions combine miscellaneous low-cardinality attributes that do not belong elsewhere.

**Purpose:**
- Reduce fact table width
- Organize flags and codes
- Avoid many small dimensions

**Before Junk Dimension:**
```sql
fact_sales
+----------+----------+-------------+-----------+---------+--------+
| date_key | prod_key | payment_type| is_sale   | is_return| promo  |
+----------+----------+-------------+-----------+---------+--------+
| 20240615 | 101      | Credit      | Y         | N       | None   |
+----------+----------+-------------+-----------+---------+--------+
Many flags in the fact table
```

**After Junk Dimension:**
```sql
dim_transaction_type (junk dimension)
+---------+-------------+--------+-----------+--------+
| type_key| payment_type| is_sale| is_return | promo  |
+---------+-------------+--------+-----------+--------+
| 1       | Credit      | Y      | N         | None   |
| 2       | Credit      | Y      | N         | %10 Off|
| 3       | Debit       | Y      | N         | None   |
| 4       | Credit      | N      | Y         | None   |
+---------+-------------+--------+-----------+--------+

fact_sales
+----------+----------+----------+
| date_key | prod_key | type_key | <- Single FK instead of multiple flags
+----------+----------+----------+
```

**Benefits:**
- Cleaner fact table
- Easier to add new flags
- Pre-defined valid combinations

### Role-Playing Dimensions

Role-playing dimensions are a single dimension table used multiple times in a fact table, each time playing a different role.

**Example: Date Dimension**
```sql
fact_order
+----------+------------+------------+-------------+
| order_id | order_date | ship_date  | cancel_date |
+----------+------------+------------+-------------+

-- All three dates reference the same dim_date, playing different roles
```

**Implementation Options:**

**Option 1: Multiple FKs to Same Table**
```sql
CREATE TABLE fact_order (
    order_key INT64,
    order_date_key INT64,    -- FK to dim_date
    ship_date_key INT64,     -- FK to dim_date (same table)
    cancel_date_key INT64,   -- FK to dim_date (same table)
    customer_key INT64,
    amount NUMERIC
);

-- Query with aliases
SELECT
    o.order_key,
    od.full_date AS order_date,
    sd.full_date AS ship_date
FROM fact_order o
JOIN dim_date od ON o.order_date_key = od.date_key
JOIN dim_date sd ON o.ship_date_key = sd.date_key;
```

**Option 2: Views for Each Role**
```sql
-- Create views for clarity
CREATE VIEW dim_order_date AS SELECT * FROM dim_date;
CREATE VIEW dim_ship_date AS SELECT * FROM dim_date;
CREATE VIEW dim_cancel_date AS SELECT * FROM dim_date;
```

**Common Role-Playing Dimensions:**
- Date (order date, ship date, due date)
- Employee (salesperson, manager, approver)
- Location (origin, destination)

### Outrigger Dimensions

Outrigger dimensions are secondary dimensions attached to another dimension (not the fact table).

**Example:**
```
fact_sales --> dim_product --> dim_category (outrigger)
                          |
                          +--> dim_brand (outrigger)
```

**When to Use:**
- Shared attributes among products
- When attributes belong to a concept, not the fact
- Subtle snowflaking of a dimension

**Trade-off:** Similar to snowflake - adds joins but reduces redundancy.

### Dimension Type Summary

| Type | Key Characteristic | Use When |
|------|-------------------|----------|
| Conformed | Shared across facts | Cross-process analysis needed |
| Degenerate | No dim table | Unique IDs with no attributes |
| Junk | Combined flags | Many low-cardinality fields |
| Role-Playing | Same dim, multiple FKs | Dimension appears multiple times |
| Outrigger | Dim has its own dim | Sub-attribute grouping needed |

### Design Decision Tree

```
Does the dimension need to be shared across facts?
    Yes --> Conformed Dimension

Is it just an ID with no attributes?
    Yes --> Degenerate Dimension

Are there many flags/codes that don't warrant their own dimensions?
    Yes --> Junk Dimension

Does the same dimension appear multiple times (e.g., multiple dates)?
    Yes --> Role-Playing Dimension

Does a dimension have sub-dimensions?
    Yes --> Outrigger or Snowflake
```

## Summary

- **Conformed dimensions** are shared across multiple fact tables for consistency
- **Degenerate dimensions** are IDs stored in fact tables without separate tables
- **Junk dimensions** combine miscellaneous flags into one lookup table
- **Role-playing dimensions** use the same dimension multiple times with different meanings
- **Outrigger dimensions** are dimensions connected to other dimensions
- Choose dimension type based on data characteristics and query needs

## Additional Resources

- [Kimball Dimension Types](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Conformed Dimensions Explained](https://www.holistics.io/blog/conformed-dimensions/)
- [Junk Dimensions Tutorial](https://www.sqlservercentral.com/articles/junk-dimensions)
