# Writing Queries in Power BI

## Learning Objectives
- Understand how Power BI generates queries from your data model
- Recognize how relationships affect query results
- Troubleshoot unexpected query behavior
- Optimize models for efficient querying

## Why This Matters

When you create a visual in Power BI, you are not writing SQL or DAX directly---Power BI generates queries based on your data model and visual configuration. Understanding how these queries work helps you:

- Design models that produce correct results
- Diagnose why a visual shows unexpected numbers
- Optimize performance by reducing query complexity

This knowledge bridges your SQL experience from Snowflake to the DAX query engine that powers Power BI.

## How Power BI Generates Queries

Every visual in Power BI is backed by a DAX query. When you drag fields onto a visual:

1. **Columns become GROUP BY**: Fields in rows, columns, or axes
2. **Measures become aggregations**: Fields in values
3. **Filters from slicers**: Applied as WHERE-like conditions
4. **Relationships determine joins**: Tables are connected automatically

### Example: Simple Visual Query

For a bar chart showing Sales by Category:

**Visual Configuration:**
- Axis: `Dim_Product[Category]`
- Values: `SUM(Fact_Sales[Amount])`

**Generated Query Logic:**
```sql
-- Conceptual SQL equivalent
SELECT 
    Dim_Product.Category,
    SUM(Fact_Sales.Amount) AS Amount
FROM Fact_Sales
JOIN Dim_Product ON Fact_Sales.ProductKey = Dim_Product.ProductKey
GROUP BY Dim_Product.Category
```

**Actual DAX query (internal):**
```dax
EVALUATE
SUMMARIZECOLUMNS(
    Dim_Product[Category],
    "Amount", SUM(Fact_Sales[Amount])
)
```

## Relationship-Aware Calculations

The data model's relationships automatically propagate filters, joining tables without explicit JOIN syntax.

### Filter Propagation

When you filter by a dimension attribute:

```
User selects: Category = "Electronics"

Filter flow:
Dim_Product[Category] = "Electronics"
    |
    v (relationship)
Fact_Sales (only rows matching Electronics products)
    |
    v (relationship)
Dim_Customer (all customers who bought Electronics)
```

### Multi-Table Calculations

Consider a visual showing:
- Customer Name (from Dim_Customer)
- Total Sales (from Fact_Sales)
- Product Count (from Fact_Sales/Dim_Product)

Power BI:
1. Groups by Customer Name
2. Filters Fact_Sales to each customer's rows
3. Sums the Amount column
4. Counts distinct products

All table joins happen automatically through defined relationships.

## Understanding Filter Context

Every DAX calculation runs within a **filter context**---the set of filters currently active.

### Sources of Filter Context

| Source | Description |
|--------|-------------|
| **Visual filters** | Fields used in rows, columns, axis |
| **Slicers** | User selections on the report page |
| **Visual-level filters** | Filters applied to specific visuals |
| **Page-level filters** | Filters applied to all visuals on a page |
| **Report-level filters** | Filters applied to all pages |
| **Row-level security** | Filters applied based on user identity |

### Context Visualization

For a visual showing Sales by Year:

```
Filter Context for Year 2023:
+------------------+
|                  |
|  Dim_Date[Year]  |
|      = 2023      |
|                  |
|       |          |
|       v (filter) |
|                  |
|   Fact_Sales     |
|  (2023 rows)     |
|                  |
+------------------+

Result: SUM only includes 2023 sales
```

## Implicit vs. Explicit Measures

### Implicit Measures (Avoid)

When you drag a numeric column directly to a visual, Power BI creates an implicit aggregation:
- Drag `Fact_Sales[Amount]` to Values
- Power BI defaults to SUM (or whatever you select from dropdown)

**Problems with implicit measures:**
- Not reusable across visuals
- Aggregation type not obvious
- No documentation or standardization

### Explicit Measures (Recommended)

Create named measures with defined aggregation logic:

```dax
Total Sales = SUM(Fact_Sales[Amount])
```

**Benefits:**
- Consistent across all visuals
- Self-documenting with meaningful names
- Central location for business logic
- Better performance in some scenarios

## Query Performance Basics

### Understanding Storage Engine vs. Formula Engine

Power BI has two engines:

| Engine | Role | Optimization |
|--------|------|--------------|
| **Storage Engine (SE)** | Retrieves data from VertiPaq cache | Very fast, parallelized |
| **Formula Engine (FE)** | Calculates complex DAX | Single-threaded, slower |

**Goal:** Push as much work as possible to the Storage Engine.

### Using Performance Analyzer

1. Go to **View** > **Performance analyzer**
2. Click **Start recording**
3. Click **Refresh visuals** (or interact with reports)
4. Review metrics for each visual

**Key metrics:**
- **DAX query**: Time to execute the query
- **Visual display**: Time to render the visual
- **Other**: Background processing time

### Common Performance Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| **High cardinality columns** | Slow filtering, large file | Remove unnecessary columns |
| **Complex measures** | Slow DAX query time | Simplify or pre-aggregate |
| **Many visuals** | Slow page load | Reduce visual count, use bookmarks |
| **Bidirectional relationships** | Unexpected results, slow queries | Use single direction |

## DirectQuery Considerations

When using DirectQuery mode, Power BI sends queries directly to the source database.

### Native Query Viewing

For DirectQuery, you can see the actual SQL:
1. In Power Query, right-click a step
2. Select **View Native Query** (if available)

### Performance Tips for DirectQuery

- Ensure source database has proper indexes
- Limit visuals per page (each generates queries)
- Avoid complex DAX that cannot push to source
- Consider aggregations tables for common queries

### Aggregations Pattern

Create pre-aggregated summary tables that Power BI queries first:

```
User Query: Sum of Sales by Month
    |
    v
[1] Check Aggregations table (small, Import mode)
    - If aggregation matches, use it (fast)
    |
    v
[2] Fall back to detail table (DirectQuery)
    - Only if aggregation doesn't cover the query
```

## Analyzing Query Behavior

### DAX Studio Integration

DAX Studio is an external tool for analyzing Power BI queries:

1. Connect DAX Studio to your Power BI file
2. Capture and analyze generated queries
3. View server timings and query plans

We will cover DAX Studio in detail later in this module.

### Common Query Troubleshooting

**Issue: Blank or unexpected values**
- Check relationships are correctly defined
- Verify cardinality settings
- Ensure filter direction allows propagation

**Issue: Different results than expected**
- Analyze the filter context
- Check for inactive relationships
- Verify measure logic handles blanks

**Issue: Slow query performance**
- Reduce visual complexity
- Simplify DAX calculations
- Check for high cardinality columns

## Summary

- Power BI generates DAX queries based on visual configuration and data model relationships
- Filter context combines all active filters to determine which rows are included in calculations
- Explicit measures provide consistency and reusability over implicit aggregations
- Performance depends on pushing work to the Storage Engine and optimizing the data model
- DirectQuery mode sends queries directly to the source, requiring source optimization

## Additional Resources

- [Understanding Query Evaluation](https://docs.microsoft.com/en-us/dax/best-practices/dax-understand-star-schema) - DAX evaluation concepts
- [Performance Analyzer](https://docs.microsoft.com/en-us/power-bi/create-reports/desktop-performance-analyzer) - Built-in performance tool
- [DAX Studio](https://daxstudio.org/) - External query analysis tool
