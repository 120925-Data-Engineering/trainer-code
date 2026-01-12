# The DAX CALCULATE Function

## Learning Objectives
- Understand CALCULATE as the most important DAX function
- Apply filter modification techniques
- Master context transition from row to filter context
- Use common CALCULATE patterns in business scenarios

## Why This Matters

CALCULATE is the most powerful and most used function in DAX. It enables time intelligence, filtered totals, what-if analysis, and percentage calculations. Mastering CALCULATE is the gateway to expert-level analytics.

## CALCULATE Syntax

```dax
CALCULATE(
    <expression>,
    [<filter1>],
    [<filter2>],
    ...
)
```

**What CALCULATE does:**
1. Takes the current filter context
2. Modifies it according to the filter arguments
3. Evaluates the expression in the new context

## Basic CALCULATE Examples

### Adding a Filter

```dax
Total Sales = SUM(Fact_Sales[Amount])

Electronics Sales = CALCULATE(
    SUM(Fact_Sales[Amount]),
    Dim_Product[Category] = "Electronics"
)
```

### Multiple Filters (AND Logic)

```dax
Premium Electronics 2023 = CALCULATE(
    SUM(Fact_Sales[Amount]),
    Dim_Product[Category] = "Electronics",
    Dim_Product[Tier] = "Premium",
    Dim_Date[Year] = 2023
)
```

### OR Logic Between Filters

```dax
Two Categories = CALCULATE(
    SUM(Fact_Sales[Amount]),
    Dim_Product[Category] IN {"Electronics", "Clothing"}
)
```

## Filter Removal with ALL

The ALL function removes filters, enabling totals regardless of context.

### ALL on a Column

```dax
Pct of Total = 
DIVIDE(
    SUM(Fact_Sales[Amount]),
    CALCULATE(SUM(Fact_Sales[Amount]), ALL(Dim_Product[Category]))
)
```

### ALL on a Table

```dax
Total Ignoring Products = CALCULATE(
    SUM(Fact_Sales[Amount]),
    ALL(Dim_Product)
)
```

### ALLEXCEPT

```dax
Pct of Category = 
DIVIDE(
    SUM(Fact_Sales[Amount]),
    CALCULATE(
        SUM(Fact_Sales[Amount]),
        ALLEXCEPT(Dim_Product, Dim_Product[Category])
    )
)
```

## Context Transition

When CALCULATE is evaluated in row context, it transforms that row into a filter context.

```dax
// Calculated column using a measure
CustomerSales = [Total Sales]
```

For each row, current values become filters and the measure evaluates in that context.

## Common CALCULATE Patterns

### Year-over-Year Comparison

```dax
PY Sales = CALCULATE([Total Sales], SAMEPERIODLASTYEAR(Dim_Date[Date]))

YoY Growth = DIVIDE([Total Sales] - [PY Sales], [PY Sales])
```

### Year-to-Date

```dax
YTD Sales = CALCULATE([Total Sales], DATESYTD(Dim_Date[Date]))
```

### Running Total

```dax
Running Total = CALCULATE(
    [Total Sales],
    FILTER(ALL(Dim_Date), Dim_Date[Date] <= MAX(Dim_Date[Date]))
)
```

### Filtered Ratios

```dax
Return Rate = DIVIDE(
    CALCULATE([Total Sales], Fact_Sales[IsReturn] = TRUE()),
    [Total Sales]
)
```

## KEEPFILTERS vs. Default Behavior

By default, CALCULATE filters override existing filters. KEEPFILTERS preserves existing:

```dax
// KEEPFILTERS intersects with existing context
Electronics = CALCULATE([Sales], KEEPFILTERS(Product[Category] = "Electronics"))
```

## USERELATIONSHIP

Activate inactive relationships:

```dax
Sales by Ship Date = CALCULATE(
    [Total Sales],
    USERELATIONSHIP(Dim_Date[Date], Fact_Sales[ShipDate])
)
```

## Summary

- CALCULATE modifies filter context and evaluates an expression in the new context
- Multiple filter arguments combine with AND logic
- ALL, ALLEXCEPT remove filters for totals and ratios
- Context transition converts row context to filter context
- Time intelligence patterns all use CALCULATE

## Additional Resources

- [CALCULATE Function](https://docs.microsoft.com/en-us/dax/calculate-function-dax) - Official documentation
- [DAX Patterns](https://www.daxpatterns.com/) - Common business patterns
