# DAX Aggregation and Statistics Functions

## Learning Objectives
- Master standard aggregation functions (SUM, AVERAGE, COUNT)
- Apply statistical functions for data analysis
- Use iterator functions (SUMX, AVERAGEX) for row-by-row calculations
- Understand when to use each type of function

## Why This Matters

Aggregation is at the heart of business intelligence. Every dashboard metric---total sales, average response time, customer count---relies on aggregation functions. Power BI's DAX provides a rich set of functions that go beyond simple totals to enable sophisticated statistical analysis.

Understanding the difference between simple aggregators (SUM) and iterators (SUMX) is crucial. Chose incorrectly and your calculations will be wrong or inefficient. This knowledge helps you build accurate, performant measures.

## Standard Aggregation Functions

These functions aggregate an entire column within the current filter context.

### SUM

Returns the sum of all values in a column.

```dax
Total Revenue = SUM(Fact_Sales[Revenue])

Total Cost = SUM(Fact_Sales[Cost])
```

**Behavior:**
- Ignores blanks and nulls
- Works only on numeric columns
- Respects filter context (slicers, visual groupings)

### AVERAGE

Returns the arithmetic mean of a column.

```dax
Average Order Value = AVERAGE(Fact_Sales[OrderAmount])

Average Temperature = AVERAGE(Weather[Temperature])
```

**Behavior:**
- Ignores blanks
- Sum divided by count of non-blank values
- Beware: blanks vs. zeros treated differently

### MIN and MAX

Return the smallest or largest value.

```dax
First Order Date = MIN(Fact_Sales[OrderDate])

Highest Sale = MAX(Fact_Sales[Amount])

Lowest Price = MIN(Dim_Product[UnitPrice])
```

**Works with:**
- Numbers
- Dates
- Text (alphabetical order)

### COUNT Functions

Multiple functions for counting, each with specific behavior:

| Function | Counts | Includes Blanks |
|----------|--------|-----------------|
| `COUNT` | Values in a column | No |
| `COUNTA` | Non-blank values (any type) | No |
| `COUNTBLANK` | Blank values | Yes (only blanks) |
| `COUNTROWS` | Rows in a table | N/A |
| `DISTINCTCOUNT` | Unique values | No |

**Examples:**
```dax
// Count of orders with an amount
Orders With Amount = COUNT(Fact_Sales[Amount])

// Count all non-blank values in any column
Non Blank Entries = COUNTA(Fact_Sales[CustomerID])

// Count missing values
Missing Amounts = COUNTBLANK(Fact_Sales[Amount])

// Total rows in the table
Total Records = COUNTROWS(Fact_Sales)

// Unique customers who made purchases
Unique Customers = DISTINCTCOUNT(Fact_Sales[CustomerKey])
```

### DISTINCTCOUNT vs. COUNTROWS

A common question: when each?

```dax
// How many sales transactions?
Transaction Count = COUNTROWS(Fact_Sales)

// How many unique customers made transactions?
Customer Count = DISTINCTCOUNT(Fact_Sales[CustomerKey])

// How many unique products were sold?
Product Count = DISTINCTCOUNT(Fact_Sales[ProductKey])
```

## Statistical Functions

DAX provides functions for statistical analysis.

### Standard Deviation and Variance

Measure data spread:

```dax
// Sample standard deviation (n-1 denominator)
Sales StdDev = STDEV.S(Fact_Sales[Amount])

// Population standard deviation (n denominator)
Sales StdDev Pop = STDEV.P(Fact_Sales[Amount])

// Sample variance
Sales Variance = VAR.S(Fact_Sales[Amount])

// Population variance
Sales Variance Pop = VAR.P(Fact_Sales[Amount])
```

**When to use which:**
- **Sample (.S)**: Your data is a sample of a larger population
- **Population (.P)**: Your data includes the entire population

### Percentiles and Medians

Find distribution points:

```dax
// Median (50th percentile)
Median Sales = MEDIAN(Fact_Sales[Amount])

// 25th percentile (Q1)
Q1 Sales = PERCENTILE.INC(Fact_Sales[Amount], 0.25)

// 75th percentile (Q3)
Q3 Sales = PERCENTILE.INC(Fact_Sales[Amount], 0.75)

// 90th percentile
P90 Sales = PERCENTILE.INC(Fact_Sales[Amount], 0.90)
```

**PERCENTILE.INC vs. PERCENTILE.EXC:**
- `.INC`: Inclusive (0 to 1)
- `.EXC`: Exclusive (0 < k < 1)

### Other Statistical Functions

```dax
// Geometric mean (for growth rates)
Avg Growth Rate = GEOMEAN(Fact_Sales[GrowthFactor])

// Range
Sales Range = MAX(Fact_Sales[Amount]) - MIN(Fact_Sales[Amount])

// Mode (most frequent value) - no built-in, requires custom logic
```

## Iterator Functions (The "X" Functions)

Iterator functions evaluate an expression row by row, then aggregate the results. They are named with an "X" suffix.

### SUMX

Iterates through a table, evaluates an expression per row, then sums the results.

**Syntax:**
```dax
SUMX(<table>, <expression>)
```

**Example: Extended Price**
```dax
// When you need to multiply columns before summing
Total Extended Price = SUMX(Fact_Sales, Fact_Sales[Quantity] * Fact_Sales[UnitPrice])
```

**How SUMX works:**

| Row | Quantity | UnitPrice | Expression Result |
|-----|----------|-----------|-------------------|
| 1 | 5 | 10 | 50 |
| 2 | 3 | 25 | 75 |
| 3 | 8 | 12 | 96 |
| **Sum** | | | **221** |

**Why not use a calculated column?**

If `ExtendedPrice` is a calculated column:
```dax
Total = SUM(Fact_Sales[ExtendedPrice])  -- Works, but stores data

Total = SUMX(Fact_Sales, Fact_Sales[Quantity] * Fact_Sales[UnitPrice])  -- Computed on demand
```

Both produce the same result, but SUMX:
- Does not consume storage
- Can use measure references
- Allows more complex expressions

### AVERAGEX

Iterates, evaluates, then averages:

```dax
// Average line revenue
Avg Line Revenue = AVERAGEX(Fact_Sales, Fact_Sales[Quantity] * Fact_Sales[UnitPrice])

// Average order total (group by order first)
Avg Order Total = AVERAGEX(
    VALUES(Fact_Sales[OrderID]),
    CALCULATE(SUM(Fact_Sales[Amount]))
)
```

### MINX and MAXX

Find minimum or maximum of an expression:

```dax
// Smallest line total
Min Line Total = MINX(Fact_Sales, Fact_Sales[Quantity] * Fact_Sales[UnitPrice])

// Latest order amount
Last Order Amount = MAXX(
    TOPN(1, Fact_Sales, Fact_Sales[OrderDate], DESC),
    Fact_Sales[Amount]
)
```

### COUNTX

Count rows where an expression is not blank:

```dax
// Count of profitable lines
Profitable Lines = COUNTX(
    Fact_Sales,
    IF(Fact_Sales[Revenue] > Fact_Sales[Cost], 1, BLANK())
)
```

### RANKX

Rank values within a context:

```dax
// Rank products by sales
Product Rank = RANKX(
    ALL(Dim_Product),
    [Total Sales],
    ,
    DESC,
    DENSE
)
```

**Parameters:**
1. Table to rank over
2. Expression to rank by
3. Value (optional, defaults to current)
4. Order (ASC or DESC)
5. Ties handling (DENSE or SKIP)

## Aggregation Over Related Tables

When your measure needs data from a related table:

```dax
// Sum from related table
Total Customer Sales = SUMX(
    Dim_Customer,
    CALCULATE(SUM(Fact_Sales[Amount]))
)

// Count with related filter
Premium Customer Count = COUNTROWS(
    FILTER(Dim_Customer, Dim_Customer[Tier] = "Premium")
)
```

## FILTER Function

FILTER returns a table with rows matching a condition. Often used with aggregations:

```dax
// Count high-value orders
High Value Orders = COUNTROWS(
    FILTER(Fact_Sales, Fact_Sales[Amount] > 1000)
)

// Sum sales for specific category
Electronics Sales = SUMX(
    FILTER(Fact_Sales, RELATED(Dim_Product[Category]) = "Electronics"),
    Fact_Sales[Amount]
)
```

## Combining Functions

Real-world measures often combine multiple functions:

```dax
// Weighted average price
Weighted Avg Price = 
DIVIDE(
    SUMX(Fact_Sales, Fact_Sales[Quantity] * Fact_Sales[UnitPrice]),
    SUM(Fact_Sales[Quantity]),
    0
)

// Percentage of high-value orders
Pct High Value = 
DIVIDE(
    COUNTROWS(FILTER(Fact_Sales, Fact_Sales[Amount] > 1000)),
    COUNTROWS(Fact_Sales),
    0
) * 100

// Interquartile range (IQR)
IQR = 
PERCENTILE.INC(Fact_Sales[Amount], 0.75) - 
PERCENTILE.INC(Fact_Sales[Amount], 0.25)
```

## Performance Considerations

### Simple Aggregators vs. Iterators

| Aspect | SUM, AVERAGE, etc. | SUMX, AVERAGEX, etc. |
|--------|--------------------|-----------------------|
| **Performance** | Very fast (Storage Engine) | Slower (Formula Engine may be involved) |
| **Memory** | Minimal | Iterates through rows |
| **When to use** | Single column aggregation | Per-row calculations needed |

### Best Practices

1. **Prefer simple aggregators** when possible:
   ```dax
   -- If ExtendedPrice column exists, use SUM
   Total = SUM(Fact_Sales[ExtendedPrice])
   
   -- Not SUMX when unnecessary
   Total = SUMX(Fact_Sales, Fact_Sales[ExtendedPrice])  -- Slower
   ```

2. **Use SUMX for row-level math** when you cannot pre-compute:
   ```dax
   -- When calculation depends on measure logic
   Total = SUMX(Fact_Sales, Fact_Sales[Quantity] * [Current Price])
   ```

3. **Be mindful of nested iterators**:
   ```dax
   -- This iterates through Customer, then Sales for each - can be slow
   Metric = SUMX(Dim_Customer, SUMX(RELATEDTABLE(Fact_Sales), ...) )
   ```

## Summary

- Standard aggregations (SUM, AVERAGE, COUNT) work on columns within filter context
- Statistical functions (STDEV, PERCENTILE, MEDIAN) enable advanced analysis
- Iterator functions (SUMX, AVERAGEX) evaluate expressions row by row before aggregating
- DISTINCTCOUNT counts unique values; COUNTROWS counts rows
- FILTER creates filtered tables for use with other aggregations
- Choose simple aggregators for performance; use iterators when row-level calculation is required

## Additional Resources

- [Aggregation Functions](https://docs.microsoft.com/en-us/dax/aggregation-functions-dax) - DAX aggregation reference
- [Statistical Functions](https://docs.microsoft.com/en-us/dax/statistical-functions-dax) - Statistical function reference
- [SQLBI - Iterator Functions](https://www.sqlbi.com/articles/understanding-the-filter-function-in-dax/) - Deep dive on iterators
