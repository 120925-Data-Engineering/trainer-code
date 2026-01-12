# Introduction to DAX

## Learning Objectives
- Understand what DAX is and its role in Power BI
- Master DAX syntax fundamentals
- Differentiate between calculated columns and measures
- Grasp the concepts of row context and filter context

## Why This Matters

DAX (Data Analysis Expressions) is the formula language that transforms Power BI from a simple charting tool into a powerful analytics platform. While drag-and-drop visuals get you started, DAX enables:

- Custom business calculations (profit margins, year-over-year growth)
- Time intelligence (year-to-date, rolling averages)
- Conditional logic and complex aggregations
- Dynamic measures that respond to user selections

As a data engineer, understanding DAX helps you communicate with analysts and design data models that support their calculation needs.

## What is DAX?

DAX is a formula language originally developed for Power Pivot in Excel and now central to Power BI, Analysis Services, and Power Apps.

### DAX Characteristics

| Characteristic | Description |
|----------------|-------------|
| **Functional** | Composed of functions that take inputs and return outputs |
| **Context-aware** | Results depend on filter and row context |
| **Column-oriented** | Operates on columns, not individual cells |
| **Case-insensitive** | `SUM`, `Sum`, and `sum` are equivalent |
| **Whitespace-flexible** | Line breaks and spaces are ignored |

### DAX vs. SQL

If you are familiar with SQL, here are key differences:

| Aspect | SQL | DAX |
|--------|-----|-----|
| **Data model** | Works on normalized tables | Works on star schema models |
| **Joins** | Explicit JOINs in queries | Implicit through relationships |
| **Grouping** | Explicit GROUP BY | Automatic based on visual |
| **Context** | Query-scoped | Row and filter context |
| **Output** | Result sets | Tables or scalar values |

### DAX vs. Excel Formulas

If you know Excel, DAX will feel familiar but has important differences:

| Aspect | Excel | DAX |
|--------|-------|-----|
| **References** | Cell references (A1, B2:B10) | Column references (Table[Column]) |
| **Calculation** | Cell by cell | Entire columns at once |
| **Functions** | Many shared | Similar but not identical |
| **Recalculation** | On change | On query/refresh |

## DAX Syntax Fundamentals

### Column References

DAX references columns using the format `TableName[ColumnName]`:

```dax
Fact_Sales[Amount]
Dim_Customer[CustomerName]
Dim_Date[Year]
```

**Best practices:**
- Always qualify with the table name for clarity
- Use quotes for table/column names with spaces: `'Sales Data'[Amount]`

### Basic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `[Revenue] + [Tax]` |
| `-` | Subtraction | `[Revenue] - [Cost]` |
| `*` | Multiplication | `[Quantity] * [UnitPrice]` |
| `/` | Division | `[Revenue] / [Quantity]` |
| `^` | Exponentiation | `[Value] ^ 2` |
| `&` | Text concatenation | `[FirstName] & " " & [LastName]` |

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal to | `[Status] = "Active"` |
| `<>` | Not equal to | `[Region] <> "Unknown"` |
| `<` | Less than | `[Quantity] < 10` |
| `>` | Greater than | `[Amount] > 1000` |
| `<=` | Less than or equal | `[Date] <= TODAY()` |
| `>=` | Greater than or equal | `[Score] >= 70` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `&&` | Logical AND | `[Year] = 2023 && [Region] = "North"` |
| `\|\|` | Logical OR | `[Status] = "Active" \|\| [Status] = "Pending"` |
| `NOT` | Logical NOT | `NOT([IsReturned])` |

## Calculated Columns vs. Measures

DAX can create two types of calculations, each with distinct purposes.

### Calculated Columns

A new column added to a table, computed row by row:

**Creation:**
1. Select a table in the Fields pane
2. Go to **Table tools** > **New column**
3. Enter the DAX formula

**Example:**
```dax
FullName = Dim_Customer[FirstName] & " " & Dim_Customer[LastName]

ProfitMargin = Fact_Sales[Revenue] - Fact_Sales[Cost]

DayOfWeek = WEEKDAY(Fact_Sales[OrderDate])
```

**Characteristics:**
- Computed during data refresh
- Stored in the data model (uses memory)
- Available for filtering and grouping
- Calculated at the row level

**When to use:**
- Creating new grouping categories
- Defining relationships between tables
- Enabling filtering by computed values

### Measures

Dynamic calculations that aggregate data based on context:

**Creation:**
1. Right-click a table in the Fields pane
2. Select **New measure**
3. Enter the DAX formula

**Example:**
```dax
Total Sales = SUM(Fact_Sales[Amount])

Average Order Value = AVERAGE(Fact_Sales[Amount])

Customer Count = DISTINCTCOUNT(Fact_Sales[CustomerKey])
```

**Characteristics:**
- Computed at query time
- Not stored in the data model
- Result depends on filter context
- Cannot be used for filtering directly

**When to use:**
- Aggregating values (sum, average, count)
- Calculating ratios and percentages
- Creating time intelligence calculations
- Any dynamic business metric

### Decision Matrix

| Need | Use |
|------|-----|
| Value per row | Calculated column |
| Aggregated total | Measure |
| Filter/group by result | Calculated column |
| Dynamic based on slicers | Measure |
| Relationship key | Calculated column |
| Memory is a concern | Measure (not stored) |

## Understanding Row Context

Row context is the concept of a "current row" during evaluation.

### When Row Context Exists

- Inside calculated column formulas
- Inside iterator functions (SUMX, AVERAGEX, FILTER)
- When using row reference operators

### Calculated Column Example

```dax
// This calculated column has row context
ExtendedPrice = Fact_Sales[Quantity] * Fact_Sales[UnitPrice]
```

For each row in Fact_Sales:
- Row 1: `Quantity=5, UnitPrice=10` -> ExtendedPrice = 50
- Row 2: `Quantity=3, UnitPrice=25` -> ExtendedPrice = 75
- Row 3: `Quantity=8, UnitPrice=12` -> ExtendedPrice = 96

### No Row Context in Measures

```dax
// This measure does NOT have row context
Total = Fact_Sales[Quantity] * Fact_Sales[UnitPrice]  -- ERROR or unexpected result
```

Without row context, DAX does not know which row's Quantity and UnitPrice to use.

**Corrected version:**
```dax
Total Extended Price = SUMX(Fact_Sales, Fact_Sales[Quantity] * Fact_Sales[UnitPrice])
```

SUMX creates row context by iterating through each row.

## Understanding Filter Context

Filter context is the set of filters applied to the data when a calculation runs.

### Sources of Filter Context

As mentioned in previous sections:
- Visual groupings (rows, columns, axes)
- Slicer selections
- Visual, page, and report filters
- Row-level security

### Filter Context in Action

For a visual with:
- Year slicer: 2023
- Category on axis: Electronics, Clothing, Food

Each bar calculates `SUM(Fact_Sales[Amount])` with different filter contexts:

```
Electronics bar: Year=2023, Category=Electronics -> $150,000
Clothing bar:    Year=2023, Category=Clothing   -> $89,000
Food bar:        Year=2023, Category=Food       -> $234,000
```

The same measure formula produces different results based on context.

### CALCULATE: Modifying Filter Context

The CALCULATE function modifies filter context:

```dax
Sales 2023 = CALCULATE(SUM(Fact_Sales[Amount]), Dim_Date[Year] = 2023)
```

This measure:
1. Starts with current filter context
2. Adds/modifies filter: Year = 2023
3. Evaluates SUM in the new context

We will explore CALCULATE in depth later this week.

## Basic DAX Functions

### Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `SUM` | Total of a column | `SUM(Fact_Sales[Amount])` |
| `AVERAGE` | Mean of a column | `AVERAGE(Fact_Sales[Amount])` |
| `MIN` | Minimum value | `MIN(Fact_Sales[OrderDate])` |
| `MAX` | Maximum value | `MAX(Fact_Sales[Amount])` |
| `COUNT` | Count of non-blank values | `COUNT(Fact_Sales[Amount])` |
| `COUNTROWS` | Count of rows in a table | `COUNTROWS(Fact_Sales)` |
| `DISTINCTCOUNT` | Count of unique values | `DISTINCTCOUNT(Fact_Sales[CustomerKey])` |

### Logical Functions

| Function | Description | Example |
|----------|-------------|---------|
| `IF` | Conditional logic | `IF([Sales] > 1000, "High", "Low")` |
| `SWITCH` | Multiple conditions | `SWITCH([Grade], "A", 4, "B", 3, 0)` |
| `AND` | Both conditions true | `AND([Year]=2023, [Region]="North")` |
| `OR` | Either condition true | `OR([Status]="New", [Status]="Pending")` |

### Text Functions

| Function | Description | Example |
|----------|-------------|---------|
| `CONCATENATE` | Join two text values | `CONCATENATE([First], [Last])` |
| `LEFT` | Extract left characters | `LEFT([Code], 3)` |
| `RIGHT` | Extract right characters | `RIGHT([Phone], 4)` |
| `LEN` | Length of text | `LEN([Description])` |
| `UPPER` | Convert to uppercase | `UPPER([Name])` |

## Your First Measures

Create these measures to practice:

```dax
// Total Sales - basic aggregation
Total Sales = SUM(Fact_Sales[Amount])

// Order Count - counting rows
Order Count = COUNTROWS(Fact_Sales)

// Average Order Value - calculated metric
Average Order Value = DIVIDE([Total Sales], [Order Count], 0)

// Customer Count - distinct values
Customer Count = DISTINCTCOUNT(Fact_Sales[CustomerKey])
```

**Note on DIVIDE:** Always use `DIVIDE(numerator, denominator, alternateResult)` instead of `/` to handle division by zero gracefully.

## Summary

- DAX is Power BI's formula language for creating calculations and business logic
- Calculated columns add new columns computed at refresh time, stored in the model
- Measures compute dynamically at query time based on filter context
- Row context is the current row during iteration; filter context is the active set of filters
- CALCULATE modifies filter context for advanced calculations (covered in detail later)

## Additional Resources

- [DAX Overview](https://docs.microsoft.com/en-us/dax/dax-overview) - Microsoft DAX documentation
- [DAX Function Reference](https://docs.microsoft.com/en-us/dax/dax-function-reference) - Complete function list
- [DAX Guide](https://dax.guide/) - Community reference for DAX functions
