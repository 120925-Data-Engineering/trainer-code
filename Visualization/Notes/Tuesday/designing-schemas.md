# Designing Schemas in Power BI

## Learning Objectives
- Understand star schema design principles in Power BI
- Create and manage table relationships
- Configure cardinality and cross-filter direction correctly
- Apply model design best practices for performance

## Why This Matters

A well-designed data model is the foundation of every successful Power BI report. While Power BI can connect to any data structure, reports built on poorly modeled data suffer from slow performance, incorrect calculations, and confusing user experiences.

In Week 5, you learned dimensional modeling in Snowflake---star schemas with fact and dimension tables. Power BI embraces the same principles. Understanding how to build and optimize these models in Power BI helps you create reports that perform well and produce accurate results.

## Star Schema in Power BI

The star schema arranges data into two types of tables:

### Fact Tables
- Contain **measurable events** (transactions, clicks, shipments)
- Store **foreign keys** pointing to dimension tables
- Include **numeric measures** for aggregation (amount, quantity, cost)
- Typically the largest tables in the model
- Named with prefixes like `Fact_` or `fct_`

### Dimension Tables
- Contain **descriptive attributes** for analysis
- Store **surrogate keys** (unique identifiers)
- Provide **context** for slicing and filtering facts
- Usually smaller than fact tables
- Named with prefixes like `Dim_` or `dim_`

### Visual Representation

```
                    [Dim_Date]
                        |
                        |
[Dim_Customer] ---- [Fact_Sales] ---- [Dim_Product]
                        |
                        |
                    [Dim_Store]
```

The fact table sits at the center, connected to dimension tables arranged around it like points of a star.

## Creating Relationships

Relationships define how tables connect and how filters propagate through the model.

### Creating Relationships in Model View

1. Open **Model View** (icon on the left sidebar)
2. Drag a column from one table to the matching column in another
3. Power BI creates the relationship automatically

### Relationship Properties

When you double-click a relationship line, you can configure:

| Property | Description |
|----------|-------------|
| **Tables and columns** | The connected tables and key columns |
| **Cardinality** | One-to-many, many-to-one, one-to-one, many-to-many |
| **Cross filter direction** | Single or both directions |
| **Active/Inactive** | Whether the relationship is used by default |

## Understanding Cardinality

Cardinality defines how many rows on one side of a relationship match rows on the other side.

### One-to-Many (1:*)
The most common relationship type:
- **One** row in the dimension table
- Matches **many** rows in the fact table

**Example:** One customer can have many orders.

```
Dim_Customer (1) -------- (*) Fact_Orders
CustomerID                    CustomerID
```

### Many-to-One (*:1)
The same as one-to-many, but viewed from the opposite direction:
- **Many** rows in the fact table
- Match **one** row in the dimension table

Power BI treats 1:* and *:1 identically---the distinction is about perspective.

### One-to-One (1:1)
Each row in one table matches exactly one row in the other:
- Rare in practice
- Often indicates tables should be merged
- May suggest a design issue

**Example:** Employee table and EmployeeDetails table with matching keys.

### Many-to-Many (*:*)
Multiple rows on both sides can match:
- Requires careful handling
- Power BI supports this but with caveats
- Often indicates need for a bridge table

**Example:** Orders and Products with a many-to-many relationship (one order can have many products, one product can be in many orders). Better modeled with an OrderDetails fact table.

## Cross-Filter Direction

Cross-filter direction controls how filters flow through relationships.

### Single Direction (Recommended Default)
Filters flow from the dimension table to the fact table only:

```
[Dim_Product] --filter--> [Fact_Sales]
```

When you filter by Product Category, the filter affects Fact_Sales but not other tables beyond the fact.

### Bidirectional
Filters flow in both directions:

```
[Dim_Product] <--filter--> [Fact_Sales]
```

**Use cases for bidirectional:**
- Showing which products have sales (filtering dimension by fact)
- Bridge tables in many-to-many relationships

**Dangers of bidirectional:**
- Circular dependencies can cause ambiguity
- Performance overhead from additional filter propagation
- Unexpected filter behavior

### Recommendation

Start with single-direction filtering. Add bidirectional only when specifically needed, and test the behavior carefully.

## Relationship Best Practices

### 1. Use Integer Keys

Integer keys for relationships offer:
- Faster lookups than text keys
- Lower memory consumption
- Better compression

**Good:**
```
Dim_Customer.CustomerKey (Int) --> Fact_Sales.CustomerKey (Int)
```

**Avoid:**
```
Dim_Customer.CustomerName (Text) --> Fact_Sales.CustomerName (Text)
```

### 2. Avoid Calculated Column Keys

Relationships should be based on:
- Columns from the source data
- Not calculated columns created in Power BI

Calculated columns consume memory and slow refresh.

### 3. One Active Path Between Tables

Power BI uses **active relationships** for automatic filtering. Have only one active path between any two tables.

Multiple paths create:
- Role-playing dimensions (e.g., OrderDate, ShipDate, DeliveryDate)
- Solution: Create inactive relationships and use USERELATIONSHIP in DAX

```
Dim_Date (OrderDate) --active--> Fact_Sales
Dim_Date (ShipDate) --inactive--> Fact_Sales
Dim_Date (DeliveryDate) --inactive--> Fact_Sales
```

### 4. Flatten When Possible

Avoid unnecessary snowflaking. Power BI performs best with a true star schema:

**Snowflake (avoid when possible):**
```
Dim_Country --> Dim_Region --> Dim_Store --> Fact_Sales
```

**Star (preferred):**
```
Dim_Store (with Country, Region columns) --> Fact_Sales
```

## The Date Table

Every data model should include a dedicated date table.

### Why a Date Table is Essential

- Enables time intelligence DAX functions (YTD, PY, etc.)
- Provides consistent date attributes (month names, quarters)
- Allows fiscal calendar customization

### Creating a Date Table in Power BI

**Option 1: Power Query**
Generate a date range in Power Query:

```m
let
    StartDate = #date(2020, 1, 1),
    EndDate = #date(2025, 12, 31),
    DateList = List.Dates(StartDate, Duration.Days(EndDate - StartDate) + 1, #duration(1, 0, 0, 0)),
    DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}, null, ExtraValues.Error),
    ChangedType = Table.TransformColumnTypes(DateTable, {{"Date", type date}})
in
    ChangedType
```

**Option 2: DAX**
Create a calculated table:

```dax
Dim_Date = 
ADDCOLUMNS(
    CALENDAR(DATE(2020, 1, 1), DATE(2025, 12, 31)),
    "Year", YEAR([Date]),
    "Month", MONTH([Date]),
    "MonthName", FORMAT([Date], "MMMM"),
    "Quarter", "Q" & QUARTER([Date]),
    "DayOfWeek", WEEKDAY([Date]),
    "DayName", FORMAT([Date], "dddd")
)
```

### Marking as Date Table

After creating the date table:
1. Select the table in Model View
2. Go to **Table tools** > **Mark as date table**
3. Select the date column

This enables time intelligence functions to work correctly.

## Common Model Patterns

### Role-Playing Dimensions

When a fact table has multiple relationships to the same dimension:

```
Dim_Date -- OrderDate --> Fact_Sales
Dim_Date -- ShipDate --> Fact_Sales (inactive)
```

**Using inactive relationships in DAX:**
```dax
ShipDateSales = CALCULATE(SUM(Fact_Sales[Amount]), USERELATIONSHIP(Dim_Date[Date], Fact_Sales[ShipDate]))
```

### Bridge Tables

For true many-to-many relationships, create a bridge (junction) table:

```
Dim_Student (*) <-- StudentCourse --> (*) Dim_Course
```

The bridge table contains only the foreign keys from both dimensions.

### Slowly Changing Dimensions (Type 2)

For historical tracking, your dimension table may have multiple rows per entity:

```
CustomerKey | CustomerID | CustomerName | ValidFrom | ValidTo | IsCurrent
1           | C001       | John Doe     | 2020-01-01| 2022-06-30| No
2           | C001       | John Smith   | 2022-07-01| 9999-12-31| Yes
```

**In Power BI:**
- Use CustomerKey (surrogate) for the relationship
- Filter to IsCurrent = Yes for current-state reporting
- Keep all rows for historical analysis

## Validating Your Model

### Check for Issues

1. **Missing relationships**: Tables without connections are orphaned
2. **Circular references**: Bidirectional filters creating loops
3. **Duplicate keys**: Dimension tables should have unique keys
4. **Hidden required columns**: Ensure report developers can access what they need

### Performance Validation

Use **Performance Analyzer** to identify slow visuals:
1. Go to **View** > **Performance analyzer**
2. Click **Start recording**
3. Refresh visuals
4. Review DAX query times for each visual

## Summary

- Star schemas with fact and dimension tables form the foundation of Power BI models
- Relationships define how tables connect and how filters propagate
- Use one-to-many cardinality with single-direction filtering as the default
- Every model needs a dedicated date table for time intelligence
- Integer keys, avoiding calculated columns, and single active paths optimize performance

## Additional Resources

- [Data Modeling in Power BI](https://docs.microsoft.com/en-us/power-bi/transform-model/desktop-modeling-view) - Microsoft documentation
- [Star Schema Design](https://docs.microsoft.com/en-us/power-bi/guidance/star-schema) - Best practices guide
- [Relationships in Power BI](https://docs.microsoft.com/en-us/power-bi/transform-model/desktop-create-and-manage-relationships) - Detailed relationship guidance
