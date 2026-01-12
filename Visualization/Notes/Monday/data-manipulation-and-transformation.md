# Data Manipulation and Transformation in Power BI

## Learning Objectives
- Navigate the Power Query Editor interface
- Understand the M language fundamentals
- Apply common data transformation steps
- Handle data types effectively during transformation

## Why This Matters

Raw data rarely arrives in the perfect format for analysis. Column names have cryptic codes, dates appear as text, and related data lives in separate tables. Before building visualizations, you must shape and clean the data.

Power Query Editor is Power BI's built-in ETL (Extract, Transform, Load) tool. While you have used Spark and dbt for large-scale transformations in your data warehouse, Power Query handles the "last mile"---final adjustments needed for specific reports without modifying the source.

Understanding Power Query also helps you decide when transformations belong in the warehouse (dbt) versus the BI tool, a common architectural question you will encounter as a data engineer.

## Power Query Editor Overview

Power Query Editor opens when you click **Transform Data** in Power BI Desktop.

### Interface Components

| Component | Location | Purpose |
|-----------|----------|---------|
| **Queries pane** | Left | Lists all tables/queries in your model |
| **Applied Steps** | Right | Shows the sequence of transformations |
| **Data Preview** | Center | Displays sample data (first 1000 rows) |
| **Formula Bar** | Top | Shows M code for the selected step |
| **Ribbon** | Top | Transformation commands organized by category |

### The Query Processing Pipeline

Power Query processes data in a defined sequence:

```
Source Connection --> Applied Steps (transformations) --> Output to Data Model
```

Each step references the previous step, creating a chain of transformations. This approach is:
- **Non-destructive**: Source data is never modified
- **Reproducible**: Steps re-execute on every refresh
- **Auditable**: Each transformation is visible and editable

## The M Language

Behind every Power Query step is M code (officially called Power Query Formula Language). While you can perform most tasks through the UI, understanding M unlocks advanced scenarios.

### M Language Characteristics

- **Functional language**: Expressions evaluate to values, no side effects
- **Case-sensitive**: `Table.AddColumn` is different from `table.addcolumn`
- **Expression-based**: Every query is a single expression that returns a table or value

### Basic M Syntax

```m
let
    // Define intermediate steps
    Source = Snowflake.Databases("account.snowflakecomputing.com", "COMPUTE_WH"),
    
    // Navigate to database
    Database = Source{[Name="ANALYTICS"]}[Data],
    
    // Navigate to schema
    Schema = Database{[Name="GOLD"]}[Data],
    
    // Select table
    Table = Schema{[Name="DIM_CUSTOMER"]}[Data],
    
    // Apply transformations
    RenamedColumns = Table.RenameColumns(Table, {{"customer_id", "CustomerID"}}),
    ChangedTypes = Table.TransformColumnTypes(RenamedColumns, {{"CustomerID", Int64.Type}})
in
    // Return final result
    ChangedTypes
```

### Key M Concepts

| Concept | Description | Example |
|---------|-------------|---------|
| **let...in** | Defines variables and returns a result | `let x = 1, y = 2 in x + y` |
| **Records** | Key-value pairs in square brackets | `[Name="John", Age=30]` |
| **Lists** | Ordered sequences in curly braces | `{1, 2, 3}` |
| **Tables** | Structured data with rows and columns | `#table({"A", "B"}, {{1, 2}})` |
| **Functions** | Reusable transformations | `(x) => x * 2` |

## Common Transformation Steps

### Renaming Columns

Make column names user-friendly and consistent:

**UI Method:**
1. Double-click the column header
2. Type the new name
3. Press Enter

**M Code:**
```m
Table.RenameColumns(Source, {
    {"old_column_name", "New Column Name"},
    {"another_old", "Another New"}
})
```

### Changing Data Types

Ensure columns have correct types for calculations and visuals:

**UI Method:**
1. Click the data type icon in the column header
2. Select the appropriate type
3. Handle any conversion errors

**Common Type Conversions:**
| From | To | Considerations |
|------|-----|----------------|
| Text | Whole Number | Ensure no non-numeric characters |
| Text | Date | Specify the date format if ambiguous |
| Decimal | Currency | Adds currency symbol formatting |
| Date/Time | Date | Removes time component |

**M Code:**
```m
Table.TransformColumnTypes(Source, {
    {"SalesAmount", Currency.Type},
    {"OrderDate", type date},
    {"CustomerID", Int64.Type}
})
```

### Filtering Rows

Remove rows that should not be included in analysis:

**UI Method:**
1. Click the dropdown arrow in the column header
2. Uncheck values to exclude or set filter conditions
3. Use "Text Filters" or "Number Filters" for complex conditions

**M Code:**
```m
// Filter by value
Table.SelectRows(Source, each [Status] = "Active")

// Filter by multiple conditions
Table.SelectRows(Source, each [Year] >= 2020 and [Region] = "North")

// Filter nulls
Table.SelectRows(Source, each [CustomerID] <> null)
```

### Adding Calculated Columns

Create new columns based on existing data:

**UI Method:**
1. Go to **Add Column** tab
2. Choose **Custom Column**
3. Enter the formula in the dialog

**Common calculated columns:**
```m
// Concatenation
Table.AddColumn(Source, "FullName", each [FirstName] & " " & [LastName])

// Conditional logic
Table.AddColumn(Source, "SizeCategory", each 
    if [Quantity] > 100 then "Large" 
    else if [Quantity] > 50 then "Medium" 
    else "Small"
)

// Date extraction
Table.AddColumn(Source, "Year", each Date.Year([OrderDate]))
```

### Merging Tables (Joins)

Combine related tables using keys:

**UI Method:**
1. Go to **Home** tab
2. Click **Merge Queries**
3. Select the tables and matching columns
4. Choose the join type

**Join Types:**
| Type | Description | M Function |
|------|-------------|------------|
| Left Outer | All left rows, matching right | `JoinKind.LeftOuter` |
| Right Outer | All right rows, matching left | `JoinKind.RightOuter` |
| Full Outer | All rows from both tables | `JoinKind.FullOuter` |
| Inner | Only matching rows | `JoinKind.Inner` |
| Left Anti | Left rows with no match | `JoinKind.LeftAnti` |

**M Code:**
```m
Table.NestedJoin(
    Orders, {"CustomerID"},           // Left table and key
    Customers, {"CustomerID"},        // Right table and key
    "CustomerDetails",                // New column name for merged data
    JoinKind.LeftOuter               // Join type
)
```

### Appending Tables (Union)

Stack tables with matching columns:

**UI Method:**
1. Go to **Home** tab
2. Click **Append Queries**
3. Select the tables to combine

**M Code:**
```m
Table.Combine({Table1, Table2, Table3})
```

### Pivoting and Unpivoting

Reshape data between wide and tall formats:

**Unpivot (wide to tall):**
```
| Product | Jan | Feb | Mar |     | Product | Month | Sales |
|---------|-----|-----|-----|  => |---------|-------|-------|
| A       | 100 | 150 | 120 |     | A       | Jan   | 100   |
| B       | 200 | 180 | 220 |     | A       | Feb   | 150   |
                                  | A       | Mar   | 120   |
                                  | B       | Jan   | 200   |
                                  ...
```

**UI Method:**
1. Select columns to unpivot
2. Right-click and choose **Unpivot Columns**

**Pivot (tall to wide):**
1. Select the column with values to become headers
2. Go to **Transform** tab
3. Click **Pivot Column**
4. Choose the values column

### Grouping and Aggregation

Summarize data by categories:

**UI Method:**
1. Go to **Transform** tab
2. Click **Group By**
3. Select grouping columns and aggregations

**M Code:**
```m
Table.Group(Source, {"Region", "Product"}, {
    {"TotalSales", each List.Sum([Sales]), type number},
    {"AvgPrice", each List.Average([Price]), type number},
    {"RowCount", each Table.RowCount(_), Int64.Type}
})
```

## Query Folding

Query folding is when Power Query translates transformation steps into native source queries (SQL), pushing computation to the database.

### Why Query Folding Matters

- **Performance**: Database processes data faster than Power Query
- **Efficiency**: Less data transferred over the network
- **Scalability**: Works with datasets too large to load locally

### Checking for Query Folding

Right-click on a step in Applied Steps:
- **View Native Query** appears: Step folds to the source
- **View Native Query** is grayed out: Step runs locally

### Steps That Typically Fold

- Column selection and removal
- Row filtering
- Sorting
- Data type changes
- Merges (joins) when both tables are from the same source

### Steps That Break Folding

- Adding index columns
- Complex custom columns
- Pivoting/Unpivoting
- Merges across different sources
- Custom M functions

### Best Practice

Organize steps so folding steps come first:
```
Source --> Filter --> Select Columns --> (fold breaks here) --> Custom Column --> Sort
```

## Summary

- Power Query Editor provides a visual interface for data transformation with M code underneath
- Common transformations include renaming, typing, filtering, joining, and aggregating
- Query folding pushes transformations to the source database for better performance
- The let...in structure organizes M code into readable, sequential steps
- Understanding both UI and M code gives you flexibility for simple and complex scenarios

## Additional Resources

- [Power Query M Language Reference](https://docs.microsoft.com/en-us/powerquery-m/) - Official M documentation
- [Power Query Best Practices](https://docs.microsoft.com/en-us/power-query/best-practices) - Microsoft recommendations
- [Query Folding Documentation](https://docs.microsoft.com/en-us/power-query/query-folding-basics) - Deep dive into folding behavior
