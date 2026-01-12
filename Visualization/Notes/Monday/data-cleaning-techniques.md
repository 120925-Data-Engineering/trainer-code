# Data Cleaning Techniques in Power BI

## Learning Objectives
- Handle null values and missing data appropriately
- Remove duplicate records using various strategies
- Split and merge columns for data normalization
- Implement error handling in data transformations
- Validate data quality within Power Query

## Why This Matters

Data quality directly impacts trust in analytics. Users quickly lose confidence in dashboards that show impossible values, missing data, or duplicate records. While your enterprise data pipelines in Snowflake and dbt should handle most data quality issues, the "last mile" of data preparation often falls to the BI layer.

Power Query provides tools to clean data that arrives imperfect, whether from legacy systems, manual uploads, or edge cases the upstream pipeline does not handle. Knowing these techniques helps you advise when cleaning belongs in the warehouse versus the BI tool.

## Handling Null Values

Null values (missing data) require careful treatment to avoid misleading analyses.

### Identifying Nulls

Power Query displays nulls as **null** in the data preview. To find all nulls in a column:

1. Click the dropdown arrow in the column header
2. Observe the **(null)** option in the filter list
3. The count beside it shows how many null values exist

### Strategies for Handling Nulls

| Strategy | When to Use | M Function |
|----------|-------------|------------|
| **Remove rows** | Nulls represent invalid records | `Table.SelectRows` |
| **Replace with value** | Default value makes sense | `Table.ReplaceValue` |
| **Replace with calculated value** | Use another column's data | `Table.ReplaceValue` |
| **Keep as null** | Analysis handles nulls correctly | No action needed |

### Replacing Null Values

**UI Method:**
1. Select the column
2. Go to **Transform** tab
3. Click **Replace Values**
4. Enter `null` in "Value To Find" and the replacement in "Replace With"

**M Code Examples:**
```m
// Replace nulls with a fixed value
Table.ReplaceValue(Source, null, 0, Replacer.ReplaceValue, {"Quantity"})

// Replace nulls with text
Table.ReplaceValue(Source, null, "Unknown", Replacer.ReplaceValue, {"Category"})

// Replace nulls with column average
let
    AvgValue = List.Average(Table.Column(Source, "Price")),
    Replaced = Table.ReplaceValue(Source, null, AvgValue, Replacer.ReplaceValue, {"Price"})
in
    Replaced

// Fill down (use previous non-null value)
Table.FillDown(Source, {"CustomerName"})

// Fill up (use next non-null value)
Table.FillUp(Source, {"CustomerName"})
```

### Handling Nulls in Calculations

When creating calculated columns, explicitly handle nulls:

```m
Table.AddColumn(Source, "TotalPrice", each 
    if [Quantity] = null or [UnitPrice] = null then null
    else [Quantity] * [UnitPrice]
)
```

## Removing Duplicates

Duplicate records can inflate counts and skew aggregations.

### Understanding Duplicate Types

| Type | Description | Example |
|------|-------------|---------|
| **Exact duplicates** | All columns identical | Same row entered twice |
| **Key duplicates** | Primary key repeated | Multiple orders with same OrderID |
| **Logical duplicates** | Same entity, different values | Customer with updated address |

### Removing Exact Duplicates

**UI Method:**
1. Select all columns (Ctrl+A in column headers)
2. Go to **Home** tab
3. Click **Remove Rows** > **Remove Duplicates**

**M Code:**
```m
Table.Distinct(Source)
```

### Removing Duplicates by Key

Keep the first occurrence based on specific columns:

**UI Method:**
1. Select the key column(s)
2. Go to **Home** tab
3. Click **Remove Rows** > **Remove Duplicates**

**M Code:**
```m
// Remove duplicates based on CustomerID
Table.Distinct(Source, {"CustomerID"})
```

### Keeping Specific Duplicates

When you need to keep the most recent or a specific duplicate:

```m
// Sort by date descending, then remove duplicates (keeps most recent)
let
    Sorted = Table.Sort(Source, {{"UpdatedDate", Order.Descending}}),
    Deduped = Table.Distinct(Sorted, {"CustomerID"})
in
    Deduped
```

## Splitting and Merging Columns

### Splitting Columns

Break a single column into multiple columns:

**Split by Delimiter:**
```
"John, Doe" --> "John" (FirstName), "Doe" (LastName)
```

**UI Method:**
1. Select the column
2. Go to **Transform** > **Split Column**
3. Choose **By Delimiter**
4. Specify the delimiter (comma, space, etc.)

**M Code:**
```m
Table.SplitColumn(Source, "FullName", 
    Splitter.SplitTextByDelimiter(", ", QuoteStyle.None), 
    {"FirstName", "LastName"}
)
```

**Split by Position:**
```
"20231215" --> "2023" (Year), "12" (Month), "15" (Day)
```

**M Code:**
```m
Table.SplitColumn(Source, "DateString",
    Splitter.SplitTextByPositions({0, 4, 6}),
    {"Year", "Month", "Day"}
)
```

### Merging Columns

Combine multiple columns into one:

**UI Method:**
1. Select multiple columns (Ctrl+Click)
2. Go to **Transform** > **Merge Columns**
3. Choose a separator

**M Code:**
```m
Table.CombineColumns(Source, {"FirstName", "LastName"}, 
    Combiner.CombineTextByDelimiter(" ", QuoteStyle.None), 
    "FullName"
)
```

### Extracting Values

Extract specific parts from text:

```m
// Extract first N characters
Table.TransformColumns(Source, {{"PostalCode", each Text.Start(_, 5)}})

// Extract last N characters
Table.TransformColumns(Source, {{"PhoneNumber", each Text.End(_, 4)}})

// Extract between delimiters
Table.TransformColumns(Source, {{"Email", each Text.BetweenDelimiters(_, "@", ".")}})

// Extract using regular expressions (requires custom function)
Table.TransformColumns(Source, {{"SKU", each Text.Select(_, {"0".."9"})}})
```

## Error Handling

Transformation errors can occur when data does not match expected formats.

### Understanding Error Types

| Error | Cause | Example |
|-------|-------|---------|
| **Expression.Error** | Invalid expression | Dividing by zero |
| **DataFormat.Error** | Type conversion failure | "abc" to number |
| **DataSource.Error** | Connection issues | Database timeout |

### Viewing Errors

When a step produces errors:
1. Affected cells display **Error** in red
2. Click **Error** to see the error message
3. The column header shows an error indicator

### Replacing Errors

**UI Method:**
1. Select the column with errors
2. Go to **Transform** tab
3. Click **Replace Errors**
4. Enter a replacement value

**M Code:**
```m
// Replace errors with null
Table.ReplaceErrorValues(Source, {{"SalesAmount", null}})

// Replace errors with a default value
Table.ReplaceErrorValues(Source, {{"SalesAmount", 0}})

// Replace errors in multiple columns
Table.ReplaceErrorValues(Source, {
    {"SalesAmount", 0},
    {"Quantity", 0},
    {"CustomerName", "Unknown"}
})
```

### Removing Error Rows

Remove rows that contain any errors:

**UI Method:**
1. Go to **Home** tab
2. Click **Remove Rows** > **Remove Errors**

**M Code:**
```m
Table.RemoveRowsWithErrors(Source)

// Remove errors from specific columns only
Table.RemoveRowsWithErrors(Source, {"SalesAmount", "Quantity"})
```

### Try-Otherwise Pattern

Prevent errors from occurring using try-otherwise:

```m
Table.AddColumn(Source, "SafeDivision", each 
    try [Revenue] / [Quantity] otherwise null
)
```

## Data Quality Validation

Beyond fixing issues, verify data meets expectations.

### Checking Value Ranges

Ensure numeric values fall within expected ranges:

```m
// Flag out-of-range values
Table.AddColumn(Source, "PriceValid", each 
    [UnitPrice] >= 0 and [UnitPrice] <= 10000
)

// Filter to only valid ranges
Table.SelectRows(Source, each [Quantity] > 0 and [Quantity] <= 1000)
```

### Validating Text Patterns

Check text columns match expected formats:

```m
// Validate email format (basic)
Table.AddColumn(Source, "EmailValid", each 
    Text.Contains([Email], "@") and Text.Contains([Email], ".")
)

// Validate phone number length
Table.AddColumn(Source, "PhoneValid", each 
    Text.Length(Text.Select([Phone], {"0".."9"})) = 10
)
```

### Checking Referential Integrity

Verify foreign keys exist in reference tables:

```m
// Get list of valid CustomerIDs
let
    ValidCustomers = Table.Column(Dim_Customer, "CustomerID"),
    
    // Flag orders with invalid customer references
    Validated = Table.AddColumn(Fact_Orders, "CustomerExists", each 
        List.Contains(ValidCustomers, [CustomerID])
    )
in
    Validated
```

### Creating Data Quality Reports

Summarize data quality issues:

```m
let
    Source = YourTable,
    NullCounts = Table.FromRecords({
        [Column = "CustomerID", NullCount = List.Count(List.Select(Table.Column(Source, "CustomerID"), each _ = null))],
        [Column = "OrderDate", NullCount = List.Count(List.Select(Table.Column(Source, "OrderDate"), each _ = null))],
        [Column = "Amount", NullCount = List.Count(List.Select(Table.Column(Source, "Amount"), each _ = null))]
    })
in
    NullCounts
```

## Standardizing Data

### Trimming Whitespace

Remove leading, trailing, or extra spaces:

**UI Method:**
1. Select the column
2. Go to **Transform** tab
3. Click **Format** > **Trim**

**M Code:**
```m
// Trim leading and trailing spaces
Table.TransformColumns(Source, {{"CustomerName", Text.Trim}})

// Clean all whitespace (including non-breaking spaces)
Table.TransformColumns(Source, {{"CustomerName", Text.Clean}})
```

### Changing Case

Standardize text capitalization:

```m
// Proper case (first letter capitalized)
Table.TransformColumns(Source, {{"CustomerName", Text.Proper}})

// Upper case
Table.TransformColumns(Source, {{"CountryCode", Text.Upper}})

// Lower case
Table.TransformColumns(Source, {{"Email", Text.Lower}})
```

### Replacing Values

Standardize variations:

```m
// Map variations to standard values
Table.ReplaceValue(Source, "USA", "United States", Replacer.ReplaceText, {"Country"})

// Replace multiple variations
let
    Step1 = Table.ReplaceValue(Source, "USA", "United States", Replacer.ReplaceText, {"Country"}),
    Step2 = Table.ReplaceValue(Step1, "US", "United States", Replacer.ReplaceText, {"Country"}),
    Step3 = Table.ReplaceValue(Step2, "U.S.A.", "United States", Replacer.ReplaceText, {"Country"})
in
    Step3
```

## Summary

- Handle nulls strategically based on business context: replace, remove, or keep
- Remove duplicates using entire rows or specific key columns
- Split combined data into normalized columns; merge columns for display purposes
- Use error handling to gracefully manage type conversion and calculation failures
- Validate data quality by checking ranges, patterns, and referential integrity
- Standardize text with trimming, case changes, and value replacements

## Additional Resources

- [Data Cleaning in Power Query](https://docs.microsoft.com/en-us/power-query/data-cleaning-transformations) - Microsoft documentation
- [Handling Errors in Power Query](https://docs.microsoft.com/en-us/power-query/dealing-with-errors) - Error handling patterns
- [Power Query Best Practices](https://docs.microsoft.com/en-us/power-query/best-practices) - Performance and quality guidelines
