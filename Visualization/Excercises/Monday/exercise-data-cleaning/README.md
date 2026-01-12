# Exercise: Data Cleaning with Power Query

## Overview
**Day:** 1-Monday
**Duration:** 2-3 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Power BI Desktop installed, completed interface exercise

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Data Manipulation and Transformation | [data-manipulation-and-transformation.md](../../content/1-Monday/data-manipulation-and-transformation.md) | Power Query Editor, M language basics |
| Data Cleaning Techniques | [data-cleaning-techniques.md](../../content/1-Monday/data-cleaning-techniques.md) | Handling nulls, type conversions |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Use Power Query Editor for data transformations
2. Handle null values and data quality issues
3. Standardize column formats and naming
4. Remove duplicates and fix data type errors

---

## The Scenario
Your analytics team received a dataset from a legacy system. The data has numerous quality issues that must be resolved before it can be used for reporting. Your task is to clean this "dirty" dataset using Power Query.

---

## Core Tasks

### Task 1: Load the Dirty Dataset (15 mins)

1. Open Power BI Desktop
2. Click **Home** > **Get Data** > **Text/CSV**
3. Load `starter_code/dirty_sales_data.csv`
4. **Do NOT click Load yet** - click **Transform Data** instead

**Initial Data Quality Assessment:**
Document the issues you observe:
- [ ] Inconsistent column names (casing, spaces)
- [ ] Wrong data types
- [ ] Null/empty values
- [ ] Duplicate rows
- [ ] Inconsistent text formats

**Checkpoint:** List at least 5 data quality issues.

---

### Task 2: Standardize Column Names (15 mins)

1. In Power Query Editor, rename columns to follow naming conventions:
   - Use PascalCase or snake_case consistently
   - Remove spaces and special characters
   - Make names descriptive

**Example transformations:**
| Original | Clean |
|----------|-------|
| `cust   name` | `CustomerName` |
| `ORDER.DATE` | `OrderDate` |
| `$amount` | `Amount` |

2. Right-click each column header > **Rename**
3. Or use **Transform** > **Replace Values** for bulk fixes

**Checkpoint:** All column names follow consistent naming convention.

---

### Task 3: Fix Data Types (20 mins)

1. Review each column's detected type (icon in header)
2. Change incorrect types:
   - Dates showing as Text -> Date type
   - Numbers showing as Text -> Decimal/Whole Number
   - Boolean values -> True/False type

**Steps:**
1. Click the type icon in column header
2. Select appropriate type
3. Handle any conversion errors

**Pay attention to:**
- Date formats (MM/DD/YYYY vs DD/MM/YYYY)
- Decimal separators (comma vs period)
- Null conversions

**Checkpoint:** All columns have appropriate data types.

---

### Task 4: Handle Null Values (30 mins)

Different strategies for different columns:

**Remove rows with nulls in critical fields:**
1. Select critical columns (OrderID, CustomerID)
2. **Home** > **Remove Rows** > **Remove Blanks**

**Replace nulls with defaults:**
1. Select column with optional nulls (e.g., Discount)
2. **Transform** > **Replace Values**
3. Replace `null` with `0` or appropriate default

**Fill down for repeated values:**
1. Select column with sparse data
2. **Transform** > **Fill** > **Down**

**Checkpoint:** Document your null-handling strategy for each column.

---

### Task 5: Standardize Text Values (30 mins)

Fix inconsistent text formatting:

**Case standardization:**
1. Select text column
2. **Transform** > **Format** > Choose:
   - Uppercase
   - Lowercase
   - Capitalize Each Word

**Trim whitespace:**
1. Select column
2. **Transform** > **Format** > **Trim**

**Replace inconsistent values:**
1. Example: "Active", "ACTIVE", "active" should all be "Active"
2. **Transform** > **Replace Values**
3. Create multiple replacements as needed

**Checkpoint:** All text values follow consistent formatting.

---

### Task 6: Remove Duplicates (20 mins)

1. Identify the unique key columns (e.g., OrderID)
2. Select key column(s)
3. **Home** > **Remove Rows** > **Remove Duplicates**

OR for keeping specific versions:

1. Sort data appropriately (e.g., by date descending)
2. Then remove duplicates (keeps first occurrence)

**Verify:**
- Row count before: ___
- Row count after: ___
- Duplicates removed: ___

**Checkpoint:** No duplicate records based on primary key.

---

### Task 7: Create Calculated Columns (30 mins)

Add useful derived columns:

**Total Amount:**
1. **Add Column** > **Custom Column**
2. Formula: `[Quantity] * [UnitPrice] * (1 - [Discount])`
3. Name: `TotalAmount`

**Year Extracted:**
1. Select date column
2. **Add Column** > **Date** > **Year**

**Status Flag:**
1. **Add Column** > **Conditional Column**
2. Create logic for "IsCompleted" based on Status

**Checkpoint:** At least 2 calculated columns created.

---

### Task 8: Review and Apply (15 mins)

1. Review the **Applied Steps** pane
   - Each transformation is recorded
   - Steps can be reordered or deleted
   
2. Click **Close & Apply**

3. In Data View, verify:
   - All transformations applied correctly
   - Data types are as expected
   - Row counts are correct

**Checkpoint:** Clean data loaded into Power BI model.

---

## Deliverables

Submit the following:

1. **Power BI File (.pbix):** With cleaned data
2. **Before/After Screenshot:** Showing data improvement
3. **Cleaning Log:** Document all transformations applied in order
4. **M Code Export:** Copy the generated M code from Advanced Editor

---

## Definition of Done

- [ ] All column names standardized
- [ ] Data types corrected for all columns
- [ ] Null values handled appropriately
- [ ] Text values standardized
- [ ] Duplicates removed
- [ ] At least 2 calculated columns created
- [ ] Cleaning log documented
- [ ] Data loads successfully into model

---

## Stretch Goals (Optional)

1. Create a reference query for reusable transformations
2. Handle errors gracefully with try/otherwise in M
3. Parameterize the file path for flexibility

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Type conversion error | Check source data format, may need Replace first |
| Replace not finding values | Check for hidden whitespace, use Trim first |
| Duplicate removal not working | Ensure correct columns selected |
| M formula error | Check column name spelling, use column picker |
| Performance slow | Reduce data with filters before complex transforms |
