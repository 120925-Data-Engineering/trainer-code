# Exercise: User-Defined Functions Practice

## Overview
**Day:** 3-Wednesday  
**Duration:** 2-3 hours  
**Mode:** Individual (Code Lab)  
**Prerequisites:** Tuesday exercises completed, data loaded into Bronze tables

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Create SQL UDFs for reusable calculations
2. Create JavaScript UDFs for complex string manipulation
3. Apply UDFs in SELECT statements and views
4. Understand performance implications of different UDF types

---

## The Scenario
Your team frequently performs the same calculations across different queries. To standardize business logic and improve code maintainability, you need to create a library of reusable functions.

---

## Core Tasks

### Task 1: SQL UDFs for Business Calculations (30 mins)

Create a SQL UDF that calculates discount pricing:

```sql
USE DATABASE <YOUR_NAME>_DEV_DB;
USE SCHEMA SILVER;

-- UDF: Calculate discounted price
CREATE OR REPLACE FUNCTION CALC_DISCOUNT_PRICE(
    original_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2)
)
RETURNS DECIMAL(10,2)
LANGUAGE SQL
COMMENT = 'Calculates price after applying discount percentage'
AS
$$
    ROUND(original_price * (1 - discount_percent / 100), 2)
$$;

-- Test it
SELECT 
    CALC_DISCOUNT_PRICE(100.00, 10) AS ten_percent_off,
    CALC_DISCOUNT_PRICE(100.00, 25) AS quarter_off,
    CALC_DISCOUNT_PRICE(49.99, 15) AS sale_price;
```

**Your Tasks:**

1. Create a UDF called `CALC_TAX` that takes an amount and tax rate (default 8.25%) and returns the tax amount.

2. Create a UDF called `CALC_TOTAL_WITH_TAX` that returns the original amount plus tax.

3. Create a UDF called `CATEGORIZE_AMOUNT` that returns:
   - 'Micro' for amounts less than 50
   - 'Small' for 50-199
   - 'Medium' for 200-999
   - 'Large' for 1000+

---

### Task 2: JavaScript UDFs for String Manipulation (45 mins)

Create JavaScript UDFs for data cleansing:

```sql
-- UDF: Mask email address
CREATE OR REPLACE FUNCTION MASK_EMAIL(email STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
COMMENT = 'Masks email address for privacy (shows first 2 chars)'
AS
$$
    if (EMAIL === null || EMAIL === undefined) {
        return null;
    }
    var parts = EMAIL.split('@');
    if (parts.length !== 2) {
        return EMAIL;
    }
    var local = parts[0];
    var masked = local.substring(0, Math.min(2, local.length)) + '***@' + parts[1];
    return masked;
$$;

-- Test it
SELECT 
    MASK_EMAIL('john.doe@example.com'),
    MASK_EMAIL('a@test.org'),
    MASK_EMAIL(NULL);
```

**Your Tasks:**

1. Create a UDF called `CLEAN_PHONE` that:
   - Removes all non-digit characters
   - Formats as (XXX) XXX-XXXX if 10 digits
   - Returns original if not 10 digits

2. Create a UDF called `EXTRACT_DOMAIN` that extracts the domain from an email address (e.g., 'example.com' from 'user@example.com').

3. Create a UDF called `TITLE_CASE` that converts a string to title case (first letter of each word capitalized).

---

### Task 3: Apply UDFs to Real Data (30 mins)

Use your UDFs on actual data:

```sql
-- Apply to sample data
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;

-- Use your UDFs with customer data
SELECT 
    C_CUSTKEY,
    C_NAME,
    <YOUR_NAME>_DEV_DB.SILVER.CATEGORIZE_AMOUNT(C_ACCTBAL) AS balance_category,
    C_ACCTBAL,
    <YOUR_NAME>_DEV_DB.SILVER.CALC_TAX(C_ACCTBAL, 8.25) AS estimated_tax
FROM CUSTOMER
LIMIT 20;
```

Create a view that uses multiple UDFs:

```sql
USE DATABASE <YOUR_NAME>_DEV_DB;
USE SCHEMA SILVER;

CREATE OR REPLACE VIEW V_CUSTOMER_ENHANCED AS
SELECT 
    C_CUSTKEY AS customer_id,
    C_NAME AS customer_name,
    CATEGORIZE_AMOUNT(C_ACCTBAL) AS account_tier,
    C_ACCTBAL AS balance,
    CALC_TAX(C_ACCTBAL, 8.25) AS tax_liability,
    MASK_EMAIL(C_NAME || '@company.com') AS masked_email
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

-- Test the view
SELECT * FROM V_CUSTOMER_ENHANCED LIMIT 10;
```

---

### Task 4: UDF Performance Comparison (30 mins)

Compare SQL vs JavaScript UDF performance:

```sql
-- Create equivalent functions
CREATE OR REPLACE FUNCTION DOUBLE_SQL(x NUMBER)
RETURNS NUMBER
LANGUAGE SQL
AS $$ x * 2 $$;

CREATE OR REPLACE FUNCTION DOUBLE_JS(x NUMBER)
RETURNS NUMBER
LANGUAGE JAVASCRIPT
AS $$ return X * 2; $$;

-- Time comparison on large dataset
-- Run each and note the execution time from Query History

-- SQL UDF
SELECT DOUBLE_SQL(C_ACCTBAL) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

-- JavaScript UDF
SELECT DOUBLE_JS(C_ACCTBAL) FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;
```

Document your findings:
- Which was faster?
- By how much?
- When would you choose JavaScript over SQL?

---

### Task 5: UDF Documentation (15 mins)

Document all UDFs you created:

```sql
-- List your UDFs
SHOW USER FUNCTIONS IN SCHEMA SILVER;

-- Describe a specific UDF
DESCRIBE FUNCTION CALC_DISCOUNT_PRICE(DECIMAL, DECIMAL);
DESCRIBE FUNCTION MASK_EMAIL(STRING);
```

Create a markdown summary of your UDF library with:
- Function name
- Parameters
- Return type
- Purpose
- Example usage

---

## Deliverables

1. **SQL Script:** `udf_library.sql` containing all UDF definitions
2. **Test Script:** `udf_tests.sql` with test cases for each UDF
3. **Performance Report:** Comparison of SQL vs JavaScript UDF performance
4. **Documentation:** UDF library reference in markdown

---

## Definition of Done

- [ ] At least 3 SQL UDFs created
- [ ] At least 3 JavaScript UDFs created
- [ ] All UDFs tested with sample data
- [ ] View created using multiple UDFs
- [ ] Performance comparison documented
- [ ] UDF library documentation complete

---

## Starter Code

See `starter_code/udf_templates.sql` for function signatures to implement.

---

## Best Practices

1. **Prefer SQL UDFs** when possible - they're faster
2. **Handle NULL inputs** explicitly in JavaScript UDFs
3. **Use COMMENT** to document purpose
4. **Avoid UDFs in WHERE clauses** - they prevent predicate pushdown
5. **Test with edge cases** - NULL, empty strings, invalid formats
