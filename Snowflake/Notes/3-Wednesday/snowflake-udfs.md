# User-Defined Functions in Snowflake

## Learning Objectives

- Understand User-Defined Functions (UDFs) and their use cases in Snowflake
- Create UDFs using SQL, JavaScript, and Python
- Apply UDFs to transform data in queries
- Recognize best practices for UDF development

## Why This Matters

While Snowflake provides extensive built-in functions, real-world data transformations often require custom logic. User-Defined Functions allow you to encapsulate reusable business logic, perform complex calculations, and extend Snowflake's capabilities. As you build your Silver and Gold layers in the Medallion architecture, UDFs help standardize transformations across your data platform.

## The Concept

### What is a UDF?

A **User-Defined Function (UDF)** is a custom function you create to extend Snowflake's built-in functionality. UDFs accept input parameters, execute logic, and return a result.

**UDF Types in Snowflake:**

| Type | Language | Use Case |
|------|----------|----------|
| **SQL UDF** | SQL | Simple calculations, query-based logic |
| **JavaScript UDF** | JavaScript | Complex logic, string manipulation, loops |
| **Python UDF** | Python | Data science, ML, library access |
| **Java UDF** | Java | Enterprise integration, existing Java code |

### SQL UDFs

SQL UDFs are the simplest and most performant. Use them for calculations that can be expressed in SQL.

**Scalar SQL UDF (returns single value):**
```sql
-- Calculate order total with tax
CREATE OR REPLACE FUNCTION calculate_order_total(
    subtotal DECIMAL(12,2),
    tax_rate DECIMAL(5,4)
)
RETURNS DECIMAL(12,2)
AS
$$
    subtotal * (1 + tax_rate)
$$;

-- Usage
SELECT 
    order_id,
    subtotal,
    calculate_order_total(subtotal, 0.0825) AS total_with_tax
FROM orders;
```

**SQL UDF with CASE Logic:**
```sql
CREATE OR REPLACE FUNCTION categorize_revenue(amount DECIMAL(12,2))
RETURNS STRING
AS
$$
    CASE 
        WHEN amount >= 10000 THEN 'Enterprise'
        WHEN amount >= 1000 THEN 'Mid-Market'
        WHEN amount >= 100 THEN 'SMB'
        ELSE 'Micro'
    END
$$;

-- Usage
SELECT 
    customer_id,
    annual_revenue,
    categorize_revenue(annual_revenue) AS segment
FROM customers;
```

**Table SQL UDF (returns table):**
```sql
CREATE OR REPLACE FUNCTION get_customer_orders(cust_id STRING)
RETURNS TABLE (order_id STRING, order_date DATE, amount DECIMAL(12,2))
AS
$$
    SELECT order_id, order_date, order_total
    FROM orders
    WHERE customer_id = cust_id
$$;

-- Usage with TABLE function
SELECT * FROM TABLE(get_customer_orders('CUST001'));
```

### JavaScript UDFs

JavaScript UDFs handle complex logic, loops, and string manipulation that are difficult in pure SQL.

**Basic JavaScript UDF:**
```sql
CREATE OR REPLACE FUNCTION format_phone_number(phone STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Remove non-numeric characters
    var digits = PHONE.replace(/\D/g, '');
    
    // Format as (XXX) XXX-XXXX
    if (digits.length === 10) {
        return '(' + digits.substring(0,3) + ') ' + 
               digits.substring(3,6) + '-' + 
               digits.substring(6);
    }
    return PHONE; // Return original if not 10 digits
$$;

-- Usage
SELECT 
    customer_id,
    raw_phone,
    format_phone_number(raw_phone) AS formatted_phone
FROM customers;
```

**JavaScript UDF with JSON Processing:**
```sql
CREATE OR REPLACE FUNCTION extract_email_domain(email STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    if (EMAIL === null || EMAIL === undefined) {
        return null;
    }
    var parts = EMAIL.split('@');
    return parts.length === 2 ? parts[1].toLowerCase() : null;
$$;
```

**JavaScript UDF with Loops:**
```sql
CREATE OR REPLACE FUNCTION calculate_compound_interest(
    principal FLOAT,
    rate FLOAT,
    years INTEGER
)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    var amount = PRINCIPAL;
    for (var i = 0; i < YEARS; i++) {
        amount = amount * (1 + RATE);
    }
    return amount;
$$;
```

**Note:** In JavaScript UDFs, parameter names are automatically converted to uppercase.

### Python UDFs

Python UDFs provide access to Python's rich ecosystem for data science and complex transformations.

**Basic Python UDF:**
```sql
CREATE OR REPLACE FUNCTION clean_text(input_text STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'clean_text_handler'
AS
$$
def clean_text_handler(input_text):
    if input_text is None:
        return None
    # Remove extra whitespace, convert to lowercase
    return ' '.join(input_text.lower().split())
$$;
```

**Python UDF with Libraries:**
```sql
CREATE OR REPLACE FUNCTION parse_json_date(date_string STRING)
RETURNS DATE
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('python-dateutil')
HANDLER = 'parse_date'
AS
$$
from dateutil import parser

def parse_date(date_string):
    if date_string is None:
        return None
    try:
        return parser.parse(date_string).date()
    except:
        return None
$$;
```

**Vectorized Python UDF (for performance):**
```sql
CREATE OR REPLACE FUNCTION vectorized_upper(texts ARRAY)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('pandas')
HANDLER = 'batch_upper'
AS
$$
import pandas as pd

def batch_upper(texts):
    series = pd.Series(texts)
    return series.str.upper().tolist()
$$;
```

### Secure UDFs

Secure UDFs hide their definition from users who have USAGE privilege but not OWNERSHIP.

```sql
CREATE OR REPLACE SECURE FUNCTION calculate_margin(
    cost DECIMAL(12,2),
    price DECIMAL(12,2)
)
RETURNS DECIMAL(5,4)
AS
$$
    (price - cost) / price
$$;
```

### UDF Overloading

Create multiple versions of a function with different parameter types:

```sql
-- Version for STRING input
CREATE OR REPLACE FUNCTION parse_amount(amount_str STRING)
RETURNS DECIMAL(12,2)
AS
$$
    TRY_TO_DECIMAL(REPLACE(REPLACE(amount_str, '$', ''), ',', ''))
$$;

-- Version for VARIANT input
CREATE OR REPLACE FUNCTION parse_amount(amount_var VARIANT)
RETURNS DECIMAL(12,2)
AS
$$
    TRY_TO_DECIMAL(amount_var::STRING)
$$;
```

### Managing UDFs

```sql
-- View function definition
DESCRIBE FUNCTION calculate_order_total(DECIMAL, DECIMAL);

-- List functions in schema
SHOW USER FUNCTIONS IN SCHEMA my_schema;

-- Drop a function
DROP FUNCTION calculate_order_total(DECIMAL, DECIMAL);

-- Grant usage
GRANT USAGE ON FUNCTION calculate_order_total(DECIMAL, DECIMAL) 
TO ROLE analyst_role;
```

### Performance Considerations

| UDF Type | Performance | Use When |
|----------|------------|----------|
| **SQL** | Fastest | Logic expressible in SQL |
| **JavaScript** | Good | Complex string/loop logic |
| **Python** | Slower | Need Python libraries, ML |

**Best Practices:**
1. Prefer SQL UDFs when possible
2. Avoid UDFs in WHERE clauses (prevents pruning)
3. Use vectorized Python UDFs for large datasets
4. Test UDFs with representative data volumes

## Summary

- **UDFs** extend Snowflake with custom reusable functions
- **SQL UDFs** are fastest and ideal for simple calculations
- **JavaScript UDFs** handle complex logic, loops, and string manipulation
- **Python UDFs** provide access to Python libraries for data science
- Use **Secure UDFs** to hide implementation details
- Choose UDF type based on performance needs and logic complexity

## Additional Resources

- [Snowflake Documentation: User-Defined Functions](https://docs.snowflake.com/en/sql-reference/user-defined-functions)
- [Snowflake Documentation: JavaScript UDFs](https://docs.snowflake.com/en/developer-guide/udf/javascript/udf-javascript-introduction)
- [Snowflake Documentation: Python UDFs](https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-introduction)
