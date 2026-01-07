-- UDFs in Snowflake 
-- UDF: User defined function 
-- There are a few ways to create UDFs within snowflake
-- SQL UDFs (easiest/simplest)
-- Javascript UDFs (good for complex logic OR string manipulation)
-- Python UDFs (primarily used for ML/Data science library access)

-- SQL UDF 
-- Simplest UDFs to work with, fastest - just runs the SQL against the db. 

-- Math UDF

CREATE OR REPLACE FUNCTION CENTS_TO_DOLLARS(cents NUMBER) -- name-of-function (argument TYPE)
RETURNS DECIMAL(12,2) -- RETURNS TYPE
LANGUAGE SQL -- What language are we using to define the UDF? SQL/Javascript/Python
AS -- looking like a psql UDF...
$$ 
    cents /100.0
$$;

-- Lets test it

SELECT CENTS_TO_DOLLARS(15099);

-- Multiple parameter UDF
CREATE OR REPLACE FUNCTION CALCULATE_TAX(amount DECIMAL(12,2), tax_rate DECIMAL(5,4) DEFAULT .0825)
RETURNS DECIMAL(12, 2)
LANGUAGE SQL
AS
$$
    ROUND(amount * tax_rate, 2)
$$;

-- testing the above with sample data
SELECT
    100.00 AS subtotal,
    CALCULATE_TAX(subtotal) AS tax,
    subtotal + tax AS TOTAL;


-- UDF with CASE logic
CREATE OR REPLACE FUNCTION CATEGORIZE_AMOUNT(amount DECIMAL(12, 2))
RETURNS STRING
LANGUAGE SQL 
AS 
$$
    CASE
        WHEN amount >= 10000 THEN 'Enterprise'
        WHEN amount >= 1000 THEN 'Mid-market'
        WHEN amount >= 100 THEN 'SMB'
        ELSE 'Micro'
    END
$$;

SELECT 
    O_ORDERKEY,
    O_TOTALPRICE,
    CATEGORIZE_AMOUNT(O_TOTALPRICE) AS customer_tier
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS; -- we can select across DBs and schemas by using fully qualified table names

-- Python UDF
-- Python UDFs are for using libraries like pandas and numpy
-- NOTE: Python UDFs require you to have Anaconda enabled on your Snowflake account 

CREATE OR REPLACE FUNCTION CLEAN_TEXT(text_input STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'clean_text' -- name of the python function that runs
AS 
$$ 
def clean_text(text_input):
    if text_input is None:
        return None
    
    # Remove extra whitespace, lowecase it, strip
    cleaned = ' '.join(text_input.lower().split())
    return cleaned.strip()
$$;

SELECT CLEAN_TEXT('    HELLO    WORLD     '); -- Returns: 'hello world'

-- Creating Javascript UDF
-- Better for complex string maniupulation, loops or logic
-- They use ES5 syntax (no let/const ONLY var)

-- This function will be used to mask emails
-- jonathan.delacruz@revature.com
-- jo****@revature.com
CREATE OR REPLACE FUNCTION MASK_EMAIL(email STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Note: Parameter names are uppercase in JavaScript UDFs
    if (EMAIL === null || EMAIL === undefined) {
        return null;
    }
    var parts = EMAIL.split('@');
    if (parts.length !== 2) {
        return EMAIL;  // Return as-is if not valid email format
    }
    // Mask: show first 2 chars, then ***, then @domain
    var local = parts[0];
    var masked = local.substring(0, Math.min(2, local.length)) + '***@' + parts[1];
    return masked;
$$;

-- Testing the above
SELECT MASK_EMAIL('john.doe@example.com');