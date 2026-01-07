# Dimensional Modeling Introduction

## Learning Objectives

- Understand what dimensional modeling is and its origins
- Learn the core components: facts and dimensions
- Know the benefits of dimensional modeling for analytics
- Appreciate the Kimball methodology

## Why This Matters

Dimensional modeling is the standard approach for designing data warehouses. It creates structures that are intuitive for business users and optimized for analytical queries. Understanding dimensional modeling is essential for building effective BI solutions.

## The Concept

### What is Dimensional Modeling?

Dimensional modeling is a design technique for data warehouses that organizes data around business processes and measurements.

**Key Principles:**
- Organize by business process (sales, inventory, etc.)
- Separate facts (measurements) from dimensions (context)
- Optimize for queries, not transactions
- Make data understandable to business users

### History and Origins

**Ralph Kimball** pioneered dimensional modeling in the 1990s:
- "The Data Warehouse Toolkit" (1996)
- Bottom-up approach: build data marts first
- Focus on usability and query performance
- Widely adopted standard for BI

### Core Components

Dimensional models consist of two types of tables:

```
+----------------+
|   DIMENSIONS   |  WHO, WHAT, WHEN, WHERE
| (Context)      |
+-------+--------+
        |
        v
+-------+--------+
|     FACTS      |  HOW MUCH, HOW MANY
| (Measurements) |
+----------------+
```

### Fact Tables

Fact tables contain the measurable, quantitative data about a business process.

**Characteristics:**
- Contain numeric measures (metrics)
- Have foreign keys to dimension tables
- Often the largest tables (millions/billions of rows)
- One row per event/transaction

**Example: Sales Fact Table**
```sql
fact_sales
+---------+----------+----------+---------+--------+-------+-------+
| date_key| cust_key | prod_key | qty     | price  | cost  | profit|
+---------+----------+----------+---------+--------+-------+-------+
| 20240615| 101      | 501      | 2       | 29.99  | 15.00 | 14.99 |
| 20240615| 102      | 502      | 1       | 49.99  | 25.00 | 24.99 |
| 20240616| 101      | 503      | 3       | 19.99  | 10.00 | 9.99  |
+---------+----------+----------+---------+--------+-------+-------+
       ^         ^         ^         ^        ^        ^        ^
       |         |         |         |        |        |        |
      FK        FK        FK      Measure  Measure  Measure  Measure
```

**Types of Facts:**
| Type | Description | Example |
|------|-------------|---------|
| Additive | Can be summed across any dimension | Revenue, Quantity |
| Semi-additive | Can be summed across some dimensions | Account Balance |
| Non-additive | Cannot be summed | Ratios, Percentages |

### Dimension Tables

Dimension tables contain descriptive attributes that provide context to facts.

**Characteristics:**
- Contain descriptive text attributes
- Have a primary key (often surrogate)
- Relatively smaller (thousands to millions of rows)
- Wide tables with many columns

**Example: Customer Dimension**
```sql
dim_customer
+------+-------+----------+--------+--------+----------+-------------+
| key  | name  | email    | city   | state  | country  | segment     |
+------+-------+----------+--------+--------+----------+-------------+
| 101  | Alice | a@mail   | NYC    | NY     | USA      | Premium     |
| 102  | Bob   | b@mail   | LA     | CA     | USA      | Standard    |
| 103  | Carol | c@mail   | London | --     | UK       | Premium     |
+------+-------+----------+--------+--------+----------+-------------+
   ^
   |
  PK (surrogate key)
```

**Common Dimensions:**
- **Date**: Day, month, quarter, year, holiday flag
- **Customer**: Name, address, segment, demographics
- **Product**: Name, category, brand, specifications
- **Location/Geography**: Store, region, country
- **Employee**: Name, department, role

### The Star Schema

When fact and dimension tables are joined, they form a star shape:

```
                    +----------------+
                    |   dim_date     |
                    +-------+--------+
                            |
+----------------+   +------+-------+   +----------------+
|  dim_customer  |---| fact_sales   |---| dim_product    |
+----------------+   +------+-------+   +----------------+
                            |
                    +-------+--------+
                    |  dim_store     |
                    +----------------+
```

We will explore star schemas in detail in the next topic.

### Why Dimensional Modeling Works

**For Business Users:**
- Intuitive structure matches how people think
- Easy to understand: "sales by customer by product by date"
- Self-service BI friendly

**For Query Performance:**
- Fewer joins than normalized models
- Predictable query patterns
- Easy to aggregate

**For BI Tools:**
- Works well with Tableau, Power BI, Looker
- Simple star join patterns
- Drill-down and roll-up natural

### Dimensional Modeling Process

**Step 1: Choose the Business Process**
- What are we measuring? (Sales, Orders, Inventory)

**Step 2: Declare the Grain**
- What does one row represent? (One sale, one day, one transaction)

**Step 3: Identify the Dimensions**
- Who, what, where, when, how? (Customer, Product, Store, Date)

**Step 4: Identify the Facts**
- What are we measuring? (Quantity, Revenue, Cost)

**Example:**
```
Business Process: Retail Sales
Grain: One row per line item on a sales receipt
Dimensions: Date, Customer, Product, Store, Promotion
Facts: Quantity sold, Unit price, Discount, Total amount
```

### Grain: The Foundation

The grain defines the level of detail in a fact table.

**Examples of Grain:**
| Fact Table | Grain |
|------------|-------|
| Sales | One row per line item |
| Daily Inventory | One row per product per store per day |
| Monthly Summary | One row per customer per month |

**Rule:** Choose the most atomic grain possible. You can always aggregate up, but you cannot disaggregate down.

### Facts and Grains in Practice

```
Atomic Grain (finest detail):
+--------+----------+----------+-----+--------+
| sale_id| product  | customer | qty | amount |
+--------+----------+----------+-----+--------+
| 001    | Widget   | Alice    | 2   | 20.00  |
| 002    | Gadget   | Alice    | 1   | 15.00  |
| 003    | Widget   | Bob      | 3   | 30.00  |
+--------+----------+----------+-----+--------+

Aggregated (derived from atomic):
+----------+--------+-----------+
| customer | month  | total_amt |
+----------+--------+-----------+
| Alice    | Jun    | 35.00     |
| Bob      | Jun    | 30.00     |
+----------+--------+-----------+
```

## Summary

- Dimensional modeling organizes data around business processes
- **Fact tables** contain measurements (numeric, additive)
- **Dimension tables** contain context (descriptive attributes)
- The grain defines what one row in the fact table represents
- Kimball methodology: identify process, grain, dimensions, then facts
- Dimensional models are intuitive for users and optimized for queries
- The star schema is the most common dimensional model structure

## Additional Resources

- [The Data Warehouse Toolkit (Kimball)](https://www.kimballgroup.com/)
- [Dimensional Modeling Techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Fact Table Design Tips](https://www.kimballgroup.com/2003/01/the-10-essential-rules-of-dimensional-modeling/)
