# Data Modeling Levels

## Learning Objectives

- Understand the three levels of data modeling (conceptual, logical, physical)
- Know what each level includes and excludes
- Learn how to progress from one level to the next
- Recognize the purpose and audience for each level

## Why This Matters

Data modeling is the foundation of database design. Understanding the three levels helps you communicate with stakeholders at different stages of a project. Business users think conceptually, analysts think logically, and database administrators think physically. Being fluent in all three levels makes you effective across teams.

## The Concept

### The Three Levels of Data Modeling

```
+------------------+
| CONCEPTUAL MODEL |  <-- What (business concepts)
|  (High-level)    |
+--------+---------+
         |
         v
+------------------+
|  LOGICAL MODEL   |  <-- How (structure and relationships)
|   (Detailed)     |
+--------+---------+
         |
         v
+------------------+
|  PHYSICAL MODEL  |  <-- Implementation (database-specific)
| (Implementation) |
+------------------+
```

Each level adds more technical detail.

### Level 1: Conceptual Model

The conceptual model captures high-level business entities and relationships.

**Purpose:**
- Define scope of the data domain
- Communicate with business stakeholders
- Establish agreement on entities and relationships

**Includes:**
- Major business entities
- High-level relationships
- No attributes or data types

**Audience:** Business stakeholders, executives, product managers

**Example:**
```
    +----------+           +----------+
    | Customer |---orders--| Order    |
    +----------+           +----------+
         |                      |
    purchases                contains
         |                      |
    +----------+           +----------+
    | Product  |           | Order    |
    +----------+           | Item     |
                           +----------+
```

**Notation:** Often uses boxes for entities and lines for relationships. May use crow's foot for cardinality (one-to-many, etc.).

### Level 2: Logical Model

The logical model adds structure, attributes, and detailed relationships.

**Purpose:**
- Define complete data structure
- Specify attributes and their types
- Establish keys and constraints
- Technology-independent design

**Includes:**
- All entities and attributes
- Primary and foreign keys
- Cardinality and optionality
- Normalization applied
- Business rules documented

**Audience:** Data analysts, architects, development teams

**Example:**
```
Customer                    Order
+-----------------+        +------------------+
| customer_id (PK)|        | order_id (PK)    |
| first_name      |        | customer_id (FK) |
| last_name       |        | order_date       |
| email           |<------>| order_status     |
| phone           |   1:M  | total_amount     |
+-----------------+        +------------------+
                                   |
                                   | 1:M
                                   v
                           +------------------+
                           | Order_Item       |
                           +------------------+
                           | item_id (PK)     |
                           | order_id (FK)    |
                           | product_id (FK)  |
                           | quantity         |
                           | unit_price       |
                           +------------------+
```

**Cardinality Notation:**
- 1:1 - One-to-one
- 1:M - One-to-many
- M:M - Many-to-many (usually resolved into junction table)

### Level 3: Physical Model

The physical model is the implementation-specific design for a particular database system.

**Purpose:**
- Create database objects
- Optimize for performance
- Define storage and indexing

**Includes:**
- Exact table and column names
- Database-specific data types
- Indexes and constraints
- Partitioning strategies
- Storage parameters

**Audience:** DBAs, database developers

**Example (BigQuery):**
```sql
CREATE TABLE project.dataset.customers (
    customer_id INT64 NOT NULL,
    first_name STRING NOT NULL,
    last_name STRING NOT NULL,
    email STRING,
    phone STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE project.dataset.orders (
    order_id INT64 NOT NULL,
    customer_id INT64 NOT NULL,
    order_date DATE NOT NULL,
    order_status STRING NOT NULL,
    total_amount NUMERIC(10, 2)
)
PARTITION BY order_date
CLUSTER BY customer_id;

CREATE TABLE project.dataset.order_items (
    item_id INT64 NOT NULL,
    order_id INT64 NOT NULL,
    product_id INT64 NOT NULL,
    quantity INT64 NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL
);
```

### Comparison of the Three Levels

| Aspect | Conceptual | Logical | Physical |
|--------|------------|---------|----------|
| Audience | Business | Analysts | DBAs |
| Purpose | Scope | Structure | Implementation |
| Entities | Major only | All | Tables |
| Attributes | None | All | Columns with types |
| Keys | None | Defined | Implemented |
| Relationships | High-level | Detailed | FK constraints |
| Technology | Independent | Independent | Specific |
| Normalization | N/A | Applied | May denormalize |

### Progression Between Levels

```
Business Requirements
         |
         v
+------------------+
| 1. Conceptual    |  Identify: Customers, Orders, Products
+--------+---------+
         |
         | Add: Attributes, Keys, Relationships
         v
+------------------+
| 2. Logical       |  Define: customer_id (PK), order_id (PK), FK
+--------+---------+
         |
         | Add: Types, Indexes, Partitions
         v
+------------------+
| 3. Physical      |  Implement: INT64, STRING, PARTITION BY
+------------------+
         |
         v
DATABASE IMPLEMENTATION
```

### Example: E-Commerce Data Modeling

**Conceptual:**
"We need to track customers, their orders, and the products they purchase."

**Logical:**
```
Customer (customer_id PK, name, email, phone)
Order (order_id PK, customer_id FK, date, status, total)
Product (product_id PK, name, category, price)
Order_Item (item_id PK, order_id FK, product_id FK, quantity, price)

Relationships:
- Customer 1:M Order
- Order 1:M Order_Item
- Product 1:M Order_Item
```

**Physical (BigQuery):**
```sql
CREATE TABLE ecomm.customers (
    customer_id INT64 NOT NULL,
    name STRING NOT NULL,
    email STRING,
    phone STRING
);

CREATE TABLE ecomm.orders (
    order_id INT64 NOT NULL,
    customer_id INT64 NOT NULL,
    order_date DATE NOT NULL,
    status STRING,
    total NUMERIC(10, 2)
)
PARTITION BY order_date;

CREATE TABLE ecomm.products (
    product_id INT64 NOT NULL,
    name STRING NOT NULL,
    category STRING,
    price NUMERIC(10, 2)
);

CREATE TABLE ecomm.order_items (
    item_id INT64 NOT NULL,
    order_id INT64 NOT NULL,
    product_id INT64 NOT NULL,
    quantity INT64,
    unit_price NUMERIC(10, 2)
);
```

### When to Use Each Level

| Level | When to Use |
|-------|-------------|
| Conceptual | Starting a new project, stakeholder alignment, scope definition |
| Logical | Detailed design, documentation, technology selection |
| Physical | Implementation, performance tuning, database creation |

### Tools for Data Modeling

- **Conceptual/Logical:** Draw.io, Lucidchart, ERDPlus, dbdiagram.io
- **Physical:** Database-specific tools, DDL scripts, Terraform

## Summary

- Data modeling has three levels: Conceptual, Logical, Physical
- **Conceptual**: High-level business entities, no attributes, for stakeholders
- **Logical**: Complete structure with attributes and keys, technology-independent
- **Physical**: Implementation-specific with data types, indexes, and storage
- Progress from conceptual to logical to physical as design matures
- Each level serves a different audience and purpose

## Additional Resources

- [Data Modeling Concepts (Oracle)](https://docs.oracle.com/en/database/oracle/oracle-database/19/dmdug/data-modeling-concepts.html)
- [Three Levels of Data Models](https://www.lucidchart.com/pages/er-diagrams)
- [ERD Tutorial](https://www.visual-paradigm.com/guide/data-modeling/what-is-entity-relationship-diagram/)
