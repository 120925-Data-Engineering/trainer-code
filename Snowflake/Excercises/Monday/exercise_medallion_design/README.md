# Exercise: Medallion Architecture Design

## Overview
**Day:** 1-Monday  
**Duration:** 2-3 hours  
**Mode:** Individual (Conceptual - Design Challenge)  
**Prerequisites:** Completed Snowflake Exploration exercise

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Design a Medallion architecture (Bronze/Silver/Gold) for a real business scenario
2. Define data quality progression across layers
3. Identify appropriate transformations at each layer
4. Document data flow using diagrams

---

## The Scenario

You are the lead data engineer for **TechMart**, an e-commerce company that sells electronics online. The company collects data from multiple sources:

- **Web Events:** Clickstream data from the website (JSON format)
- **Orders:** Transaction data from the order management system (CSV exports)
- **Products:** Product catalog from the inventory system (API responses in JSON)
- **Customers:** Customer profiles from the CRM (database extracts)

Your task is to design a Medallion architecture that:
1. Ingests raw data into the Bronze layer
2. Cleanses and standardizes data in the Silver layer
3. Creates business-ready analytics in the Gold layer

---

## Core Tasks

### Task 1: Data Source Analysis (30 mins)

Review the sample data structures below:

**Web Events (JSON):**
```json
{
  "event_id": "e-abc123",
  "event_type": "page_view",
  "timestamp": "2024-01-15T10:30:00Z",
  "user_id": "u-12345",
  "session_id": "s-99887",
  "page_url": "/products/laptop-pro",
  "device": "mobile",
  "properties": {
    "referrer": "google.com",
    "product_id": "p-001"
  }
}
```

**Orders (CSV columns):**
```
order_id, customer_id, order_date, status, total_amount, shipping_address, payment_method
```

**Products (JSON):**
```json
{
  "product_id": "p-001",
  "name": "Laptop Pro 15",
  "category": "Electronics",
  "subcategory": "Laptops",
  "price": 1299.99,
  "inventory_count": 45,
  "attributes": {
    "brand": "TechBrand",
    "weight_kg": 1.8
  }
}
```

Document the following for each data source:
- Data format (JSON, CSV, etc.)
- Update frequency (real-time, daily, etc.)
- Key fields that need transformation
- Potential data quality issues

---

### Task 2: Bronze Layer Design (45 mins)

Design the Bronze layer tables. For each table, specify:
1. Table name (following naming conventions)
2. Column definitions (use VARIANT for JSON where appropriate)
3. Metadata columns (ingestion timestamp, source file, etc.)

Use the template in `templates/bronze_table_template.md`

**Deliverable:** Complete Bronze layer schema design for all four data sources.

---

### Task 3: Silver Layer Design (45 mins)

Design the Silver layer tables. Focus on:
1. Data typing (convert strings to appropriate types)
2. Standardization (consistent date formats, uppercase codes)
3. Deduplication strategy
4. Null handling approach

For each Silver table, define:
- Column names and types
- Primary key or unique constraint
- Transformations applied from Bronze

Use the template in `templates/silver_table_template.md`

---

### Task 4: Gold Layer Design (45 mins)

Design at least TWO Gold layer objects that answer business questions:

**Option A: Daily Sales Summary**
- Total revenue by date
- Order count by date
- Average order value

**Option B: Customer 360 View**
- Customer ID
- Total lifetime value
- Order count
- Most recent order date
- Favorite product category

**Option C: Product Performance Dashboard**
- Product ID and name
- Total units sold
- Total revenue
- View-to-purchase conversion rate

For each Gold object, specify:
- Purpose and business question answered
- Source tables (Silver layer)
- Aggregations or joins required
- Suggested materialization strategy (table, view)

---

### Task 5: Data Flow Diagram (30 mins)

Create a data flow diagram showing:
1. External data sources
2. Bronze layer tables
3. Silver layer tables
4. Gold layer objects
5. Arrows showing data flow direction

Use the Mermaid template in `templates/data_flow.mermaid` or draw by hand.

---

## Deliverables

Submit the following files:

1. **bronze_design.md** - Bronze layer table definitions
2. **silver_design.md** - Silver layer table definitions with transformations
3. **gold_design.md** - Gold layer objects with business purpose
4. **data_flow_diagram.mermaid** (or hand-drawn scan) - Visual architecture

---

## Definition of Done

- [ ] All four data sources mapped to Bronze tables
- [ ] Silver tables defined with explicit data types
- [ ] At least 2 Gold layer objects designed
- [ ] Data flow diagram shows complete lineage
- [ ] Each design document follows the provided template

---

## Evaluation Criteria

| Criterion | Points |
|-----------|--------|
| Bronze layer captures raw data appropriately | 20 |
| Silver layer applies correct transformations | 25 |
| Gold layer answers clear business questions | 25 |
| Data flow diagram is complete and accurate | 15 |
| Naming conventions are consistent | 10 |
| Documentation is clear and professional | 5 |

---

## Stretch Goals (Optional)

1. Add a fourth Gold layer object for a use case of your choice
2. Include data quality checks at each layer transition
3. Design a historical tracking approach for slowly changing customer data
