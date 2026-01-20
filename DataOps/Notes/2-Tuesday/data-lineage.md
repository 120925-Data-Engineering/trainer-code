# Data Lineage

## Learning Objectives
- Define data lineage and explain its importance
- Distinguish between technical and business lineage
- Identify tools and approaches for tracking lineage
- Apply lineage knowledge to impact analysis and debugging

## Why This Matters

Imagine receiving an urgent message: "The revenue numbers on the executive dashboard look wrong." You need to answer several questions immediately:

- Where does this data come from?
- What transformations are applied?
- What other reports might be affected?
- When did the data last update correctly?

Without data lineage, you are navigating blindly. You would have to trace through code, examine configuration files, and interview colleagues to reconstruct the data flow. With proper lineage, these answers are immediately visible.

**Data lineage** provides a map of your data landscape. It shows how data flows from sources through transformations to consumption, enabling rapid debugging, confident change management, and regulatory compliance.

## The Concept

### What Is Data Lineage?

Data lineage is the record of data's origin, movement, and transformation throughout its lifecycle. Think of it as the data's family tree or supply chain documentation.

Lineage answers fundamental questions:

| Question | Lineage Provides |
|----------|------------------|
| Where did this data come from? | Source systems, tables, files |
| How was it transformed? | ETL processes, business logic |
| Where is it used? | Reports, dashboards, ML models |
| When was it last updated? | Pipeline execution history |
| Who is responsible? | Data owners and stewards |

### Types of Data Lineage

#### Technical Lineage

Technical lineage documents the physical flow of data: specific tables, columns, queries, and code.

**Characteristics:**
- Column-level detail
- Includes technical metadata (data types, row counts)
- Generated automatically by tools
- Used by engineers for debugging

**Example:**
```
raw_orders.order_total 
  -> stg_orders.order_total 
    -> fct_orders.order_total 
      -> revenue_dashboard.total_revenue
```

#### Business Lineage

Business lineage describes the conceptual flow of business entities and metrics.

**Characteristics:**
- Business term definitions
- Calculation logic in plain language
- Maintained by data stewards
- Used by analysts and business users

**Example:**
```
Revenue (KPI)
  Calculated as: Sum of all completed order totals
  Source: Order Management System
  Updated: Daily at 6 AM
  Owner: Finance Team
```

### Why Lineage Matters

#### Impact Analysis

Before making changes, lineage shows what will be affected:

"If I modify this column, what dashboards will break?"
"If I change this business rule, which reports need updating?"

Without lineage, changes are risky because downstream impacts are unknown.

#### Root Cause Analysis

When data is wrong, lineage helps trace the problem:

"The dashboard shows zero revenue. Let me trace back through the lineage..."
- Dashboard reads from `fct_orders`
- `fct_orders` joins `stg_orders` and `dim_products`
- `stg_orders` reads from `raw_orders`
- `raw_orders` was empty today because the source system ingestion failed

#### Compliance and Auditing

Regulations like GDPR require organizations to know:
- Where personal data is stored
- How it is processed
- Who has access

Lineage provides the documentation needed for compliance audits.

#### Data Discovery

New team members can understand the data landscape:

"I need customer lifetime value. Where can I find it?"
"Follow the lineage from the customer_metrics table to understand its source."

### Lineage Granularity

Lineage can be tracked at different levels of detail:

| Level | Detail | Use Case |
|-------|--------|----------|
| System | System A feeds System B | Architecture documentation |
| Table | Table X becomes Table Y | Pipeline documentation |
| Column | Column A.x becomes Column B.y | Impact analysis |
| Row | Row 123 came from rows 45, 67 | Audit trails |

Most organizations maintain table-level lineage and selectively add column-level detail for critical data.

### Lineage Sources

Where does lineage information come from?

#### Automatic Extraction

Tools can parse code to extract lineage:
- SQL queries reveal table and column relationships
- dbt compiles models and records dependencies
- ETL tools log transformation metadata

**Advantages:** Comprehensive, always current
**Disadvantages:** Only captures technical lineage

#### Manual Documentation

Data stewards document business context:
- Data dictionaries
- Glossaries
- Business process documentation

**Advantages:** Captures business meaning
**Disadvantages:** Requires maintenance, may become stale

#### Runtime Observation

Systems can observe data flowing and record lineage:
- Data cataloging tools watch queries
- Pipeline orchestrators log executions

**Advantages:** Captures actual usage patterns
**Disadvantages:** May miss infrequent flows

### Lineage Visualization

Lineage is typically displayed as a directed graph:

```
[Source System A] -----> [Raw Layer] -----> [Staging Layer]
                                                   |
[Source System B] -----> [Raw Layer] -----> [Staging Layer] -----> [Mart Layer] -----> [Dashboard]
```

Interactive lineage tools allow users to:
- Click on any node to see its details
- Trace upstream to find data sources
- Trace downstream to find consumers
- Filter by time, owner, or data type

### Lineage in Practice

#### dbt Lineage

dbt automatically generates lineage from model dependencies:

```sql
-- models/fct_orders.sql
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.total
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c 
    ON o.customer_id = c.customer_id
```

The `ref()` function creates a dependency, and dbt visualizes this in its docs:

```
stg_orders -----> fct_orders
                      ^
dim_customers --------+
```

#### Snowflake Lineage

Snowflake tracks lineage through query history:
- Access History shows which queries touched which objects
- Object Dependencies shows views and their base tables
- Tag-based lineage tracks sensitive data flows

#### Dedicated Lineage Tools

Enterprise tools provide comprehensive lineage:
- **Atlan**: Data catalog with built-in lineage
- **Collibra**: Data governance platform with lineage
- **DataHub**: Open-source metadata platform
- **Apache Atlas**: Open-source governance tool

## Code Example

### Querying dbt Lineage

dbt exposes lineage in its manifest file (`target/manifest.json`):

```python
"""
Extract lineage information from dbt manifest.
"""

import json
from pathlib import Path


def load_manifest(manifest_path: str) -> dict:
    """Load dbt manifest file."""
    with open(manifest_path, 'r') as f:
        return json.load(f)


def get_model_dependencies(manifest: dict, model_name: str) -> dict:
    """Get upstream and downstream dependencies for a model."""
    
    # Find the model node
    model_key = None
    for key, node in manifest['nodes'].items():
        if node.get('name') == model_name:
            model_key = key
            break
    
    if not model_key:
        return {'error': f'Model {model_name} not found'}
    
    node = manifest['nodes'][model_key]
    
    # Get upstream dependencies (what this model depends on)
    upstream = node.get('depends_on', {}).get('nodes', [])
    
    # Get downstream dependencies (what depends on this model)
    downstream = []
    for key, other_node in manifest['nodes'].items():
        if model_key in other_node.get('depends_on', {}).get('nodes', []):
            downstream.append(key)
    
    return {
        'model': model_name,
        'upstream': upstream,
        'downstream': downstream
    }


def print_lineage_tree(manifest: dict, model_name: str, depth: int = 0):
    """Print lineage tree for a model."""
    deps = get_model_dependencies(manifest, model_name)
    
    indent = "  " * depth
    print(f"{indent}{model_name}")
    
    for upstream in deps.get('upstream', []):
        # Extract model name from key like 'model.project.stg_orders'
        upstream_name = upstream.split('.')[-1]
        print_lineage_tree(manifest, upstream_name, depth + 1)


# Example usage
if __name__ == "__main__":
    manifest = load_manifest('target/manifest.json')
    
    print("=== Lineage for fct_orders ===")
    deps = get_model_dependencies(manifest, 'fct_orders')
    print(f"Upstream: {deps['upstream']}")
    print(f"Downstream: {deps['downstream']}")
```

### Documenting Lineage in Markdown

```markdown
# Data Lineage: Customer Lifetime Value

## Business Definition
Customer Lifetime Value (CLV) represents the total revenue generated 
by a customer over their entire relationship with the company.

## Calculation
CLV = Sum of order totals for all completed orders by the customer

## Source Lineage

### Level 1: Source Systems
- **Order Management System (OMS)** - Source of truth for orders
- **Customer Database (CRM)** - Source of truth for customer info

### Level 2: Raw Layer
- `raw.oms_orders` - Daily extract from OMS
- `raw.crm_customers` - Daily extract from CRM

### Level 3: Staging Layer
- `staging.stg_orders` - Cleaned orders with standardized columns
- `staging.stg_customers` - Cleaned customers with deduplication

### Level 4: Mart Layer
- `marts.fct_orders` - Order fact table joined with dimensions
- `marts.customer_metrics` - Customer summary including CLV

### Level 5: Consumption
- Executive Dashboard (Tableau)
- Customer Analytics Report (weekly email)
- ML Model: Churn Prediction

## Data Flow Diagram

raw.oms_orders --> staging.stg_orders --> marts.fct_orders
                                              |
                                              v
raw.crm_customers --> staging.stg_customers --> marts.customer_metrics --> Dashboard
                                                        |
                                                        v
                                                    ML Model

## Update Schedule
- Source systems extract: Daily 2 AM
- Staging transforms: Daily 3 AM
- Mart builds: Daily 4 AM
- Dashboard refresh: Daily 6 AM

## Owners
- Data Source: IT Operations Team
- Transformations: Data Engineering Team
- Business Logic: Finance Analytics Team
- Consumption: Business Intelligence Team
```

### SQL for Impact Analysis

```sql
-- Snowflake: Find all objects that depend on a specific table
-- This uses ACCESS_HISTORY to find actual usage patterns

-- What views reference this table?
SELECT 
    referencing_object_name,
    referencing_object_domain,
    COUNT(*) as reference_count,
    MAX(query_start_time) as last_accessed
FROM snowflake.account_usage.access_history,
LATERAL FLATTEN(base_objects_accessed) AS f
WHERE f.value:objectName::STRING = 'ANALYTICS.MARTS.FCT_ORDERS'
GROUP BY referencing_object_name, referencing_object_domain
ORDER BY reference_count DESC;

-- What queries use this table?
SELECT 
    query_id,
    user_name,
    query_text,
    start_time
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%fct_orders%'
  AND start_time > DATEADD(day, -30, CURRENT_DATE)
ORDER BY start_time DESC
LIMIT 100;
```

## Summary

- **Data lineage** traces data from source through transformations to consumption
- **Technical lineage** shows physical data flows (tables, columns, queries)
- **Business lineage** describes business entities and their relationships
- Lineage enables **impact analysis** (what will break if I change this?)
- Lineage supports **root cause analysis** (where did this wrong value come from?)
- Lineage is required for **regulatory compliance** (where is personal data?)
- **dbt** generates lineage automatically from model dependencies
- **Snowflake** tracks lineage through query history and access logs
- Lineage should be documented at multiple granularities based on need

## Additional Resources

- [dbt Documentation: DAG](https://docs.getdbt.com/docs/build/documentation#the-dag) - dbt's lineage visualization
- [Snowflake Data Lineage](https://docs.snowflake.com/en/user-guide/object-dependencies) - Snowflake's built-in lineage features
- [OpenLineage Specification](https://openlineage.io/) - Open standard for lineage metadata
