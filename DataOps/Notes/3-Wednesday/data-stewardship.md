# Data Stewardship

## Learning Objectives
- Define data stewardship and its role in data governance
- Explain the responsibilities of data stewards
- Understand data ownership models and accountability frameworks
- Identify how to establish stewardship within an organization

## Why This Matters

You have learned about data governance pillars, access controls, and compliance requirements. But policies without people to enforce them are just documents. Data stewardship is the operational layer that makes governance work in practice.

Consider a situation where two departments define "active customer" differently. Sales counts anyone who has ever purchased; Marketing counts only those active in the last 90 days. Without a steward to resolve this, analysts produce conflicting reports, and leadership loses trust in data.

Data stewards bridge the gap between technical systems and business meaning. They ensure data is not just stored correctly but understood and used correctly.

## The Concept

### What Is Data Stewardship?

**Data stewardship** is the management and oversight of an organization's data assets to help provide business users with high-quality data that is easily accessible, usable, and safe.

A **data steward** is the person responsible for managing data elements---both the content and the metadata. They are the day-to-day caretakers of data quality and meaning.

### Stewardship vs. Ownership

| Role | Focus | Who |
|------|-------|-----|
| Data Owner | Strategic decisions, accountability | Business leader |
| Data Steward | Operational management, quality | Subject matter expert |
| Data Custodian | Technical implementation, security | IT/Data Engineering |

**Analogy:** In real estate, the owner decides whether to sell or renovate, the property manager handles tenant issues and maintenance, and the contractor does the physical work.

### Data Steward Responsibilities

#### 1. Data Quality Management

- Define quality rules and thresholds
- Monitor quality metrics
- Investigate and resolve quality issues
- Coordinate remediation with data custodians

#### 2. Metadata Management

- Maintain data definitions and business glossaries
- Document data lineage and relationships
- Keep metadata current as systems change
- Ensure consistent terminology across systems

#### 3. Policy Enforcement

- Ensure data usage follows policies
- Validate access requests against business need
- Report policy violations
- Participate in access reviews

#### 4. Issue Resolution

- Serve as escalation point for data disputes
- Resolve conflicting definitions
- Mediate between business units
- Document decisions and rationales

### Data Ownership Models

Organizations structure ownership differently based on size and culture:

#### Centralized Ownership

One team owns all data governance.

**Pros:** Consistent standards, clear accountability
**Cons:** Bottleneck, may lack domain expertise

#### Domain-Based Ownership

Business domains own their data (e.g., Sales owns customer data).

**Pros:** Deep domain expertise, clear accountability
**Cons:** Potential inconsistency, cross-domain conflicts

#### Federated Ownership

Central standards with domain execution.

**Pros:** Balance of consistency and expertise
**Cons:** Requires coordination, more complex

### Establishing Stewardship

#### Step 1: Identify Data Domains

Group data by business function:
- Customer Data
- Financial Data
- Product Data
- Employee Data

#### Step 2: Assign Stewards

For each domain, identify someone who:
- Understands the business context
- Has authority to make decisions
- Is available for ongoing responsibilities

#### Step 3: Define Responsibilities

Document what stewards are accountable for:
- Quality metrics to monitor
- Reviews to conduct
- Escalation paths

#### Step 4: Provide Tools and Training

Stewards need:
- Access to data catalogs
- Quality monitoring dashboards
- Training on governance processes

#### Step 5: Establish Governance Rhythm

Regular activities:
- Weekly quality reviews
- Monthly steward meetings
- Quarterly access reviews
- Annual policy updates

### Stewardship Operating Model

A typical stewardship structure:

```
Data Governance Council
        |
        v
Domain Data Owners (VP level)
        |
        v
Data Stewards (Manager/SME level)
        |
        v
Data Custodians (Engineering)
```

### Measuring Stewardship Effectiveness

Track metrics to ensure stewardship is working:

| Metric | Target |
|--------|--------|
| Data quality scores | >95% |
| Metadata completeness | 100% of critical fields |
| Issue resolution time | <5 business days |
| Access review completion | 100% within deadline |
| Definition disputes | Decreasing trend |

## Code Example

### Stewardship Assignment Registry

```python
"""
Simple registry for tracking data stewardship assignments.
"""

from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Steward:
    name: str
    email: str
    department: str

@dataclass
class DataDomain:
    domain_id: str
    name: str
    description: str
    owner: str
    steward: Steward
    key_tables: List[str]
    quality_threshold: float

# Example domain assignments
DOMAINS = [
    DataDomain(
        domain_id="CUST",
        name="Customer Data",
        description="All customer-related information",
        owner="VP of Sales",
        steward=Steward("Alice Smith", "alice@company.com", "Sales Ops"),
        key_tables=["dim_customers", "customer_preferences", "customer_addresses"],
        quality_threshold=0.98
    ),
    DataDomain(
        domain_id="FIN",
        name="Financial Data",
        description="Financial transactions and reporting",
        owner="CFO",
        steward=Steward("Bob Johnson", "bob@company.com", "Finance"),
        key_tables=["fct_transactions", "dim_accounts", "fct_revenue"],
        quality_threshold=0.99
    ),
    DataDomain(
        domain_id="PROD",
        name="Product Data",
        description="Product catalog and inventory",
        owner="VP of Product",
        steward=Steward("Carol Davis", "carol@company.com", "Product Ops"),
        key_tables=["dim_products", "dim_categories", "inventory_levels"],
        quality_threshold=0.97
    )
]

def get_steward_for_table(table_name: str) -> Optional[Steward]:
    """Find the steward responsible for a given table."""
    for domain in DOMAINS:
        if table_name in domain.key_tables:
            return domain.steward
    return None

# Example usage
if __name__ == "__main__":
    steward = get_steward_for_table("dim_customers")
    if steward:
        print(f"Contact {steward.name} ({steward.email}) for dim_customers issues")
```

### Stewardship Dashboard Query

```sql
-- Query for stewardship dashboard: quality by domain
WITH domain_quality AS (
    SELECT 
        'Customer' as domain,
        AVG(quality_score) as avg_quality,
        COUNT(CASE WHEN quality_score < 0.95 THEN 1 END) as tables_below_threshold
    FROM data_quality_scores
    WHERE table_name IN ('dim_customers', 'customer_preferences')
    
    UNION ALL
    
    SELECT 
        'Financial' as domain,
        AVG(quality_score) as avg_quality,
        COUNT(CASE WHEN quality_score < 0.95 THEN 1 END) as tables_below_threshold
    FROM data_quality_scores
    WHERE table_name IN ('fct_transactions', 'dim_accounts')
)
SELECT 
    domain,
    ROUND(avg_quality * 100, 1) as quality_pct,
    tables_below_threshold,
    CASE 
        WHEN avg_quality >= 0.98 THEN 'Healthy'
        WHEN avg_quality >= 0.95 THEN 'Warning'
        ELSE 'Critical'
    END as status
FROM domain_quality;
```

## Summary

- **Data stewardship** is the operational management of data quality, definitions, and policies
- **Stewards** are the day-to-day caretakers who ensure data is understood and used correctly
- Stewards differ from **owners** (strategic) and **custodians** (technical)
- Key responsibilities: quality management, metadata curation, policy enforcement, issue resolution
- **Ownership models** range from centralized to domain-based to federated
- Establishing stewardship requires identifying domains, assigning stewards, defining responsibilities
- Measure effectiveness through quality scores, resolution times, and review completion

## Additional Resources

- [DAMA-DMBOK on Data Stewardship](https://www.dama.org/cpages/body-of-knowledge) - Industry framework
- [Data Governance Institute](https://datagovernance.com/) - Practical governance guidance
- [The Data Steward's Handbook](https://www.dataversity.net/) - Community resources
