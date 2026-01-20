# Data Governance Pillars

## Learning Objectives
- Define data governance and its four pillars
- Explain the role of organizational structures in governance
- Identify common governance frameworks and standards
- Understand how governance supports business objectives

## Why This Matters

Throughout this training, you have developed technical skills to build and operate data pipelines. But technical excellence alone does not guarantee that data is trustworthy, secure, and valuable to the organization.

Consider this scenario: An analyst publishes a report using a "customers" table they found in the data warehouse. A week later, Finance publishes contradictory numbers using a different "customers" table. Leadership asks: "Which number is correct?" Without governance, no one knows.

**Data governance** establishes the frameworks, policies, and processes that ensure data is managed as a strategic asset. It answers questions that technology alone cannot: Who owns this data? What are the quality standards? Who is authorized to access it? How long must it be retained?

## The Concept

### What Is Data Governance?

Data governance is a collection of practices and processes that ensure the formal management of data assets within an organization. It provides the structure, guidelines, and accountability needed to manage data as a strategic resource.

Governance is not about restricting access or slowing down work. Effective governance enables faster decisions, builds trust in data, and reduces risk. It is the foundation that makes self-service analytics possible.

### The Four Pillars of Data Governance

Data governance rests on four interconnected pillars:

| Pillar | Focus | Key Question |
|--------|-------|--------------|
| Data Ownership | Accountability | Who is responsible for this data? |
| Data Security | Protection | Who can access this data and how? |
| Data Cataloging | Discovery | What data do we have and where is it? |
| Data Quality | Trust | Is this data accurate and reliable? |

### Pillar 1: Data Ownership

**Definition:** Data ownership establishes clear accountability for data assets.

**Key Concepts:**

**Data Owner:** The business leader accountable for a data domain. They make decisions about how data is used and accessed but typically do not manage it day-to-day.

**Data Steward:** The operational role responsible for managing data quality, definitions, and documentation. Stewards implement the owner's decisions.

**Data Custodian:** The technical role responsible for data storage, security, and infrastructure. Often filled by IT or data engineering teams.

**Why It Matters:**
- Without ownership, no one is accountable when things go wrong
- Decisions about data access and quality need a single point of authority
- Cross-functional data issues need a clear escalation path

**Ownership Model Example:**

| Domain | Owner | Steward | Custodian |
|--------|-------|---------|-----------|
| Customer Data | VP of Sales | Sales Ops Manager | Data Engineering |
| Financial Data | CFO | Finance Analyst | Data Engineering |
| Product Data | VP of Product | Product Manager | Data Engineering |

### Pillar 2: Data Security

**Definition:** Data security protects data from unauthorized access, use, disclosure, modification, or destruction.

**Key Concepts:**

**Access Control:** Mechanisms that determine who can access what data and with what permissions (read, write, delete).

**Data Classification:** Categorizing data by sensitivity level (public, internal, confidential, restricted) to apply appropriate protections.

**Encryption:** Protecting data at rest and in transit through cryptographic techniques.

**Audit Logging:** Recording who accessed what data and when for accountability and compliance.

**Security Controls:**

```
Public Data         -> Open access, no restrictions
Internal Data       -> Employee access, basic authentication
Confidential Data   -> Role-based access, need-to-know basis
Restricted Data     -> Strict controls, encryption, audit logging
```

**Why It Matters:**
- Data breaches damage reputation and incur regulatory penalties
- Privacy regulations (GDPR, HIPAA) require specific security measures
- Customers and partners trust organizations that protect their data

### Pillar 3: Data Cataloging

**Definition:** Data cataloging creates an inventory of data assets with metadata that enables discovery and understanding.

**Key Concepts:**

**Data Catalog:** A centralized repository of metadata about data assets, including descriptions, locations, owners, and relationships.

**Metadata:** Information about data including technical metadata (schema, data types) and business metadata (definitions, usage context).

**Data Dictionary:** Documentation of data elements including names, definitions, data types, and valid values.

**Searchability:** The ability for users to find relevant data through search and browsing.

**Catalog Contents:**

| Metadata Type | Examples |
|---------------|----------|
| Technical | Table name, column types, row count, file location |
| Business | Description, business definition, usage guidelines |
| Operational | Owner, last updated, refresh schedule |
| Quality | Quality scores, known issues, data freshness |

**Why It Matters:**
- Users cannot use data they cannot find
- Duplicate data creation wastes resources
- Shared understanding of data definitions reduces errors

### Pillar 4: Data Quality

**Definition:** Data quality ensures that data meets the standards required for its intended use.

**Key Concepts:**

**Quality Dimensions:** The characteristics by which data quality is measured (accuracy, completeness, timeliness, consistency, validity, uniqueness).

**Quality Rules:** Specific tests and thresholds that define acceptable quality levels.

**Quality Monitoring:** Ongoing measurement and alerting on quality metrics.

**Quality Remediation:** Processes for identifying, prioritizing, and fixing quality issues.

**Quality Framework:**

```
Define Standards -> Measure Quality -> Monitor Trends -> Remediate Issues -> Report Status
```

**Why It Matters:**
- Poor quality data leads to poor decisions
- Downstream systems magnify upstream errors
- Trust in data is built through consistent quality

### Governance Organizational Structures

Effective governance requires organizational support:

#### Data Governance Council

A cross-functional committee that sets data policies and resolves disputes.

**Composition:** 
- Executive sponsor (CDO or equivalent)
- Data owners from major domains
- IT/Data Engineering leadership
- Compliance/Legal representative

**Responsibilities:**
- Define governance policies
- Resolve cross-domain data issues
- Prioritize governance initiatives
- Monitor governance metrics

#### Operating Model

How governance activities are distributed:

**Centralized:** A central team handles all governance activities. Good for consistency but may not scale.

**Federated:** Domain teams handle their own governance within central guidelines. Good for scale but requires coordination.

**Hybrid:** Central team sets standards and provides tools; domain teams execute. Balances consistency and scale.

### Governance Frameworks

Established frameworks provide structure for governance programs:

**DAMA-DMBOK (Data Management Body of Knowledge):**
- Comprehensive framework covering 11 knowledge areas
- Industry standard for data management practices
- Provides detailed guidance and best practices

**COBIT (Control Objectives for Information Technology):**
- IT governance framework that includes data management
- Strong on controls and risk management
- Aligns IT with business objectives

**ISO/IEC 38505:**
- International standard for data governance
- Provides principles and guidelines
- Framework-agnostic approach

### Governance Maturity Model

Organizations progress through governance maturity levels:

**Level 1 - Ad Hoc:**
- No formal governance
- Tribal knowledge
- Reactive issue resolution

**Level 2 - Awareness:**
- Recognition of governance need
- Initial policies in place
- Key roles identified

**Level 3 - Defined:**
- Formal governance program
- Clear roles and responsibilities
- Documented policies and procedures

**Level 4 - Managed:**
- Metrics-driven governance
- Active quality monitoring
- Regular governance reviews

**Level 5 - Optimized:**
- Continuous improvement culture
- Proactive issue prevention
- Governance embedded in processes

## Code Example

### Data Classification Schema in YAML

```yaml
# data_classification.yml
# Defines data classification levels and requirements

classification_levels:
  - level: public
    description: "Information that can be freely shared"
    controls:
      - access: "Open to all employees"
      - encryption_at_rest: false
      - encryption_in_transit: true
      - audit_logging: false
      - retention_days: 365
    examples:
      - "Marketing materials"
      - "Public product information"

  - level: internal
    description: "Information for internal use only"
    controls:
      - access: "Authenticated employees only"
      - encryption_at_rest: true
      - encryption_in_transit: true
      - audit_logging: false
      - retention_days: 730
    examples:
      - "Internal reports"
      - "Business metrics"

  - level: confidential
    description: "Sensitive business information"
    controls:
      - access: "Role-based, need-to-know"
      - encryption_at_rest: true
      - encryption_in_transit: true
      - audit_logging: true
      - retention_days: 2555
    examples:
      - "Customer PII"
      - "Financial data"
      - "Employee records"

  - level: restricted
    description: "Highly sensitive, regulated data"
    controls:
      - access: "Explicit approval required"
      - encryption_at_rest: true
      - encryption_in_transit: true
      - audit_logging: true
      - data_masking: true
      - retention_days: 2555
    examples:
      - "PHI (Health Information)"
      - "Payment card data"
      - "Authentication credentials"
```

### Data Ownership Registry

```python
"""
Data ownership registry for tracking data asset accountability.
"""

from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime


@dataclass
class DataOwner:
    """Represents a data owner."""
    name: str
    email: str
    department: str
    title: str


@dataclass
class DataSteward:
    """Represents a data steward."""
    name: str
    email: str
    team: str


@dataclass
class DataAsset:
    """Represents a governed data asset."""
    asset_id: str
    name: str
    description: str
    location: str
    classification: str
    owner: DataOwner
    steward: DataSteward
    quality_score: Optional[float]
    last_updated: datetime
    refresh_frequency: str
    tags: List[str]


class GovernanceRegistry:
    """Registry for tracking data governance metadata."""
    
    def __init__(self):
        self.assets = {}
    
    def register_asset(self, asset: DataAsset) -> None:
        """Register a new data asset."""
        self.assets[asset.asset_id] = asset
    
    def get_assets_by_owner(self, owner_email: str) -> List[DataAsset]:
        """Find all assets owned by a specific person."""
        return [
            asset for asset in self.assets.values()
            if asset.owner.email == owner_email
        ]
    
    def get_assets_by_classification(
        self, 
        classification: str
    ) -> List[DataAsset]:
        """Find all assets with a specific classification."""
        return [
            asset for asset in self.assets.values()
            if asset.classification == classification
        ]
    
    def assets_needing_review(
        self, 
        days_since_update: int = 30
    ) -> List[DataAsset]:
        """Find assets that have not been updated recently."""
        cutoff = datetime.now() - timedelta(days=days_since_update)
        return [
            asset for asset in self.assets.values()
            if asset.last_updated < cutoff
        ]


# Example usage
if __name__ == "__main__":
    registry = GovernanceRegistry()
    
    # Define ownership
    sales_owner = DataOwner(
        name="Jane Smith",
        email="jane.smith@company.com",
        department="Sales",
        title="VP of Sales"
    )
    
    sales_steward = DataSteward(
        name="John Doe",
        email="john.doe@company.com",
        team="Sales Operations"
    )
    
    # Register asset
    customer_asset = DataAsset(
        asset_id="cust_001",
        name="Customer Master",
        description="Golden record for customer information",
        location="PROD_DB.ANALYTICS.DIM_CUSTOMERS",
        classification="confidential",
        owner=sales_owner,
        steward=sales_steward,
        quality_score=0.98,
        last_updated=datetime.now(),
        refresh_frequency="Daily at 6 AM",
        tags=["customer", "master data", "PII"]
    )
    
    registry.register_asset(customer_asset)
    
    # Query the registry
    confidential_assets = registry.get_assets_by_classification("confidential")
    print(f"Confidential assets: {len(confidential_assets)}")
```

## Summary

- **Data governance** provides the framework for managing data as a strategic asset
- The **four pillars** are ownership, security, cataloging, and quality
- **Data ownership** establishes accountability through owners, stewards, and custodians
- **Data security** protects data through access control, classification, and encryption
- **Data cataloging** enables discovery through metadata and documentation
- **Data quality** ensures data meets standards for its intended use
- **Governance councils** provide cross-functional oversight and decision-making
- Organizations progress through **maturity levels** from ad hoc to optimized
- Frameworks like **DAMA-DMBOK** provide comprehensive guidance

## Additional Resources

- [DAMA-DMBOK 2nd Edition](https://www.dama.org/cpages/body-of-knowledge) - The definitive data management framework
- [Harvard Business Review: What Is Data Governance?](https://hbr.org/2017/05/what-is-data-governance-a-best-practices-framework-for-managing-data-assets) - Business perspective on governance
- [ISACA COBIT Framework](https://www.isaca.org/resources/cobit) - IT governance framework including data management
