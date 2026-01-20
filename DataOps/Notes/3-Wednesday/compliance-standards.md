# Compliance Standards for Data Engineers

## Learning Objectives
- Explain key compliance standards affecting data systems (GDPR, HIPAA, SOC 2)
- Identify the data requirements of each standard
- Understand how data engineers support compliance efforts
- Recognize the technical controls needed for compliance

## Why This Matters

Compliance is not just a legal checkbox---it shapes how data systems must be designed and operated. Regulations like GDPR can impose fines of up to 4% of global annual revenue for violations. HIPAA violations can result in fines up to $1.5 million per incident category per year.

As a data engineer, your pipeline designs, access controls, and data retention policies directly impact compliance. Understanding these requirements helps you build systems that are compliant by design rather than requiring costly retrofits.

## The Concept

### Overview of Key Standards

| Standard | Scope | Focus | Penalties |
|----------|-------|-------|-----------|
| GDPR | EU residents' data | Privacy rights | Up to 4% of global revenue |
| HIPAA | US health information | Patient data protection | Up to $1.5M per category/year |
| SOC 2 | Service organizations | Security controls | Loss of customer trust |
| PCI DSS | Payment card data | Cardholder protection | Fines + loss of processing ability |

### GDPR (General Data Protection Regulation)

**Scope:** Any organization processing personal data of EU residents, regardless of where the organization is located.

#### Key Principles

1. **Lawfulness**: Data processing requires a legal basis
2. **Purpose Limitation**: Data used only for stated purposes
3. **Data Minimization**: Collect only what is needed
4. **Accuracy**: Keep data accurate and up to date
5. **Storage Limitation**: Retain data only as long as necessary
6. **Integrity and Confidentiality**: Protect data appropriately

#### Data Subject Rights

| Right | Data Engineer Implication |
|-------|---------------------------|
| Right to Access | Must be able to export user data |
| Right to Rectification | Must support data corrections |
| Right to Erasure ("Right to be Forgotten") | Must be able to delete user data |
| Right to Portability | Must export in machine-readable format |

#### Technical Requirements

- **Encryption** at rest and in transit
- **Access controls** limiting who can see personal data
- **Audit logs** recording data access
- **Data retention** policies with automated deletion
- **Consent management** tracking

### HIPAA (Health Insurance Portability and Accountability Act)

**Scope:** Any organization handling Protected Health Information (PHI) in the US healthcare system.

#### Protected Health Information (PHI)

Health information combined with identifiers:
- Names, dates, phone numbers, email
- Social Security numbers, medical record numbers
- Health conditions, treatments, payments

#### Key Rules

**Privacy Rule:** Defines permitted uses and disclosures of PHI.

**Security Rule:** Technical safeguards for electronic PHI (ePHI):
- Access controls (unique user IDs, automatic logoff)
- Audit controls (record system activity)
- Integrity controls (protect from alteration)
- Transmission security (encryption)

**Breach Notification Rule:** Notify affected individuals within 60 days.

#### Minimum Necessary Standard

Access only the minimum PHI necessary for the task. This directly impacts:
- Role design (narrow, specific roles)
- Query patterns (filter to needed fields)
- Data masking (hide unnecessary PHI)

### SOC 2 (Service Organization Control 2)

**Scope:** Service organizations that store, process, or transmit customer data.

#### Trust Service Criteria

| Criteria | Focus |
|----------|-------|
| Security | Protection against unauthorized access |
| Availability | System uptime and performance |
| Processing Integrity | Complete, accurate processing |
| Confidentiality | Protection of confidential information |
| Privacy | Personal information handling |

#### Evidence Requirements

SOC 2 audits require evidence of controls:
- Access control policies and logs
- Change management procedures
- Incident response documentation
- Encryption implementation
- Backup and recovery testing

### How Data Engineers Support Compliance

#### 1. Design for Privacy

- Collect minimum necessary data
- Pseudonymize where possible
- Implement purpose-based access

```sql
-- Example: Separate PII from analytics data
CREATE VIEW analytics_orders AS
SELECT 
    order_id,
    product_id,
    quantity,
    -- Pseudonymized customer reference
    HASH(customer_id) as customer_hash,
    order_date
FROM orders;
```

#### 2. Implement Access Controls

- Role-based access with least privilege
- Separate duties (admin vs. analyst)
- Time-bound access for temporary needs

#### 3. Enable Audit Trails

- Log all access to sensitive data
- Track data changes with timestamps
- Maintain query history

```sql
-- Snowflake: Query access history
SELECT 
    user_name,
    query_text,
    start_time
FROM snowflake.account_usage.query_history
WHERE query_text ILIKE '%customers%'
ORDER BY start_time DESC;
```

#### 4. Support Data Subject Requests

- Build data export capabilities
- Implement deletion workflows
- Track consent and preferences

#### 5. Manage Data Retention

- Define retention periods by data type
- Automate deletion of expired data
- Document retention policies

```sql
-- Delete data older than retention period
DELETE FROM user_events
WHERE event_date < DATEADD(year, -2, CURRENT_DATE);
```

## Code Example

### Compliance Audit Query

```sql
-- GDPR: Find all data for a specific user (data subject access request)
SELECT 
    'customers' as source_table,
    customer_id,
    email,
    name,
    created_at
FROM customers
WHERE email = 'user@example.com'

UNION ALL

SELECT 
    'orders' as source_table,
    customer_id,
    NULL as email,
    NULL as name,
    order_date as created_at
FROM orders
WHERE customer_id = (
    SELECT customer_id FROM customers WHERE email = 'user@example.com'
)

UNION ALL

SELECT 
    'support_tickets' as source_table,
    customer_id,
    NULL as email,
    NULL as name,
    created_at
FROM support_tickets
WHERE customer_id = (
    SELECT customer_id FROM customers WHERE email = 'user@example.com'
);
```

### Data Retention Policy Implementation

```python
"""
Automated data retention enforcement.
"""

from datetime import datetime, timedelta
from dataclasses import dataclass

@dataclass
class RetentionPolicy:
    table_name: str
    date_column: str
    retention_days: int
    regulation: str

POLICIES = [
    RetentionPolicy("user_events", "event_date", 730, "GDPR"),
    RetentionPolicy("audit_logs", "log_date", 2555, "SOC2"),
    RetentionPolicy("support_tickets", "created_at", 1825, "GDPR"),
]

def generate_deletion_sql(policy: RetentionPolicy) -> str:
    return f"""
    DELETE FROM {policy.table_name}
    WHERE {policy.date_column} < DATEADD(day, -{policy.retention_days}, CURRENT_DATE)
    -- Regulation: {policy.regulation}
    """

# Generate deletion statements
for policy in POLICIES:
    print(generate_deletion_sql(policy))
```

## Summary

- **GDPR** protects EU residents' personal data with rights to access, correction, and deletion
- **HIPAA** protects health information with strict access and audit requirements
- **SOC 2** provides assurance of security controls for service organizations
- Data engineers support compliance through **access controls**, **audit logs**, and **retention policies**
- Build **privacy by design**: collect minimum data, pseudonymize, limit access
- Implement **deletion workflows** for data subject requests
- Maintain **documentation** of data flows and controls for audits

## Additional Resources

- [GDPR Official Text](https://gdpr.eu/tag/gdpr/) - Full regulation text with guidance
- [HHS HIPAA Guidance](https://www.hhs.gov/hipaa/index.html) - US government HIPAA resources
- [AICPA SOC 2 Overview](https://www.aicpa.org/soc2) - SOC 2 framework information
