# PII Handling and Data Masking

## Learning Objectives
- Define PII and identify common types in data systems
- Explain techniques for identifying PII in datasets
- Implement masking strategies including static and dynamic masking
- Understand tokenization and secure data handling practices

## Why This Matters

Modern data systems contain vast amounts of personal information: names, email addresses, phone numbers, social security numbers, health records. This data is valuable for analytics but dangerous if mishandled.

A single data breach exposing customer PII can cost millions in regulatory fines and lost business. As a data engineer, you are the last line of defense. Understanding PII handling is a core professional responsibility.

## The Concept

### What Is PII?

**Personally Identifiable Information (PII)** is any information that can be used to identify an individual, either alone or in combination with other data.

#### Direct Identifiers

| Type | Examples |
|------|----------|
| Full Name | John Smith, Maria Garcia |
| Government IDs | SSN, Passport Number |
| Financial Identifiers | Credit Card Number, Bank Account |
| Contact Information | Email, Phone, Physical Address |

#### Indirect Identifiers

| Type | Risk Level |
|------|------------|
| Date of Birth | High (with ZIP, identifies 87% of US population) |
| ZIP Code | Medium (combination risk) |
| Gender | Low alone, high in combination |

### Identifying PII in Data

#### Pattern Matching

```
SSN:         \d{3}-\d{2}-\d{4}
Email:       [\w\.-]+@[\w\.-]+\.\w+
Phone:       \d{3}[-.]?\d{3}[-.]?\d{4}
```

#### Column Name Analysis

Suspicious names: `*_name`, `*_email`, `*_ssn`, `*_phone`, `date_of_birth`

### Data Masking Techniques

#### Static Masking

Permanently transforms PII in a copy of the data.

| Technique | Description | Example |
|-----------|-------------|---------|
| Redaction | Replace with fixed value | John becomes XXXXX |
| Substitution | Replace with fake value | John becomes Robert |
| Character Masking | Mask portion | 123-45-6789 becomes XXX-XX-6789 |

#### Dynamic Masking

Shows different data to different users at query time. Original data remains unchanged; masking applies based on user role.

### Tokenization

Replaces PII with non-sensitive placeholders while maintaining a secure mapping.

1. PII value enters: `john@email.com`
2. Token generated: `TKN_A7B3C9X2`
3. Mapping stored securely
4. Token used in analytics

## Code Example

### Snowflake Dynamic Masking

```sql
-- Create masking policy
CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ADMIN') THEN val
    ELSE CONCAT('****@', SPLIT_PART(val, '@', 2))
  END;

-- Apply to column
ALTER TABLE customers MODIFY COLUMN email 
    SET MASKING POLICY email_mask;

-- SSN masking: show last 4 only
CREATE OR REPLACE MASKING POLICY ssn_mask AS (val STRING) 
RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ADMIN', 'HR_FULL_ACCESS') THEN val
    ELSE CONCAT('XXX-XX-', RIGHT(REPLACE(val, '-', ''), 4))
  END;
```

### Static Masking in Python

```python
import hashlib
from faker import Faker

fake = Faker()

def mask_email(email: str) -> str:
    hash_val = hashlib.md5(email.encode()).hexdigest()[:8]
    return f"user_{hash_val}@example.com"

def mask_ssn(ssn: str) -> str:
    digits = ''.join(c for c in ssn if c.isdigit())
    return f"XXX-XX-{digits[-4:]}" if len(digits) >= 4 else "XXX-XX-XXXX"

def mask_name(name: str) -> str:
    return fake.name()
```

## Summary

- **PII** is any information that can identify an individual
- **Identify PII** through pattern matching and column name analysis
- **Static masking** permanently transforms PII for non-production use
- **Dynamic masking** shows different data based on user role
- **Tokenization** replaces PII with reversible tokens
- Apply **data minimization**: collect only what you need

## Additional Resources

- [Snowflake Dynamic Data Masking](https://docs.snowflake.com/en/user-guide/security-column-ddm-intro)
- [NIST PII Definition](https://csrc.nist.gov/glossary/term/personally_identifiable_information)
- [GDPR on Personal Data](https://gdpr.eu/eu-gdpr-personal-data/)
