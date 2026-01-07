# ETL vs. ELT Comparison

## Learning Objectives

- Compare ETL and ELT approaches directly
- Know when to use each approach
- Understand hybrid approaches
- Make informed architecture decisions

## Why This Matters

Choosing between ETL and ELT (or a hybrid) affects your entire data architecture. The right choice depends on data volumes, latency requirements, team skills, and infrastructure. Understanding the trade-offs helps you design effective data pipelines.

## The Concept

### Direct Comparison

```
ETL:
Source --> Extract --> Transform --> Load --> Warehouse
                           ^
                      ETL Server
                      (Transformation
                       happens here)

ELT:
Source --> Extract --> Load --> Transform --> Warehouse
                                    ^
                              Data Warehouse
                              (Transformation
                               happens here)
```

### Feature Comparison

| Aspect | ETL | ELT |
|--------|-----|-----|
| Transform location | External ETL server | Inside data warehouse |
| Raw data in target | No | Yes |
| Latency | Longer (transform first) | Shorter (load first) |
| Flexibility | Less (fixed transforms) | More (re-transform anytime) |
| Infrastructure | ETL servers needed | Just warehouse + storage |
| Skills required | ETL tool expertise | SQL skills |
| Cost model | ETL tool licenses + servers | Warehouse compute |
| Data history | Transformed only | Raw data preserved |
| Schema changes | Pipeline rebuild needed | Re-transform from raw |

### Detailed Trade-offs

**Performance:**
| Scenario | ETL | ELT |
|----------|-----|-----|
| Small data (GB) | Fast, simple | Overkill |
| Large data (TB+) | Bottleneck on ETL server | Scales with warehouse |
| Complex transforms | May be faster (optimized tools) | Depends on SQL efficiency |

**Architecture Complexity:**
| Scenario | ETL | ELT |
|----------|-----|-----|
| Small team | More tools to manage | Simpler stack |
| Enterprise | Established patterns | Modern, fewer components |
| Multi-source | Complex orchestration | Unified in warehouse |

**Data Quality:**
| Scenario | ETL | ELT |
|----------|-----|-----|
| Validation | At transform stage | Can validate post-load |
| Bad data | Rejected before load | Loaded but flagged |
| Re-processing | Re-run entire pipeline | Re-transform from raw |

### When to Use ETL

**Choose ETL when:**
- Legacy systems require traditional approach
- Regulatory requirement to filter data before storage
- Sensitive data must not be stored raw
- Complex transformations suit specialized tools
- Small datasets where ETL overhead is minimal

**ETL Examples:**
- HIPAA-compliant healthcare (PII filtered before load)
- Legacy mainframe data migration
- Existing Informatica/SSIS investment
- Simple, stable transformation requirements

### When to Use ELT

**Choose ELT when:**
- Using cloud data warehouses (BigQuery, Snowflake, Redshift)
- Need data fast with transformation flexibility
- Want to preserve raw data for re-processing
- Team has strong SQL skills
- Modern, greenfield architecture

**ELT Examples:**
- Cloud-native analytics platforms
- Data lake to warehouse pipelines
- Agile analytics (requirements change frequently)
- Self-service BI environments

### Decision Framework

```
                       Do you need to filter sensitive data
                       before it enters the warehouse?
                              /            \
                           Yes              No
                            |                |
                          ETL               |
                                            |
                           Are you using a modern cloud
                           data warehouse?
                              /            \
                           Yes              No
                            |                |
                          ELT              ETL
                            |
                    Does your team prefer SQL
                    over ETL tools?
                              /            \
                           Yes              No
                            |                |
                          ELT           ETL or Hybrid
```

### Hybrid Approaches

Many organizations use both:

**Pre-Processing + ELT:**
```
Source --> Light Transform --> Load Raw --> ELT in Warehouse
           (PII masking,
            format conversion)
```

**ETL for Complex + ELT for Simple:**
```
Complex source --> ETL --> Warehouse
Simple source --> ELT --> Warehouse
```

**Streaming + ELT:**
```
Stream --> Light Transform --> Load --> Batch ELT --> Analytics
           (Real-time filters)          (Deep transforms)
```

### Real-World Example

**E-Commerce Data Pipeline:**

**ETL Approach:**
```
1. Extract: Pull orders from PostgreSQL
2. Transform (Informatica):
   - Clean addresses
   - Lookup foreign keys
   - Calculate metrics
3. Load: Insert to Redshift fact table
```

**ELT Approach:**
```
1. Extract: Pull orders from PostgreSQL
2. Load: Insert raw orders to BigQuery staging
3. Transform (SQL):
   - Clean addresses
   - Lookup foreign keys
   - Calculate metrics
4. Move to fact table
```

**Key Difference:**
- ETL: If transformation logic changes, re-run entire pipeline on source
- ELT: Raw data in warehouse; just re-run SQL transform

### Cost Comparison

**ETL Costs:**
- ETL tool licenses (often expensive)
- ETL server infrastructure
- Development and maintenance expertise
- Warehouse storage (transformed data only)

**ELT Costs:**
- Storage (raw + transformed data)
- Warehouse compute for transformations
- SQL/dbt development
- No separate ETL tool licenses

**Cloud Example (Illustrative):**
```
ETL Setup:
- Informatica Cloud: $3000/month
- ETL VMs: $500/month
- Warehouse storage: $200/month
Total: ~$3700/month

ELT Setup:
- BigQuery storage: $400/month (includes raw)
- BigQuery compute: $1000/month
Total: ~$1400/month
```

### Maturity Model

```
Traditional                              Modern
    ^                                        ^
    |                                        |
    +-- ETL with on-prem warehouse           |
    +-- ETL with cloud warehouse             |
    +-- Hybrid ETL/ELT                       |
    +-- Full ELT with streaming     ---------+
```

Most organizations modernizing move from ETL toward ELT.

## Summary

- **ETL**: Transform before loading; traditional approach, external servers
- **ELT**: Load first, transform in warehouse; modern, cloud-native
- Choose ETL for compliance, legacy systems, specialized transformations
- Choose ELT for cloud warehouses, flexibility, SQL-first teams
- Hybrid approaches combine benefits of both
- ELT is becoming the default for new cloud-native data platforms

## Additional Resources

- [ETL vs ELT (AWS)](https://aws.amazon.com/compare/the-difference-between-etl-and-elt/)
- [ETL vs ELT (Snowflake)](https://www.snowflake.com/guides/etl-vs-elt/)
- [Modern Data Integration (Fivetran)](https://www.fivetran.com/blog/etl-vs-elt)
- [dbt and ELT](https://docs.getdbt.com/docs/introduction)
