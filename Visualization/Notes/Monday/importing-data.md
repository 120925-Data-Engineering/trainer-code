# Importing Data into Power BI

## Learning Objectives
- Understand the difference between Import and DirectQuery modes
- Choose the appropriate data connectivity mode for different scenarios
- Configure data refresh strategies for imported data
- Recognize the performance implications of each approach

## Why This Matters

When connecting Power BI to a data source, you face a fundamental choice: should Power BI **import** the data into its own engine, or should it **query** the source directly every time a user interacts with the report? This decision significantly impacts performance, data freshness, and infrastructure costs.

Making the right choice requires understanding your data volumes, refresh requirements, and user expectations. As a data engineer, you will often advise stakeholders on this decision and design the underlying data architecture to support it.

## Data Connectivity Modes

Power BI offers three primary connectivity modes:

### Import Mode

In Import mode, Power BI extracts data from the source and loads it into an in-memory columnar database (VertipAQ engine).

**How it works:**
1. Power BI connects to the data source
2. Data is transformed using Power Query
3. Compressed data is stored inside the .pbix file
4. Queries run against the local in-memory engine
5. Scheduled refresh updates the local data

**Advantages:**
- Fastest query performance (data is local and compressed)
- Full DAX functionality available
- Works offline once data is loaded
- Reduces load on source systems

**Disadvantages:**
- Data is not real-time (only as fresh as last refresh)
- Dataset size limits (1 GB for Pro, 100 GB for Premium)
- Memory consumption scales with data volume
- Refresh can be time-consuming for large datasets

### DirectQuery Mode

In DirectQuery mode, Power BI sends queries directly to the source database whenever a user interacts with a visual.

**How it works:**
1. Power BI maintains only metadata (no data stored locally)
2. Each visual interaction generates SQL queries
3. Source database executes the queries
4. Results are returned and rendered in real-time
5. No scheduled refresh needed (data is always current)

**Advantages:**
- Always shows current data (real-time or near-real-time)
- No dataset size limits (queries run on source)
- Minimal Power BI storage requirements
- Changes in source reflect immediately

**Disadvantages:**
- Slower query performance (network latency + source query time)
- Source database must handle all query load
- Limited DAX functionality (some functions not supported)
- Requires constant network connectivity

### Live Connection

A specialized mode for connecting to existing semantic models:
- **Analysis Services** (on-premises or Azure)
- **Power BI datasets** (connecting to published datasets)

Live Connection provides real-time access to pre-built enterprise data models without duplicating data.

## Choosing the Right Mode

Use this decision framework to select the appropriate connectivity mode:

### Choose Import When:
- Data changes infrequently (daily, weekly, monthly)
- Dataset size is under 1 GB (Pro) or 100 GB (Premium)
- Users expect sub-second response times
- Source system has limited query capacity
- Users need offline access to reports
- You need full DAX functionality

### Choose DirectQuery When:
- Data must be real-time or near-real-time
- Dataset is too large to import
- Security rules must be enforced at the source
- Source database is optimized for analytics queries
- Compliance requires data to stay in source system

### Decision Matrix

| Factor | Import | DirectQuery |
|--------|--------|-------------|
| **Data freshness** | Up to refresh schedule | Real-time |
| **Performance** | Very fast | Depends on source |
| **Dataset size** | Limited (1-100 GB) | Unlimited |
| **DAX support** | Full | Limited |
| **Source load** | Only during refresh | Every query |
| **Network dependency** | None after import | Constant |

## Composite Models: The Hybrid Approach

Power BI Premium enables **composite models** that combine Import and DirectQuery within a single dataset.

**Use case example:**
- Import small, slowly-changing dimension tables (products, regions)
- DirectQuery large, frequently-changing fact tables (transactions)

This hybrid approach offers:
- Fast filtering on imported dimensions
- Real-time data from DirectQuery facts
- Reduced import refresh time

```
[Imported Dim_Product] ---> [DirectQuery Fact_Sales] <--- [Imported Dim_Date]
```

**Configuration:**
1. Connect to dimension tables using Import mode
2. Connect to fact tables using DirectQuery mode
3. Create relationships across storage modes
4. Power BI handles the query routing automatically

## Data Refresh Strategies

For Import mode, keeping data current requires a refresh strategy.

### Manual Refresh
- User clicks "Refresh" in Power BI Desktop or Service
- Suitable for ad-hoc analysis and development

### Scheduled Refresh (Power BI Service)
- Configure automatic refresh up to 8 times daily (Pro) or 48 times (Premium)
- Requires a gateway for on-premises/private sources
- Set specific times aligned with source data availability

**Configuration steps:**
1. Publish report to Power BI Service
2. Navigate to dataset settings
3. Configure data source credentials
4. Set refresh schedule (days and times)
5. Enable notifications for refresh failures

### Incremental Refresh
For large datasets, refresh only new or changed data:
- Define a date/time column for partitioning
- Specify the range of data to refresh (e.g., last 7 days)
- Historical data is loaded once and preserved
- Dramatically reduces refresh time and resource usage

**Example configuration:**
```
RangeStart = Today - 7 days (refresh window)
RangeEnd = Today
Detect data changes = Yes (only refresh if data changed)
```

### Dataflows and Refresh Orchestration

Power BI Dataflows provide:
- Centralized data preparation in the cloud
- Shared datasets across multiple reports
- Integration with Azure Data Factory for orchestration

**Architecture with Airflow:**
```
Airflow DAG --> Snowflake transformation --> Power BI Dataflow refresh --> Dataset refresh
```

## Performance Implications

### Import Mode Performance

The VertipAQ engine uses several optimization techniques:
- **Column compression**: Repeated values stored efficiently
- **Hash encoding**: Unique values mapped to integers
- **Run-length encoding**: Sequential duplicates compressed
- **Aggregations**: Pre-computed summaries for large tables

**Best practices for Import performance:**
1. Remove unnecessary columns before import
2. Reduce cardinality where possible (round decimals, truncate timestamps)
3. Set correct data types (integers over text for IDs)
4. Disable auto date/time for fewer hidden tables

### DirectQuery Performance

Source database optimization is critical:
- Create indexes on frequently filtered columns
- Materialize aggregations for common queries
- Use partitioning for large tables
- Monitor and optimize slow queries

**Best practices for DirectQuery:**
1. Limit visuals per page (each generates queries)
2. Avoid complex calculations in DAX (push to source)
3. Use aggregations table for summarized views
4. Implement query folding in Power Query

## Storage Mode Configuration

### Setting Storage Mode for Individual Tables

In Power BI Desktop Model view:
1. Select a table in the model diagram
2. Open the Properties pane
3. Set **Storage mode** to Import, DirectQuery, or Dual

### Dual Storage Mode

Dual mode tables can:
- Act as Import when joining with other Import tables
- Act as DirectQuery when joining with DirectQuery tables
- Optimize cross-storage-mode queries automatically

## Data Type Considerations

When importing data, verify data types are set correctly:

| Source Type | Power BI Type | Notes |
|-------------|---------------|-------|
| VARCHAR | Text | High cardinality impacts compression |
| INTEGER | Whole Number | Most efficient for measures |
| DECIMAL | Decimal Number | Use for currency, precise calculations |
| TIMESTAMP | Date/Time | Consider separating date and time |
| BOOLEAN | True/False | Very efficient storage |
| VARIANT | Text | JSON parsed as text, flatten in Power Query |

## Summary

- Import mode loads data into Power BI's in-memory engine for fastest performance
- DirectQuery sends queries to the source database for real-time data access
- Composite models combine both approaches for large-scale, real-time scenarios
- Scheduled refresh keeps imported data current, with incremental refresh for efficiency
- Storage mode selection significantly impacts query performance and data freshness

## Additional Resources

- [DirectQuery in Power BI](https://docs.microsoft.com/en-us/power-bi/connect-data/desktop-directquery-about) - Detailed DirectQuery guidance
- [Incremental Refresh](https://docs.microsoft.com/en-us/power-bi/connect-data/incremental-refresh-overview) - Configuration and best practices
- [Composite Models](https://docs.microsoft.com/en-us/power-bi/transform-model/desktop-composite-models) - Mixing storage modes
