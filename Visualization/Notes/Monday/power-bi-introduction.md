# Introduction to Power BI

## Learning Objectives
- Understand what Power BI is and its role in modern data analytics
- Identify the different Power BI editions and their use cases
- Compare Power BI with other business intelligence tools
- Recognize where Power BI fits in the modern data stack

## Why This Matters

In the journey from raw data to business insight, visualization is the final mile. Throughout this training, you have built the infrastructure to collect, process, and warehouse data using tools like Spark, Kafka, Airflow, Snowflake, and dbt. But data sitting in a warehouse does not drive decisions---people need to see and interact with it.

Power BI is Microsoft's flagship business intelligence platform, used by over 97% of Fortune 500 companies. As a data engineer, you will frequently work alongside analysts and business users who consume data through Power BI dashboards. Understanding how Power BI works helps you design better data models, optimize query performance, and build data pipelines that serve visualization tools effectively.

This week marks your transition from **data engineering** to **data storytelling**---connecting your Snowflake data warehouse to Power BI and building dashboards that transform numbers into narratives.

## What is Power BI?

Power BI is a comprehensive business intelligence platform that enables users to connect to data sources, transform data, create visualizations, and share insights across an organization.

### The Power BI Ecosystem

Power BI is not a single tool but a collection of interconnected services:

| Component | Description | Primary Users |
|-----------|-------------|---------------|
| **Power BI Desktop** | Windows application for building reports and data models | Report developers, analysts |
| **Power BI Service** | Cloud-based platform (app.powerbi.com) for sharing and collaboration | All users |
| **Power BI Mobile** | iOS and Android apps for viewing reports on the go | Executives, field workers |
| **Power BI Report Server** | On-premises solution for organizations with strict data residency requirements | Enterprise IT |
| **Power BI Embedded** | APIs for embedding reports in custom applications | Developers |

### Core Capabilities

Power BI provides an end-to-end analytics workflow:

1. **Connect**: Access data from hundreds of sources---databases, files, cloud services, APIs
2. **Transform**: Clean and shape data using Power Query (a low-code data preparation tool)
3. **Model**: Build relationships between tables and define business logic
4. **Visualize**: Create interactive charts, tables, and dashboards
5. **Share**: Publish reports and collaborate with colleagues

## Power BI Editions

Microsoft offers Power BI in several licensing tiers:

### Power BI Desktop (Free)

The desktop authoring tool is completely free to download and use. You can:
- Connect to any data source
- Build complete data models
- Create sophisticated reports
- Save files locally (.pbix format)

The limitation is sharing---you cannot publish to the Power BI Service without a license.

### Power BI Pro

The standard business license (currently ~$10/user/month) enables:
- Publishing reports to the Power BI Service
- Sharing dashboards with other Pro users
- Collaboration through workspaces
- Scheduled data refresh (up to 8 times per day)
- 1 GB maximum dataset size

### Power BI Premium Per User (PPU)

An enhanced individual license (~$20/user/month) that includes:
- All Pro features
- Larger dataset sizes (up to 100 GB)
- More frequent refreshes (up to 48 times per day)
- Advanced AI features
- Paginated reports

### Power BI Premium Per Capacity

An organizational license based on dedicated cloud capacity:
- Unlimited viewer licenses (only creators need Pro licenses)
- Massive datasets (up to 400 GB)
- On-premises reporting via Power BI Report Server
- Advanced workload management
- XMLA endpoint for external tool connectivity

### Choosing the Right Edition

| Scenario | Recommended Edition |
|----------|---------------------|
| Learning and personal projects | Desktop (Free) |
| Small team, everyone creates reports | Pro |
| Power users with large datasets | Premium Per User |
| Large organization with many viewers | Premium Per Capacity |

## Power BI vs. Other BI Tools

### Compared to Tableau

| Aspect | Power BI | Tableau |
|--------|----------|---------|
| **Cost** | Lower, especially for Microsoft 365 customers | Higher licensing costs |
| **Learning curve** | Gentler for Excel users | Steeper but more flexible |
| **Data modeling** | Strong (integrated with Analysis Services) | Requires external data prep |
| **Visualization** | Good, rapidly improving | Industry-leading aesthetics |
| **Ecosystem** | Tight Microsoft integration | Platform-agnostic |

### Compared to Looker

| Aspect | Power BI | Looker |
|--------|----------|--------|
| **Architecture** | Import-based (primarily) | Query-based (LookML) |
| **Self-service** | Excellent for analysts | Requires developer involvement |
| **Version control** | Limited native support | Git-native with LookML |
| **Cloud focus** | Multi-cloud, on-premises | Cloud-first (Google Cloud) |

### Compared to Apache Superset

| Aspect | Power BI | Superset |
|--------|----------|----------|
| **Cost** | Commercial license | Open source (free) |
| **Setup complexity** | Minimal (SaaS) | Requires infrastructure |
| **Features** | Full-featured enterprise BI | Growing feature set |
| **Support** | Microsoft enterprise support | Community-driven |

## Power BI in the Modern Data Stack

Power BI sits at the **semantic and presentation layer** of the modern data stack:

```
Data Sources --> Ingestion --> Transformation --> Warehouse --> BI/Analytics
   (APIs,         (Kafka,       (dbt,            (Snowflake,    (Power BI,
    Files,         Airflow)      Spark)           BigQuery)      Tableau)
    DBs)
```

### Integration with Snowflake

A key focus this week is connecting Power BI to the Snowflake data warehouse you worked with in Week 5. This integration leverages:

- **DirectQuery**: Power BI sends queries directly to Snowflake, ensuring users always see fresh data
- **Import mode**: Power BI extracts and caches data for faster performance with scheduled refreshes
- **dbt metrics layer**: Power BI can consume semantic definitions from dbt for consistent business logic

### The Role of the Data Engineer

While analysts typically build the final visualizations, data engineers play a crucial role:

- **Designing efficient data models** that Power BI can consume
- **Optimizing queries** to ensure fast dashboard performance
- **Building data pipelines** that refresh Power BI datasets on schedule
- **Implementing security** through row-level permissions that Power BI inherits

## Power BI Desktop Interface Overview

When you first open Power BI Desktop, you will see three main views:

### Report View
The canvas where you design visualizations. Key elements include:
- **Canvas**: The workspace where you drag and drop visuals
- **Visualizations pane**: Select chart types and configure visual properties
- **Fields pane**: Browse available tables and columns
- **Filters pane**: Apply slicers and filters to the report

### Data View
A spreadsheet-like view of your data tables. Use this to:
- Inspect imported data
- Create calculated columns
- Verify data types and formats

### Model View
A diagram showing relationships between tables. Essential for:
- Building star schemas
- Defining cardinality and cross-filter direction
- Troubleshooting relationship issues

## Key Terminology

| Term | Definition |
|------|------------|
| **Dataset** | A collection of tables, relationships, and measures published to the Power BI Service |
| **Report** | A collection of visualizations on one or more pages |
| **Dashboard** | A single-page collection of pinned visuals from multiple reports |
| **Workspace** | A container for collaboration, holding datasets, reports, and dashboards |
| **Dataflow** | Reusable data preparation logic stored in the Power BI Service |
| **Measure** | A DAX calculation that aggregates data dynamically |

## Summary

- Power BI is a comprehensive business intelligence platform spanning desktop authoring, cloud sharing, and mobile consumption
- The platform offers flexible licensing from free desktop use to enterprise-scale Premium capacity
- Power BI excels at connecting to enterprise data sources like Snowflake and integrates tightly with the Microsoft ecosystem
- As a data engineer, understanding Power BI helps you design better data models and optimize pipelines for visualization consumption

## Additional Resources

- [Power BI Documentation](https://docs.microsoft.com/en-us/power-bi/) - Official Microsoft documentation
- [Power BI Blog](https://powerbi.microsoft.com/en-us/blog/) - Latest features and announcements
- [Guy in a Cube YouTube Channel](https://www.youtube.com/c/GuyinaCube) - Practical tutorials and best practices
