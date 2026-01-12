# Connecting to Data Sources

## Learning Objectives
- Identify the types of data sources Power BI can connect to
- Understand authentication methods for different data sources
- Connect Power BI to Snowflake and other enterprise databases
- Apply best practices for secure and performant connections

## Why This Matters

A dashboard is only as valuable as the data behind it. Power BI's true power lies in its ability to connect to virtually any data source---from simple Excel files on your desktop to enterprise data warehouses like Snowflake running in the cloud.

As data engineers, you have spent weeks building pipelines that land data in Snowflake. Now you will learn how to expose that data to Power BI so analysts can build the visualizations that drive business decisions. Understanding connection options helps you advise stakeholders on the right approach for their use case.

## Data Source Categories

Power BI can connect to hundreds of data sources, organized into several categories:

### File-Based Sources
| Source | Description | Common Use Cases |
|--------|-------------|------------------|
| **Excel** | .xlsx and .xls workbooks | Ad-hoc analysis, small datasets |
| **CSV/Text** | Comma or tab-delimited files | Data exports, log files |
| **JSON** | JavaScript Object Notation files | API responses, configuration data |
| **XML** | Extensible Markup Language files | Legacy system exports |
| **Parquet** | Columnar storage format | Optimized analytics data |
| **PDF** | Extract tables from PDF documents | Scanned reports, invoices |

### Database Sources
| Source | Description | Connection Method |
|--------|-------------|-------------------|
| **SQL Server** | Microsoft's relational database | Native connector with DirectQuery |
| **PostgreSQL** | Open-source relational database | Native connector |
| **MySQL** | Popular open-source database | Native connector |
| **Oracle** | Enterprise database | Requires Oracle client |
| **Snowflake** | Cloud data warehouse | Native connector (ODBC) |
| **Azure SQL Database** | Cloud SQL Server | Native connector with Azure AD |

### Cloud Services
| Source | Description | Authentication |
|--------|-------------|----------------|
| **Azure Blob Storage** | Object storage for files | Account key, SAS token, Azure AD |
| **Azure Data Lake** | Enterprise data lake | Azure AD, service principal |
| **Amazon S3** | AWS object storage | Access key and secret |
| **Google BigQuery** | Google's data warehouse | OAuth, service account |
| **Salesforce** | CRM platform | OAuth |
| **SharePoint** | Microsoft collaboration | Microsoft 365 account |

### Online Services
Power BI offers connectors for SaaS applications including:
- Microsoft Dynamics 365
- Google Analytics
- Adobe Analytics
- GitHub
- Zendesk
- And many more through the Power BI connector ecosystem

## Connecting to Snowflake

Since you worked extensively with Snowflake in Week 5, let us walk through connecting Power BI to your Snowflake data warehouse.

### Prerequisites
1. Snowflake account URL (e.g., `xy12345.us-east-1.snowflakecomputing.com`)
2. User credentials with appropriate permissions
3. Warehouse name for compute resources
4. Database and schema names for your data

### Connection Steps

1. **Open Power BI Desktop** and click **Get Data** from the Home ribbon

2. **Search for Snowflake** in the connector list and select it

3. **Enter the Server URL**: Use your Snowflake account identifier
   ```
   xy12345.us-east-1.snowflakecomputing.com
   ```

4. **Enter the Warehouse**: Specify which compute warehouse to use
   ```
   COMPUTE_WH
   ```

5. **Choose Data Connectivity Mode**:
   - **Import**: Loads data into Power BI (faster queries, scheduled refresh)
   - **DirectQuery**: Queries Snowflake in real-time (always current, but slower)

6. **Authenticate**: Enter your Snowflake username and password
   - For production, consider using Azure AD or SSO integration

7. **Select Tables**: Navigate to your database and schema, then select the tables or views to import

### Connection String Options

For advanced scenarios, you can specify additional parameters:

| Parameter | Purpose | Example |
|-----------|---------|---------|
| `Role` | Snowflake role to assume | `ANALYST_ROLE` |
| `Database` | Default database | `ANALYTICS_DB` |
| `Schema` | Default schema | `GOLD` |
| `Warehouse` | Compute warehouse | `REPORTING_WH` |

## Authentication Methods

Different data sources support different authentication mechanisms:

### Basic Authentication (Username/Password)
The simplest method, suitable for:
- Development and testing
- Personal datasets
- Sources without SSO support

**Security consideration**: Credentials are stored encrypted in the Power BI file.

### Windows Authentication
Uses your Windows domain credentials. Common for:
- On-premises SQL Server
- Analysis Services
- SharePoint on-premises

### OAuth 2.0
Modern token-based authentication for:
- Cloud services (Salesforce, Google, etc.)
- Microsoft services (SharePoint Online, Dynamics 365)
- Snowflake (with Azure AD integration)

**Benefit**: No passwords stored in Power BI; users authenticate at runtime.

### Service Principal (Azure AD)
For automated, unattended scenarios:
- Scheduled refresh in Power BI Service
- Embedded analytics
- CI/CD pipelines

**Setup**: Create an Azure AD application registration with appropriate permissions.

### Organizational Account
Microsoft 365 work account authentication for:
- Microsoft cloud services
- Power Platform data sources

## Connection Best Practices

### 1. Use Views Instead of Direct Table Access

Instead of connecting Power BI directly to raw tables, create database views that:
- Abstract the underlying schema from report developers
- Apply row-level filtering for security
- Optimize column selection for the use case

```sql
-- Snowflake view optimized for Power BI
CREATE OR REPLACE VIEW gold.vw_sales_summary AS
SELECT 
    d.date_key,
    d.year,
    d.quarter,
    d.month_name,
    p.product_name,
    p.category,
    c.customer_name,
    c.region,
    f.quantity,
    f.revenue,
    f.cost
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.date_key = d.date_key
JOIN gold.dim_product p ON f.product_key = p.product_key
JOIN gold.dim_customer c ON f.customer_key = c.customer_key;
```

### 2. Limit Data at the Source

Apply filters during connection to reduce data volume:
- Date ranges (last 2 years instead of all history)
- Geographic filters (only relevant regions)
- Status filters (exclude archived records)

### 3. Select Only Required Columns

Power BI imports entire columns, so:
- Deselect columns not needed for analysis
- Remove audit columns (created_by, modified_date)
- Exclude large text or binary fields

### 4. Use Parameters for Dynamic Connections

Create Power Query parameters for:
- Server names (dev vs. prod)
- Date ranges
- Filter values

```m
// M code example: Parameterized connection
let
    Source = Snowflake.Databases(ServerParameter, WarehouseParameter),
    Database = Source{[Name=DatabaseParameter]}[Data],
    Schema = Database{[Name="GOLD"]}[Data]
in
    Schema
```

### 5. Document Connection Settings

Maintain documentation of:
- Data source connection strings
- Required credentials and their storage location
- Refresh schedules and their dependencies
- Row-level security implementations

## Gateway Requirements

When Power BI Service needs to connect to on-premises or private network data sources, you need an **On-premises Data Gateway**.

### When a Gateway is Required
- On-premises databases (SQL Server, Oracle)
- Private network resources behind a firewall
- DirectQuery connections that must route through your network

### When a Gateway is NOT Required
- Cloud data sources (Snowflake, Azure SQL, BigQuery)
- File uploads (refreshed by re-upload)
- Cloud services with native connectors

### Gateway Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Standard mode** | Shared gateway for multiple users | Enterprise deployments |
| **Personal mode** | Single-user gateway | Personal development |

## Troubleshooting Common Connection Issues

### "Unable to connect to the server"
- Verify the server name and port
- Check network connectivity and firewall rules
- Ensure the database service is running

### "Login failed for user"
- Verify username and password
- Check that the user has database access permissions
- For Snowflake, ensure the role has warehouse usage grants

### "The driver was not found"
- Install the required ODBC driver
- For Snowflake, install the Snowflake ODBC driver
- Restart Power BI Desktop after driver installation

### "DirectQuery is not supported"
- Some connectors only support Import mode
- Check the connector documentation for supported modes
- Consider using Import with scheduled refresh as an alternative

## Summary

- Power BI connects to hundreds of data sources including files, databases, and cloud services
- Snowflake connections use the native connector with options for Import or DirectQuery mode
- Authentication methods range from basic username/password to enterprise SSO with Azure AD
- Best practices include using views, limiting data, and parameterizing connections
- On-premises data gateways bridge the gap between cloud Power BI and private network resources

## Additional Resources

- [Power BI Data Sources Documentation](https://docs.microsoft.com/en-us/power-bi/connect-data/power-bi-data-sources) - Complete list of supported connectors
- [Snowflake Connector for Power BI](https://docs.snowflake.com/en/user-guide/odbc-powerbi) - Official Snowflake documentation
- [On-premises Data Gateway Guide](https://docs.microsoft.com/en-us/data-integration/gateway/) - Gateway installation and configuration
