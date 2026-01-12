# Dataset Refresh in Power BI

## Learning Objectives
- Configure scheduled dataset refresh in Power BI Service
- Understand the on-premises data gateway
- Implement incremental refresh for large datasets
- Monitor and troubleshoot refresh failures

## Why This Matters

Dashboards are only useful when data is current. Stale data leads to wrong decisions. Refresh configuration ensures your reports show up-to-date information without manual intervention.

As a data engineer, you will often coordinate refresh schedules with upstream pipelines. Understanding refresh helps you design systems that keep visualization layers synchronized with source data.

## Refresh Fundamentals

### What Gets Refreshed

When a dataset refreshes:
1. Power BI connects to all data sources
2. Executes Power Query transformations
3. Loads data into the in-memory model
4. Recalculates relationships and measures
5. Updates all reports using this dataset

### Refresh Limitations by License

| License | Max Refreshes/Day | Max Dataset Size |
|---------|-------------------|------------------|
| Power BI Pro | 8 | 1 GB |
| Power BI Premium Per User | 48 | 100 GB |
| Power BI Premium Capacity | 48+ (configurable) | 400 GB |

## Manual Refresh

Trigger an on-demand refresh:

1. Navigate to your workspace in Power BI Service
2. Find the dataset
3. Click the ellipsis (...) > "Refresh now"

Useful for:
- Testing after data source changes
- Updating before important meetings
- Verifying connectivity after config changes

## Scheduled Refresh

Automate refresh at specified times.

### Configuration Steps

1. Go to dataset Settings (ellipsis > Settings)
2. Expand "Scheduled refresh"
3. Toggle "Keep your data up to date" to On
4. Set refresh frequency and times
5. Add email for failure notifications
6. Click "Apply"

### Schedule Options

- **Frequency**: Daily or specific days
- **Time zones**: Select your local time zone
- **Times**: Add up to 8 times per day (Pro) or 48 (Premium)

### Best Practices for Scheduling

- **Align with source updates**: Refresh after ETL jobs complete
- **Avoid peak hours**: Schedule during off-peak times
- **Stagger refreshes**: Do not refresh all datasets at once
- **Allow time for processing**: Complex datasets need time

## Data Gateway

Required for refreshing data from on-premises or private network sources.

### When Gateway is Needed

- On-premises databases (SQL Server, Oracle)
- File shares within corporate network
- Private cloud resources (Azure VNet)
- Any source not directly accessible from Power BI Service

### Gateway Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Standard** | Shared gateway for the organization | Enterprise deployment |
| **Personal** | Single user, single machine | Personal development |

### Gateway Installation

1. Download from Power BI Service: Settings > Manage gateways > Download
2. Run installer on a server with access to data sources
3. Sign in with Power BI account
4. Configure gateway name and recovery key
5. Add data sources to the gateway

### Configuring Data Sources

After gateway installation:
1. In Power BI Service, go to Settings > Manage gateways
2. Select your gateway
3. Click "Add data source"
4. Specify connection string, authentication, credentials
5. Test connection

### Dataset Gateway Configuration

1. In dataset settings, expand "Gateway connection"
2. Select the gateway
3. Map each data source to gateway data sources
4. Verify connection status

## Incremental Refresh

For large datasets, refresh only new or changed data.

### Benefits

- Significantly faster refresh times
- Reduced load on source systems
- Lower memory usage during refresh
- Better scalability for growing data

### Requirements

- Requires Premium or Premium Per User license
- Dataset must have a date/time column for partitioning
- Source query must support query folding with date filters

### Configuration

1. In Power BI Desktop, define parameters:
   - RangeStart (type DateTime)
   - RangeEnd (type DateTime)

2. Filter your fact table using these parameters:
   ```
   Source query: WHERE OrderDate >= RangeStart AND OrderDate < RangeEnd
   ```

3. Right-click the table > Incremental refresh
4. Configure settings:
   - Store rows in the last N years/months
   - Refresh rows in the last N days/months
   - Detect data changes (optional)

5. Publish to Power BI Service (incremental refresh only works in Service)

### How It Works

Power BI creates partitions:
- Historical partitions (archived, not refreshed)
- Recent partitions (refreshed each time)
- Real-time partition (optional, for premium)

Only the refresh window partitions are reprocessed.

## Monitoring Refresh

### Refresh History

View past refresh results:
1. Dataset settings > Refresh history
2. See status, duration, and timing
3. Click failed refresh for error details

### Common Refresh Failures

| Error | Cause | Solution |
|-------|-------|----------|
| **Credentials expired** | Auth token needs renewal | Re-enter credentials in dataset settings |
| **Gateway unreachable** | Gateway server offline | Check gateway machine status |
| **Data source error** | Source database issue | Verify source is accessible |
| **Timeout** | Query takes too long | Optimize source query or increase timeout |
| **Memory error** | Dataset too large | Reduce data volume or upgrade capacity |

### Notifications

Configure email alerts:
- Dataset settings > Scheduled refresh
- Add email addresses for failure notifications
- Multiple recipients supported

## Dataflows

Power BI dataflows provide ETL capabilities in the cloud.

### What Dataflows Do

- Extract data from sources
- Transform using Power Query Online
- Store in Azure Data Lake
- Reusable across multiple datasets

### Dataflow Refresh

Dataflows refresh independently:
- Set schedule in dataflow settings
- Datasets depending on dataflows refresh after dataflow completes
- Chain refreshes: Dataflow -> Dataset

### Benefits for Refresh

- Centralized data preparation
- Reduced source queries (pull once, use many)
- Linked entities for shared data

## Refresh Orchestration

### With Airflow/External Tools

Trigger refresh programmatically:
1. Use Power BI REST API
2. Call "Refresh Dataset" endpoint
3. Integrate into Airflow DAG

```python
# Example: Trigger Power BI refresh from Airflow
import requests

url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/refreshes"
headers = {"Authorization": f"Bearer {access_token}"}
response = requests.post(url, headers=headers)
```

### Refresh Order Dependencies

When multiple datasets depend on each other:
1. Refresh source datasets first
2. Use dataflows as intermediate layer
3. Time dependent refreshes sequentially

## Summary

- Scheduled refresh keeps data current without manual intervention
- Gateway is required for on-premises or private network sources
- Incremental refresh dramatically improves performance for large datasets
- Monitor refresh history to identify and resolve failures
- Use Power BI REST API to integrate refresh with external orchestration

## Additional Resources

- [Data Refresh Guide](https://docs.microsoft.com/en-us/power-bi/connect-data/refresh-data) - Official documentation
- [On-premises Gateway](https://docs.microsoft.com/en-us/data-integration/gateway/) - Gateway setup guide
- [Incremental Refresh](https://docs.microsoft.com/en-us/power-bi/connect-data/incremental-refresh-overview) - Configuration details
