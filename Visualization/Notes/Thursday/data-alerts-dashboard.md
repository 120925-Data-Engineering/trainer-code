# Data Alerts and Dashboards in Power BI

## Learning Objectives
- Understand the difference between reports and dashboards
- Create dashboards by pinning visuals
- Configure data alerts for proactive monitoring
- Design effective dashboard layouts

## Why This Matters

Reports are for exploration; dashboards are for monitoring. While reports let users dig into data, dashboards provide at-a-glance status updates that help stakeholders stay informed without active analysis.

Data alerts take this further by pushing notifications when metrics cross thresholds. Instead of checking dashboards constantly, users receive automated alerts when action is needed.

## Reports vs. Dashboards

| Aspect | Report | Dashboard |
|--------|--------|-----------|
| **Location** | Power BI Desktop or Service | Power BI Service only |
| **Purpose** | Detailed analysis | High-level monitoring |
| **Interactivity** | Slicers, drill-through, filters | Limited (click to go to report) |
| **Content** | Multi-page with many visuals | Single page, pinned tiles |
| **Source** | Single dataset | Multiple reports/datasets |
| **Refresh** | Based on dataset refresh | Real-time (if data streaming) |

## Creating a Dashboard

Dashboards are created in Power BI Service, not Desktop.

### Step-by-Step Creation

1. **Publish a report** from Power BI Desktop to Service
2. **Open the report** in Power BI Service
3. **Pin visuals** to a dashboard:
   - Hover over a visual
   - Click the pin icon
   - Choose "New dashboard" or an existing one
   - Click "Pin"
4. **Repeat** for additional visuals
5. **Navigate** to the dashboard from the left navigation

### What Can Be Pinned

| Item | Source |
|------|--------|
| Report visuals | Any visual from a report |
| Entire report pages | "Pin live page" option |
| Q&A results | After asking a question |
| Quick Insights | Auto-generated insights |
| Excel ranges | From Excel Online |
| Images and text | Custom tiles |
| Videos | Web URLs |

## Dashboard Tiles

Each pinned item becomes a tile on the dashboard.

### Tile Configuration

Select a tile and click the ellipsis (...) to access:
- **Tile details**: Title, subtitle, link destination
- **Open in focus mode**: Enlarge for viewing
- **Edit details**: Customize the tile
- **Refresh**: Manually refresh data

### Tile Sizing and Layout

- Resize by dragging corners
- Reposition by dragging
- Grid-based layout snaps tiles
- Mix large overview tiles with smaller detail tiles

### Phone Layout

Create a mobile-optimized dashboard:
1. Dashboard ellipsis > Edit > Phone layout
2. Arrange tiles for vertical viewing
3. Resize for mobile screen

## Data Alerts

Receive notifications when data crosses thresholds.

### Alert Requirements

Data alerts work on:
- Dashboard tiles (not report visuals directly)
- Card, gauge, and KPI visuals only
- Numeric measures with specific values

### Creating an Alert

1. Pin a Card, Gauge, or KPI to a dashboard
2. Open the dashboard
3. Click the ellipsis (...) on the tile
4. Select "Manage alerts"
5. Click "+ Add alert rule"

### Alert Configuration

| Setting | Description |
|---------|-------------|
| **Title** | Name for this alert |
| **Condition** | Above or below threshold |
| **Threshold** | Value that triggers alert |
| **Notification frequency** | At most once per hour/day |
| **Email notification** | Send email when triggered |
| **Push notification** | Send to Power BI mobile app |

### Alert Example

For a "Total Sales Today" card:
- Condition: Above $50,000
- Frequency: At most once per day
- Action: Email and mobile notification

### Managing Alerts

View all alerts:
- Gear icon > Settings > Alerts
- See all active alerts across dashboards
- Enable/disable or delete alerts

## Dashboard Features

### Natural Language Query

Dashboards have a Q&A box at the top:
- Ask questions about any dataset on the dashboard
- Pin results as new tiles
- Quick insights without navigating to reports

### Comments and Subscriptions

**Comments:**
- Add comments to dashboards for collaboration
- Tag colleagues with @mentions

**Subscriptions:**
- Schedule email snapshots of the dashboard
- Daily, weekly, or specific times
- Recipients do not need Power BI license

### Featured Dashboards

Pin a dashboard as "featured":
- Appears first when opening Power BI Service
- One featured dashboard per user

## Dashboard Design Best Practices

### Layout Principles

**Executive summary layout:**
```
[KPI Card 1] [KPI Card 2] [KPI Card 3] [KPI Card 4]

[Trend Chart - Large]              [Comparison Chart]

[Top Performers Table]             [Geographic Map]
```

### Recommended Practices

1. **Limit tile count**: 6-10 tiles maximum
2. **KPIs prominent**: Place key metrics at top
3. **Consistent sizing**: Similar visuals same size
4. **Clear titles**: Make tile purpose obvious
5. **Action-oriented**: Show what needs attention

### What to Include

- High-level KPIs (current vs. target)
- Key trends (daily/weekly/monthly)
- Exception highlights (alerts, anomalies)
- Quick access to detailed reports

### What to Avoid

- Too many tiles (overwhelming)
- Complex visuals (save for reports)
- Excessive text (dashboards are visual)
- Unrelated metrics (focus on one domain)

## Dashboard Security

### Sharing Dashboards

- Share directly with users or groups
- Assign view or edit permissions
- Create apps for broader distribution

### Row-Level Security

RLS defined in the dataset affects dashboards:
- Users see only their authorized data
- Same tile shows different values per user

## Summary

- Dashboards aggregate pinned tiles from multiple reports for at-a-glance monitoring
- Data alerts send notifications when metrics cross defined thresholds
- Cards, gauges, and KPIs support alert configuration
- Dashboard design should prioritize clarity and key metrics
- Subscriptions enable scheduled email delivery of dashboard snapshots

## Additional Resources

- [Dashboard Creation Guide](https://docs.microsoft.com/en-us/power-bi/consumer/end-user-dashboards) - Official documentation
- [Data Alerts](https://docs.microsoft.com/en-us/power-bi/create-reports/service-set-data-alerts) - Alert configuration guide
