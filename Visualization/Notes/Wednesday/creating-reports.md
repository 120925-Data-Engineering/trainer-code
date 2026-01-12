# Creating Reports and Choosing Visuals

## Learning Objectives
- Select appropriate visual types for different data scenarios
- Configure visual options effectively
- Understand when to use each chart type
- Build effective data tables and matrices

## Why This Matters

The wrong chart can hide insights or mislead users. A pie chart with 50 slices is unreadable. A line chart without trend data is misleading. Choosing the right visual is as important as having accurate data.

This module guides you through Power BI's visual library and when to use each type.

## Visual Categories

Power BI organizes visuals into functional categories:

### Comparison Visuals
Show relative values across categories.

### Trend Visuals
Display changes over time.

### Part-to-Whole Visuals
Show composition and proportions.

### Relationship Visuals
Reveal connections between variables.

### Distribution Visuals
Display data spread and frequency.

### Tables and Matrices
Show detailed data with rows and columns.

## Bar and Column Charts

**Use when:** Comparing values across categories

### Clustered Bar/Column
Side-by-side bars for comparing groups:
- Column (vertical): Category labels fit horizontally
- Bar (horizontal): Long category labels

### Stacked Bar/Column
Bars divided into segments showing composition:
- Shows total and breakdown simultaneously
- Harder to compare middle segments

### 100% Stacked
All bars equal height, showing percentage breakdown:
- Compare proportions, not absolute values
- Good for composition comparison

**Best practices:**
- Start axis at zero
- Limit to 6-8 categories
- Sort by value (usually descending)
- Use consistent colors for same categories across visuals

## Line Charts

**Use when:** Showing trends over continuous time

### Single Line
One metric over time:
- Sales trend
- Stock price history
- Temperature changes

### Multiple Lines
Compare several metrics or categories:
- Limit to 4-5 lines for readability
- Use distinct colors
- Consider legend placement

### Area Charts
Line with filled area below:
- Emphasizes magnitude
- Stacked area shows composition over time

**Best practices:**
- Time on X-axis (left to right)
- Include clear date labels
- Consider data point markers for sparse data

## Pie and Donut Charts

**Use when:** Showing parts of a whole (limited categories)

### When to Use
- Maximum 5-6 slices
- Percentages sum to 100%
- One dominant category to highlight

### When to Avoid
- Many small slices
- Comparing across multiple pies
- Exact value comparison needed

**Best practices:**
- Order slices by size (largest first)
- Use labels for percentages
- Consider a bar chart as alternative

## Cards and KPIs

**Use when:** Highlighting single important metrics

### Card Visual
Single large number with optional category label:
- Total Sales: $1.2M
- Active Customers: 4,521

### Multi-row Card
Several KPIs in a compact format.

### KPI Visual
Shows actual vs. target with trend:
- Current value
- Goal
- Trend indicator

**Best practices:**
- Place prominently (top of page)
- Use clear formatting (currency, thousands)
- Include context (period, comparison)

## Tables

**Use when:** Users need exact values or want to look up specific records

### Configuration Options
- Column formatting (alignment, colors)
- Totals and subtotals
- Conditional formatting
- URL links

### When Tables Work Best
- Detailed data exploration
- Supporting drill-through pages
- Export-ready data views

**Best practices:**
- Limit columns to essential data
- Use conditional formatting sparingly
- Right-align numbers, left-align text

## Matrix

**Use when:** Cross-tabulating data by two dimensions

### Features
- Row and column hierarchies
- Expand/collapse levels
- Subtotals and grand totals
- Stepped or tabular layout

### Example
Sales by Region and Quarter:
```
              Q1      Q2      Q3      Q4    Total
North        100     120     130     115     465
South         80      90      95     100     365
East         110     125     140     135     510
West          90     100     110     105     405
Total        380     435     475     455   1,745
```

## Maps

**Use when:** Displaying geographic patterns

### Map Types

| Type | Use Case |
|------|----------|
| **Map (bubble)** | Points sized by value |
| **Filled map** | Regions colored by value |
| **Shape map** | Custom region shapes |
| **ArcGIS** | Advanced geographic analysis |

### Geographic Fields
Power BI auto-detects:
- Country/Region
- State/Province
- City
- Postal code
- Latitude/Longitude

**Best practices:**
- Use filled maps for regional comparisons
- Use bubble maps for point locations
- Consider colorblind-friendly palettes

## Scatter Charts

**Use when:** Showing relationship between two variables

### Configuration
- X-axis: First measure
- Y-axis: Second measure
- Size: Third measure (optional)
- Legend: Category grouping

### Use Cases
- Correlation analysis
- Cluster identification
- Outlier detection

## Gauges

**Use when:** Showing progress toward a goal

### Configuration
- Value: Current metric
- Minimum: Scale start
- Maximum: Scale end
- Target: Goal line

**Best practices:**
- Use for single KPIs with clear targets
- Consider cards or KPI visuals as alternatives
- Avoid multiple gauges on one page

## Selecting the Right Visual

### Decision Guide

| Data Question | Recommended Visual |
|---------------|-------------------|
| How do values compare? | Bar/Column chart |
| What is the trend over time? | Line chart |
| What is the breakdown? | Pie (few) or Stacked bar |
| What is the exact value? | Card or Table |
| How are X and Y related? | Scatter chart |
| Where is activity? | Map |
| Are we on target? | Gauge or KPI |

## Visual Configuration

### Common Settings

All visuals share configuration options in the Format pane:

| Category | Settings |
|----------|----------|
| **Visual** | Size, position, alt text |
| **General** | Title, background, border |
| **X/Y Axis** | Labels, range, gridlines |
| **Data labels** | Show values on visuals |
| **Legend** | Position, title, colors |
| **Colors** | Series colors, conditional colors |

### Interaction Settings

Control how visuals affect each other:
1. Select a visual
2. Go to Format > Edit interactions
3. For each other visual, choose: Filter, Highlight, or None

## Summary

- Choose visuals based on the question being answered
- Bar/column for comparison, line for trends, pie for limited composition
- Tables and matrices provide detailed data exploration
- Cards and KPIs highlight key metrics prominently
- Configure interactions to create a cohesive, interactive experience

## Additional Resources

- [Visual Types in Power BI](https://docs.microsoft.com/en-us/power-bi/visuals/power-bi-visualization-types-for-reports-and-q-and-a) - Official catalog
- [Choosing the Right Chart](https://docs.microsoft.com/en-us/power-bi/guidance/report-user-experience-visual-selection) - Selection guidance
