# Conditional Formatting in Power BI

## Learning Objectives
- Apply data bars and color scales to tables and matrices
- Create rules-based conditional formatting
- Use icon sets to indicate status
- Implement DAX-driven dynamic formatting

## Why This Matters

Numbers alone can be overwhelming. Conditional formatting transforms rows of data into visual patterns that reveal insights at a glance. Red cells jump out as problems, green cells indicate success, and gradients show relative performance.

Effective conditional formatting guides users to what matters without requiring them to read every number.

## Conditional Formatting Options

Power BI provides several formatting types:

| Type | Description | Use Case |
|------|-------------|----------|
| **Background color** | Cell background changes | Highlight high/low values |
| **Font color** | Text color changes | Status indication |
| **Data bars** | In-cell bar charts | Show relative magnitude |
| **Icons** | Small icons in cells | Status symbols |
| **Web URL** | Text becomes clickable | Link to detail pages |

## Applying Basic Formatting

### Background Color

1. Select a table or matrix visual
2. In Format pane, expand the column to format
3. Click "Conditional formatting" (fx icon)
4. Select "Background color"

### Color Scale Options

| Option | Result |
|--------|--------|
| **Color scale** | Gradient from low to high |
| **Rules** | Specific colors for value ranges |
| **Field value** | Color based on another field |

### Basic Color Scale

Configure color gradient:
- **Minimum color**: Color for lowest value
- **Maximum color**: Color for highest value
- **Center color** (optional): Color for middle value
- **Summarize by**: How to aggregate (Sum, Average, etc.)

Example: Red (low) to Green (high) for sales performance

## Data Bars

In-cell bars showing relative values.

### Creating Data Bars

1. Select table column
2. Conditional formatting > Data bars
3. Configure settings

### Data Bar Settings

| Setting | Description |
|---------|-------------|
| **Show bar only** | Hide text, show only bar |
| **Minimum** | Value at which bar starts |
| **Maximum** | Value at which bar is full |
| **Positive bar** | Color for positive values |
| **Negative bar** | Color for negative values |
| **Bar direction** | Left-to-right or right-to-left |

Data bars are excellent for quickly comparing magnitudes within a column.

## Rules-Based Formatting

Define specific rules instead of gradients.

### Creating Rules

1. Conditional formatting > Background color > Rules
2. Add rules with conditions and colors

### Rule Structure

Each rule specifies:
- Field to evaluate
- Operator (greater than, between, equals)
- Value(s) to compare
- Color to apply

### Example: Traffic Light Rules

For a "Performance" measure:
| Rule | Condition | Color |
|------|-----------|-------|
| 1 | >= 1.0 | Green |
| 2 | >= 0.8 AND < 1.0 | Yellow |
| 3 | < 0.8 | Red |

### Rule Evaluation Order

Rules evaluate top to bottom:
- First matching rule applies
- Place most specific rules first
- Default (last rule) catches remaining values

## Icon Sets

Small icons indicating status.

### Adding Icons

1. Conditional formatting > Icons
2. Select icon style
3. Configure rules for each icon

### Icon Styles

Power BI offers several icon sets:
- Traffic lights (circles)
- Flags
- Signs
- Shapes
- Ratings (stars)
- Directional arrows

### Icon Configuration

For each icon, specify:
- Minimum value to show this icon
- Operator (>=, >, etc.)
- Position relative to value or field

### Icon Layout

| Layout | Description |
|--------|-------------|
| **Left of data** | Icon before number |
| **Right of data** | Icon after number |
| **Icon only** | Hide number, show only icon |

## DAX-Driven Formatting

Use measures to determine colors dynamically.

### Color Measure

Create a measure that returns a color value:

```dax
Performance Color = 
SWITCH(
    TRUE(),
    [Performance] >= 1.0, "#4CAF50",  -- Green
    [Performance] >= 0.8, "#FFC107",  -- Amber
    "#F44336"  -- Red
)
```

### Applying DAX Colors

1. Conditional formatting > Background color
2. Select "Field value"
3. Choose your color measure

### Advanced Example: Gradient Based on Rank

```dax
Rank Color = 
VAR CurrentRank = [Product Rank]
VAR MaxRank = MAXX(ALL(Dim_Product), [Product Rank])
VAR Intensity = DIVIDE(CurrentRank, MaxRank)
RETURN
    "rgba(0, 100, 200, " & FORMAT(1 - Intensity, "0.00") & ")"
```

This creates a dynamic opacity gradient based on rank.

## Formatting Matrices

Matrices have additional formatting considerations.

### Row/Column Formatting

Different formatting for:
- Row headers
- Column headers
- Values cells
- Subtotals
- Grand totals

### Subtotal Formatting

To highlight totals:
1. Format pane > Subtotals
2. Apply distinct background color
3. Set font weight to bold

### Stepped Layout Formatting

When using stepped layout:
- Consider how colors flow across indented levels
- Test readability at all hierarchy levels

## Formatting Cards and KPIs

### Card Conditional Formatting

Cards can change color based on value:
1. Select card visual
2. Format pane > Callout value > Conditional formatting
3. Configure rules or gradient

### KPI Indicators

KPI visuals have built-in conditional formatting:
- Automatically shows red/yellow/green based on target
- Configure thresholds in Format pane

## Best Practices

### Color Accessibility

- Use colorblind-friendly palettes
- Add icons or text for color-coded meaning
- Avoid red/green alone (use shapes too)

### Highlighting vs. Overcoloring

- Highlight exceptions, not all data
- Too much color is distracting
- Use neutral tones for normal values

### Consistency

- Same color meanings across report
- Green = good, red = bad (convention)
- Document color meanings if non-obvious

### Performance

- Avoid overly complex DAX color measures
- Test with large data volumes

## Summary

- Conditional formatting reveals patterns without reading every number
- Data bars show relative magnitude within columns
- Rules-based formatting assigns specific colors to value ranges
- Icons provide quick status indication
- DAX measures enable dynamic, complex formatting logic
- Follow accessibility and consistency best practices

## Additional Resources

- [Conditional Formatting Documentation](https://docs.microsoft.com/en-us/power-bi/create-reports/desktop-conditional-table-formatting) - Official guide
- [Color Accessibility Guide](https://docs.microsoft.com/en-us/power-bi/guidance/accessibility-color-contrast) - Inclusive design
