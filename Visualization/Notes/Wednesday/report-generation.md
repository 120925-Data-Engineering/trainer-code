# Report Generation in Power BI

## Learning Objectives
- Understand Power BI report structure and components
- Apply design principles for effective data visualization
- Create well-organized report layouts
- Navigate between report pages effectively

## Why This Matters

A report is more than a collection of charts---it tells a story with data. Well-designed reports enable users to quickly find insights and make decisions. Poorly designed reports confuse users and undermine trust in the data.

This week you transition from building data models to designing the user experience. Report generation skills complement your data engineering expertise by helping you understand how analysts consume the data you prepare.

## Report Structure

A Power BI report contains multiple levels of organization:

### Hierarchy

```
Report (.pbix file)
  |-- Page 1 (Report Page)
  |     |-- Visual 1 (Chart/Table)
  |     |-- Visual 2
  |     |-- Visual 3
  |-- Page 2
  |     |-- Visual 1
  |     |-- Visual 2
  |-- Page 3
        ...
```

### Components

| Component | Description |
|-----------|-------------|
| **Report** | The complete .pbix file containing all pages and data |
| **Page** | A single canvas containing visuals (like a slide) |
| **Visual** | Individual chart, table, card, or other element |
| **Filter** | Controls what data appears in visuals |
| **Bookmark** | Saved view state for navigation |

## Pages: Organizing Your Report

### Page Types

| Type | Purpose | Example |
|------|---------|---------|
| **Overview/Summary** | High-level KPIs and trends | Executive dashboard |
| **Detail** | Drill-down into specific areas | Sales by region detail |
| **Tooltip** | Custom hover information | Product details on hover |
| **Drill-through** | Context-specific deep dive | Customer profile |

### Page Naming

Use clear, descriptive page names:
- "Sales Overview" not "Page 1"
- "Customer Analysis" not "Customers"
- "Monthly Trends" not "Trends"

### Page Navigation

**Tab navigation**: Users click tabs at the bottom (default)

**Button navigation**: Create custom navigation:
1. Insert a button (Insert > Buttons)
2. Set Action type to "Page navigation"
3. Select destination page

## Visual Layout Best Practices

### The Z-Pattern

Users typically scan from top-left to bottom-right in a Z pattern:

```
[1. Most important KPIs]  -----> [2. Key context]
         |
         v
[3. Supporting details]   -----> [4. Drill-down options]
```

Place critical information where eyes naturally go first.

### Grid Alignment

Align visuals to a consistent grid:
- Enable View > Gridlines and Snap to grid
- Use consistent spacing between visuals
- Align edges of related visuals

### Visual Hierarchy

Create hierarchy through:
- **Size**: Larger visuals draw more attention
- **Position**: Top-left most prominent
- **Color**: Bold colors stand out
- **Contrast**: High contrast for important elements

### Whitespace

Leave breathing room:
- Avoid filling every pixel
- Group related visuals with space between groups
- Margins around the page edges

## Report Canvas Settings

### Page Size Options

| Option | Dimensions | Use Case |
|--------|------------|----------|
| **16:9** | 1280 x 720 | Standard presentations, monitors |
| **4:3** | 960 x 720 | Older displays |
| **Letter** | 816 x 1056 | Print-ready reports |
| **Custom** | Your choice | Specific display requirements |

### Accessing Canvas Settings

1. Click empty canvas area (no visual selected)
2. Open Format pane
3. Expand "Canvas settings"

### Background and Wallpaper

- **Canvas background**: Behind all visuals
- **Wallpaper**: Behind the canvas (visible in margins)

Use subtle colors that do not compete with data.

## Report Themes

Themes provide consistent styling across all visuals.

### Built-in Themes

1. Go to View > Themes
2. Browse the theme gallery
3. Click to apply

### Custom Themes

Create a JSON theme file for brand consistency:

```json
{
  "name": "Company Theme",
  "foreground": "#333333",
  "background": "#FFFFFF",
  "dataColors": ["#0066CC", "#FF6600", "#00CC66"],
  "tableAccent": "#0066CC"
}
```

Apply: View > Themes > Browse for themes

### Theme Elements

| Element | Controls |
|---------|----------|
| **dataColors** | Palette for series colors |
| **foreground** | Default text color |
| **background** | Default visual background |
| **tableAccent** | Headers and totals in tables |
| **visualStyles** | Per-visual-type defaults |

## Filter Types

Filters control what data appears in visuals.

### Filter Levels

| Level | Scope | Location |
|-------|-------|----------|
| **Visual-level** | Single visual | Right-click visual > Filters |
| **Page-level** | All visuals on current page | Filters pane |
| **Report-level** | All pages | Filters pane (report section) |

### Slicers vs. Filters

| Slicers | Filters Pane |
|---------|--------------|
| Visible on canvas | Hidden in pane (can be hidden/shown) |
| User-interactive | Can be locked for view-only |
| Take up visual space | No canvas space |
| Part of the report design | Administrative control |

## Headers and Titles

### Visual Titles

Each visual has a title property:
- Enable/disable in Format > Title
- Customize font, size, color
- Use concise, descriptive titles

### Page Titles

Add text boxes for page headers:
1. Insert > Text box
2. Position at top of page
3. Apply consistent styling

### Title Best Practices

- Be specific: "Monthly Sales Trend" not "Chart"
- Include context: "Sales by Region (2023 YTD)"
- Avoid redundancy with axis labels

## Mobile Layouts

Power BI supports phone-optimized layouts:

1. Go to View > Mobile layout
2. Drag visuals to the phone canvas
3. Resize for mobile viewing

Mobile layout is separate from desktop---changes do not affect each other.

## Summary

- Reports contain pages, and pages contain visuals
- Use the Z-pattern for visual layout with important content top-left
- Apply consistent themes for brand alignment
- Filters operate at visual, page, and report levels
- Consider mobile layout for on-the-go access

## Additional Resources

- [Report Design Guide](https://docs.microsoft.com/en-us/power-bi/create-reports/desktop-report-themes) - Theme documentation
- [Power BI Layout Best Practices](https://docs.microsoft.com/en-us/power-bi/guidance/report-page-tooltips) - Microsoft guidance
