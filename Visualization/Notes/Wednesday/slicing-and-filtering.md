# Slicing and Filtering in Power BI

## Learning Objectives
- Create and configure slicers for interactive filtering
- Understand filter hierarchy (visual, page, report levels)
- Control filter interactions between visuals
- Build effective filter experiences for users

## Why This Matters

Slicers and filters transform static reports into interactive exploration tools. Users expect to drill into data, compare scenarios, and answer their own questions without requesting new reports. Effective filtering design is critical to user satisfaction.

## Slicers

Slicers are visual filter controls placed on the report canvas.

### Creating Slicers

1. Select the Slicer visual from the Visualizations pane
2. Drag a field to the slicer
3. Configure appearance and behavior in Format pane

### Slicer Types

| Field Type | Slicer Style Options |
|------------|---------------------|
| **Text** | List, Dropdown, Tile |
| **Numeric** | Between, Less than, Greater than |
| **Date** | Between, Relative, Slider |
| **Hierarchy** | Dropdown with levels |

### Date Slicers

Date fields offer specialized options:

**Relative date:** Filter to dynamic periods
- Last 7 days
- This month
- Last quarter rate

**Between:** Fixed date range selector

**Slider:** Click and drag date range

### Slicer Formatting

Common formatting options:
- **Single select vs. Multi-select** (Ctrl+click for multi)
- **Show "Select All" option**
- **Orientation** (vertical list vs. horizontal tiles)
- **Header** (show or hide field name)
- **Search** (enable for long lists)

### Sync Slicers Across Pages

Make a slicer affect multiple pages:

1. Go to View > Sync slicers
2. Select your slicer
3. Check pages to sync
4. Choose visibility per page

This enables consistent filtering across a multi-page report.

## Filter Pane

The Filter pane provides administrative control over filtering.

### Filter Levels

| Level | Scope | Use Case |
|-------|-------|----------|
| **Visual-level** | Single visual | Show only specific data in one chart |
| **Page-level** | All visuals on page | Context for entire page |
| **Report-level** | All pages | Global constraint (e.g., date range) |

### Adding Filters

1. Select the visual (for visual-level) or click empty canvas
2. Drag fields to appropriate filter section in Filters pane
3. Configure filter type and values

### Filter Types

| Type | Description |
|------|-------------|
| **Basic filtering** | Check/uncheck values from a list |
| **Advanced filtering** | Contains, starts with, is blank, etc. |
| **Top N** | Show top/bottom N by a measure |
| **Relative date** | Dynamic date filtering |

### Filter Pane Visibility

Control user access to the filter pane:
- **Show pane but lock filters:** Users see but cannot change
- **Hide pane entirely:** Only slicer interaction
- **Full access:** Users can modify all filters

Configure in View > Filters > Format filter pane.

## Filter Interactions

Control how selecting data in one visual affects others.

### Default Behavior

When you select a bar in a chart:
- Other visuals **highlight** related data
- Or other visuals **filter** to selected data

### Configuring Interactions

1. Select the visual that initiates the filter
2. Go to Format > Edit interactions
3. For each other visual, select: Filter, Highlight, or None

### Interaction Icons

| Icon | Meaning |
|------|---------|
| Filter funnel | Filter: Shows only matching data |
| Bar with highlight | Highlight: Dims non-matching data |
| Circle with line | None: No effect |

### When to Use Each

| Interaction | Best For |
|-------------|----------|
| **Highlight** | Showing context (what changes, what stays) |
| **Filter** | Focusing analysis (removing distractions) |
| **None** | Independent visuals (totals, KPIs) |

## Drill-Through Filtering

Drill-through enables navigation to detailed pages with context.

### Creating Drill-Through

1. Create a detail page (e.g., "Customer Details")
2. Add a field to the drill-through filter wells on that page
3. On other pages, right-click a data point
4. Select Drill through > [Detail page name]

### How It Works

When drilling through:
- Filter context from the source visual carries over
- Detail page shows data for selected item only
- Back button returns to source page

### Drill-Through Best Practices

- Create specific detail pages (Customer, Product, Region)
- Include Back button for navigation
- Show the drill-through context clearly on the detail page

## Cross-Filtering Patterns

### Pattern 1: Global Date Slicer

One date slicer at top filters all visuals:
- Sync across all pages
- Set to filter (not highlight)
- Use relative date for dynamic defaults

### Pattern 2: Category Hierarchy

Linked slicers for hierarchical filtering:
1. Select Region -> filters States slicer
2. Select State -> filters Cities slicer

Requires setting appropriate filter interactions.

### Pattern 3: Comparison Mode

Side-by-side visuals with independent filters:
- Duplicate visuals
- Set interactions to "None" between pairs
- Separate slicers for each set

### Pattern 4: Spotlight

Click-to-focus interface:
- Clicking a chart segment filters the page
- Provides quick drill-down without slicers
- Good for exploratory analysis

## Bookmarks for Filter States

Save filter configurations as bookmarks:

1. Set desired filter state
2. Go to View > Bookmarks
3. Click "Add" to save current state
4. Name the bookmark (e.g., "2023 Electronics View")

Use buttons to navigate between bookmarks:
1. Insert a button
2. Set Action to "Bookmark"
3. Select the target bookmark

## Performance Considerations

### Slicer Performance

Large slicers (thousands of items) can slow page load:
- Consider dropdown instead of list
- Enable search for filtering options
- Limit visible items with "Top N" filter

### Filter Complexity

Each filter adds query complexity:
- Combine related filters when possible
- Avoid redundant filters at multiple levels
- Test with Performance Analyzer

## Summary

- Slicers provide user-friendly visual filtering controls
- Filter pane offers visual, page, and report-level filtering
- Configure interactions to control cross-visual filtering behavior
- Drill-through enables context-aware navigation to detail pages
- Bookmarks save filter states for one-click navigation
- Consider performance when adding many slicers or complex filters

## Additional Resources

- [Slicers in Power BI](https://docs.microsoft.com/en-us/power-bi/visuals/power-bi-visualization-slicers) - Slicer documentation
- [Filter Pane Guide](https://docs.microsoft.com/en-us/power-bi/create-reports/power-bi-report-filter) - Filter configuration
- [Drill-through Documentation](https://docs.microsoft.com/en-us/power-bi/create-reports/desktop-drillthrough) - Drill-through setup
