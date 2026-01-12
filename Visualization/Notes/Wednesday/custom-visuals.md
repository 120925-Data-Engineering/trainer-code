# Custom Visuals in Power BI

## Learning Objectives
- Discover and import custom visuals from AppSource
- Configure popular custom visual options
- Understand when to use custom vs. built-in visuals
- Explore the Power BI visuals SDK basics

## Why This Matters

Power BI's built-in visuals cover most scenarios, but sometimes you need specialized visualizations. The Power BI marketplace (AppSource) offers hundreds of custom visuals created by Microsoft and third-party developers. Understanding how to leverage these extends your report capabilities significantly.

## The AppSource Marketplace

AppSource is Power BI's official marketplace for custom visuals.

### Accessing AppSource

1. In Power BI Desktop, click the ellipsis (...) in the Visualizations pane
2. Select "Get more visuals"
3. Browse or search the marketplace
4. Click "Add" to install

### Visual Categories in AppSource

| Category | Examples |
|----------|----------|
| **Charts** | Bullet charts, waterfall, tornado |
| **Infographics** | Chiclet slicers, timeline, word cloud |
| **Maps** | Route maps, flow maps, synoptic panels |
| **Gauges** | Linear gauges, thermometers |
| **KPIs** | Card with states, power KPI |
| **Filters** | Hierarchy slicers, date range slicers |

### Certification Badge

Look for the blue checkmark (certified) badge:
- Microsoft tested for security
- Follows coding standards
- Safe for enterprise use

## Popular Custom Visuals

### Chiclet Slicer

**Purpose:** Visual slicer with images or formatted buttons

**Use case:** Product category selection with logos, region selection with flags

**Configuration:**
- Category field for options
- Optional image URL field
- Button layout (rows x columns)

### Hierarchy Slicer

**Purpose:** Expandable tree slicer for hierarchical data

**Use case:** Geographic hierarchy (Country > State > City), product hierarchy

**Configuration:**
- Multiple fields create hierarchy levels
- Expand/collapse at each level
- Search capability

### Bullet Chart

**Purpose:** Compare actual value against target with qualitative ranges

**Use case:** KPI performance (Sales vs. Target with red/yellow/green zones)

**Configuration:**
- Value: Actual measure
- Target: Goal measure
- Minimum/Maximum: Scale bounds
- Qualitative ranges: Threshold values

### Infographic Designer

**Purpose:** Create pictograph-style visualizations

**Use case:** Representing counts with icons (people, products, etc.)

**Configuration:**
- Value field
- Icon selection
- Size and layout options

### Word Cloud

**Purpose:** Display text frequency with size indicating importance

**Use case:** Survey responses, product reviews, trending topics

**Configuration:**
- Category: Text field
- Values: Optional frequency measure
- Colors, fonts, layout

### Tornado Chart

**Purpose:** Compare two measures side-by-side

**Use case:** Before/after comparison, male/female breakdown

**Configuration:**
- Legend: Category
- Value: Two measures (one per side)

## Importing Custom Visuals

### From AppSource

1. Open Visualizations pane ellipsis
2. Select "Get more visuals"
3. Search or browse
4. Click "Add"
5. Visual appears in pane

### From File (.pbiviz)

For visuals not in AppSource (internal development or private visuals):

1. Visualizations pane ellipsis
2. Select "Import a visual from a file"
3. Browse to .pbiviz file
4. Click Open

## Managing Visuals in Your Report

### Organizational Custom Visuals

IT admins can pre-approve visuals:
1. Admin adds to organizational visuals store
2. Users access via "My organization" tab in marketplace
3. Ensures consistency and security

### Visual Version Updates

When updates are available:
- Notification appears in Visualizations pane
- Click to update to latest version
- Test for any breaking changes

## When to Use Custom Visuals

### Choose Custom When:

- Built-in visuals cannot represent your data effectively
- Specific chart type needed (bullet chart, tornado, etc.)
- Enhanced slicer functionality required
- Brand-specific visualization requirements

### Stick with Built-In When:

- Built-in visual meets the need
- Report must work in all deployment scenarios
- Security/certification is paramount
- Performance is critical (some custom visuals are slower)

## Considerations for Custom Visuals

### Performance

Custom visuals may:
- Load slower than built-in visuals
- Have larger file sizes
- Require more client-side processing

### Compatibility

Not all custom visuals support:
- Publish to web
- Embedded scenarios
- Export to PDF/PowerPoint
- Mobile view

### Updates and Support

Third-party visuals may:
- Have limited support documentation
- Update less frequently
- Become abandoned over time

## Power BI Visuals SDK (Overview)

For developers who want to create custom visuals:

### SDK Capabilities

- Create fully custom visualizations
- Use D3.js, Plotly, or other JS libraries
- Package as .pbiviz files
- Submit to AppSource

### Basic Structure

A custom visual project includes:
```
custom-visual/
  |-- src/
  |     |-- visual.ts      (main code)
  |     |-- settings.ts    (configuration options)
  |-- assets/
  |     |-- icon.png       (visual icon)
  |-- capabilities.json    (data bindings)
  |-- pbiviz.json          (metadata)
```

### Development Process

1. Install Power BI Visuals tools: `npm install -g powerbi-visuals-tools`
2. Create project: `pbiviz new <projectname>`
3. Develop and test locally
4. Package: `pbiviz package`
5. Import .pbiviz to Power BI

This is an advanced topic typically for developers, not analysts.

## Summary

- AppSource provides hundreds of certified and community custom visuals
- Popular options include hierarchy slicers, bullet charts, and infographic designers
- Look for the certification badge for enterprise-approved visuals
- Custom visuals extend capability but consider performance and compatibility
- The Visuals SDK enables creating entirely new visual types (developer topic)

## Additional Resources

- [AppSource Marketplace](https://appsource.microsoft.com/en-us/marketplace/apps?product=power-bi-visuals) - Browse visuals
- [Custom Visuals Documentation](https://docs.microsoft.com/en-us/power-bi/developer/visuals/) - Developer docs
- [Power BI Visuals Gallery](https://community.powerbi.com/t5/Power-BI-Visuals-Downloads/bd-p/PowerBIVisualsDownloads) - Community visuals
