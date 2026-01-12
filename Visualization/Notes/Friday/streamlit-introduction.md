# Introduction to Streamlit

## Learning Objectives
- Understand what Streamlit is and its architecture
- Identify when Streamlit is the right tool
- Recognize Streamlit's role in the data engineering ecosystem
- Compare Streamlit with traditional BI tools

## Why This Matters

Power BI excels at enterprise reporting but sometimes you need something different: a custom data application that solves a specific problem. Streamlit lets you build interactive web applications using only Python---no HTML, CSS, or JavaScript required.

As a data engineer fluent in Python, Streamlit extends your skills into the visualization layer. You can rapidly prototype dashboards, build internal tools, and create data applications that integrate directly with your existing Python codebase.

## What is Streamlit?

Streamlit is an open-source Python framework for building data applications without web development expertise.

### Core Concept

Streamlit turns Python scripts into interactive web apps:

```python
import streamlit as st

st.title("Hello World")
st.write("This is my first Streamlit app!")
```

Run with `streamlit run app.py` and a browser opens with your app.

### Architecture

Streamlit works differently from traditional web frameworks:

| Aspect | Traditional Web | Streamlit |
|--------|----------------|-----------|
| **Code structure** | Routes, templates, views | Single script, top-to-bottom |
| **State management** | Sessions, databases | Session state, caching |
| **Deployment** | Complex servers | Simple (Python + dependencies) |
| **Skills needed** | HTML, CSS, JS, backend | Python only |

### The Streamlit Run Loop

When a user interacts with your app:
1. The entire script re-runs from top to bottom
2. Widgets remember their state
3. Cached functions return stored results
4. Updated output appears instantly

This "script re-run" model is simple but powerful.

## When to Use Streamlit

### Ideal Use Cases

| Scenario | Why Streamlit |
|----------|---------------|
| **Internal tools** | Quick to build, easy to maintain |
| **Data exploration** | Interactive queries and charts |
| **Prototyping** | Rapid iteration before production |
| **ML model demos** | Show predictions with real inputs |
| **Data quality reports** | Custom validation dashboards |
| **Admin interfaces** | CRUD operations for data |

### Compared to Power BI

| Factor | Streamlit | Power BI |
|--------|-----------|----------|
| **Flexibility** | Full Python control | Constrained to DAX/visuals |
| **Custom logic** | Native Python code | Limited to measures |
| **Learning curve** | Python knowledge | Tool-specific skills |
| **Sharing** | Deploy as web app | Power BI Service |
| **Enterprise features** | Limited | Extensive (RLS, governance) |
| **Cost** | Free, open-source | License required |

### When NOT to Use Streamlit

- Enterprise-wide BI with governance needs
- Complex drill-through reports
- Pixel-perfect report formatting
- Large user base with row-level security
- Integration with Microsoft ecosystem

## Streamlit in the Data Stack

Streamlit connects directly to your data sources:

```
Data Sources --> Snowflake --> Streamlit App --> Users
                    ^
                    |
            (Python querying)
```

### Integration Points

- Query Snowflake, PostgreSQL, BigQuery directly
- Use pandas DataFrames for data manipulation
- Connect to REST APIs
- Load machine learning models
- Access file systems and cloud storage

### With Your Week 5 Work

Streamlit can consume the Snowflake data warehouse you built:
- Query dbt-transformed models
- Display warehouse metrics
- Build monitoring dashboards

## Key Features Overview

### Display Elements

| Function | Purpose |
|----------|---------|
| `st.write()` | Smart display (text, data, charts) |
| `st.title()` | Page title |
| `st.header()` | Section header |
| `st.markdown()` | Formatted markdown |
| `st.dataframe()` | Interactive table |
| `st.metric()` | KPI card |

### Input Widgets

| Widget | Purpose |
|--------|---------|
| `st.button()` | Clickable button |
| `st.selectbox()` | Dropdown selection |
| `st.slider()` | Numeric range |
| `st.text_input()` | Text entry |
| `st.date_input()` | Date picker |
| `st.file_uploader()` | File upload |

### Charts

| Function | Purpose |
|----------|---------|
| `st.line_chart()` | Quick line chart |
| `st.bar_chart()` | Quick bar chart |
| `st.plotly_chart()` | Plotly integration |
| `st.altair_chart()` | Altair integration |

### Layout

| Function | Purpose |
|----------|---------|
| `st.sidebar` | Left sidebar |
| `st.columns()` | Multi-column layout |
| `st.expander()` | Collapsible sections |
| `st.tabs()` | Tabbed interface |

## Your First Streamlit App

A complete example:

```python
import streamlit as st
import pandas as pd

# Title
st.title("Sales Dashboard")

# Sidebar for filters
region = st.sidebar.selectbox("Select Region", ["All", "North", "South", "East", "West"])

# Sample data
data = pd.DataFrame({
    "Region": ["North", "South", "East", "West"],
    "Sales": [100000, 85000, 92000, 78000]
})

# Apply filter
if region != "All":
    data = data[data["Region"] == region]

# Display metrics
col1, col2 = st.columns(2)
col1.metric("Total Sales", f"${data['Sales'].sum():,.0f}")
col2.metric("Regions", len(data))

# Show chart
st.bar_chart(data.set_index("Region"))

# Show data table
st.dataframe(data)
```

This creates a complete interactive dashboard with filtering, KPIs, charts, and data tables---all in about 30 lines of Python.

## Summary

- Streamlit transforms Python scripts into interactive web applications
- The "script re-run" model simplifies development but requires understanding
- Ideal for internal tools, prototypes, and data exploration
- Complements Power BI for scenarios requiring custom Python logic
- Connects directly to data sources like Snowflake using Python libraries

## Additional Resources

- [Streamlit Documentation](https://docs.streamlit.io/) - Official documentation
- [Streamlit Gallery](https://streamlit.io/gallery) - Example applications
- [Streamlit Community](https://discuss.streamlit.io/) - Forums and support
