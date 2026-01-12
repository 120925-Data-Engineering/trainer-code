# Streamlit Core Components

## Learning Objectives
- Use essential Streamlit display and input components
- Build interactive data displays with st.dataframe() and st.metric()
- Create user interfaces with sidebars and layout options
- Handle user input with selection widgets

## Why This Matters

Streamlit's power comes from its pre-built components. Understanding these building blocks lets you rapidly assemble interactive applications without writing custom HTML or JavaScript.

## Display Components

### st.write() - The Swiss Army Knife

`st.write()` intelligently displays almost anything:

```python
import streamlit as st
import pandas as pd

# Text
st.write("Hello, World!")

# Numbers
st.write(42)

# DataFrames
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
st.write(df)

# Multiple arguments
st.write("The answer is:", 42)
```

### Text Display

| Function | Purpose |
|----------|---------|
| `st.title()` | Large page title |
| `st.header()` | Section header |
| `st.subheader()` | Subsection header |
| `st.text()` | Fixed-width text |
| `st.markdown()` | Markdown formatting |
| `st.caption()` | Small text captions |
| `st.code()` | Code block |

```python
st.title("My Dashboard")
st.header("Sales Analysis")
st.subheader("Regional Breakdown")
st.markdown("**Bold** and *italic* text")
st.code("print('Hello')", language="python")
```

### st.dataframe() - Interactive Tables

Display pandas DataFrames with sorting and scrolling:

```python
import pandas as pd
import streamlit as st

df = pd.DataFrame({
    "Product": ["A", "B", "C"],
    "Sales": [100, 150, 80],
    "Growth": [0.1, 0.25, -0.05]
})

st.dataframe(df, height=200)
```

Configuration options:
- `height`: Fixed height in pixels
- `width`: Fixed width
- `use_container_width=True`: Fill container width
- `hide_index=True`: Hide the row index

### st.table() - Static Tables

For simple, non-interactive tables:

```python
st.table(df)
```

### st.metric() - KPI Cards

Display metrics with delta indicators:

```python
st.metric(
    label="Total Sales",
    value="$1.2M",
    delta="12%"
)
```

With negative delta:
```python
st.metric("Costs", "$500K", "-5%", delta_color="inverse")
```

Multiple metrics in columns:
```python
col1, col2, col3 = st.columns(3)
col1.metric("Sales", "$1.2M", "12%")
col2.metric("Customers", "1,234", "5%")
col3.metric("Avg Order", "$450", "-2%")
```

## Input Widgets

### st.selectbox() - Dropdown Selection

```python
option = st.selectbox(
    "Select a region",
    ["North", "South", "East", "West"]
)
st.write(f"You selected: {option}")
```

### st.multiselect() - Multiple Selection

```python
options = st.multiselect(
    "Select categories",
    ["Electronics", "Clothing", "Food", "Home"],
    default=["Electronics"]
)
```

### st.slider() - Numeric Range

```python
# Single value
age = st.slider("Select age", 0, 100, 25)

# Range
values = st.slider("Select range", 0.0, 100.0, (25.0, 75.0))
```

### st.text_input() - Text Entry

```python
name = st.text_input("Enter your name", placeholder="John Doe")
```

### st.number_input() - Numeric Entry

```python
quantity = st.number_input("Quantity", min_value=0, max_value=100, value=1, step=1)
```

### st.date_input() - Date Picker

```python
import datetime
date = st.date_input("Select date", datetime.date(2023, 1, 1))
```

### st.checkbox() - Toggle

```python
show_data = st.checkbox("Show raw data")
if show_data:
    st.dataframe(df)
```

### st.radio() - Single Selection

```python
choice = st.radio("Choose one", ["Option A", "Option B", "Option C"])
```

### st.button() - Action Button

```python
if st.button("Calculate"):
    result = expensive_computation()
    st.write(result)
```

## Layout Components

### st.sidebar - Left Sidebar

```python
with st.sidebar:
    st.header("Filters")
    region = st.selectbox("Region", ["All", "North", "South"])
    date = st.date_input("Date")
```

Or use sidebar prefix:
```python
region = st.sidebar.selectbox("Region", ["All", "North", "South"])
```

### st.columns() - Multi-Column Layout

```python
col1, col2, col3 = st.columns(3)

with col1:
    st.header("Column 1")
    st.write("Content here")

with col2:
    st.header("Column 2")
    st.metric("Value", 100)

with col3:
    st.header("Column 3")
    st.bar_chart([1, 2, 3])
```

Unequal widths:
```python
col1, col2 = st.columns([2, 1])  # 2/3 and 1/3 width
```

### st.tabs() - Tabbed Interface

```python
tab1, tab2, tab3 = st.tabs(["Overview", "Details", "Raw Data"])

with tab1:
    st.write("Overview content")

with tab2:
    st.write("Details content")

with tab3:
    st.dataframe(df)
```

### st.expander() - Collapsible Sections

```python
with st.expander("Show more details"):
    st.write("Hidden content that expands")
    st.dataframe(df)
```

### st.container() - Logical Groups

```python
container = st.container()
container.write("This appears first")
st.write("This appears second")
container.write("This also appears in the container above")
```

### st.empty() - Placeholder

```python
placeholder = st.empty()
# Later, fill it
placeholder.write("Now there's content")
```

## Complete Example

```python
import streamlit as st
import pandas as pd

# Page config
st.set_page_config(page_title="Sales Dashboard", layout="wide")

# Title
st.title("Sales Analytics Dashboard")

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    region = st.selectbox("Region", ["All", "North", "South", "East", "West"])
    date_range = st.slider("Year", 2020, 2023, (2022, 2023))
    show_details = st.checkbox("Show detailed data")

# Sample data
df = pd.DataFrame({
    "Region": ["North", "South", "East", "West"],
    "Sales": [150000, 120000, 95000, 85000],
    "Growth": [0.12, 0.08, 0.15, -0.02]
})

# Filter data
if region != "All":
    df = df[df["Region"] == region]

# KPI row
col1, col2, col3 = st.columns(3)
col1.metric("Total Sales", f"${df['Sales'].sum():,.0f}")
col2.metric("Avg Growth", f"{df['Growth'].mean():.1%}")
col3.metric("Regions", len(df))

# Charts in tabs
tab1, tab2 = st.tabs(["Charts", "Data"])

with tab1:
    st.bar_chart(df.set_index("Region")["Sales"])

with tab2:
    st.dataframe(df, use_container_width=True)

# Expandable details
if show_details:
    with st.expander("Additional Details"):
        st.write("Regional breakdown with extended metrics")
        st.table(df)
```

## Summary

- `st.write()` handles most display needs automatically
- `st.dataframe()` and `st.metric()` are essential for data display
- Input widgets (selectbox, slider, etc.) collect user input
- Layout with sidebar, columns, tabs, and expanders organizes content
- Combine components to build complete interactive applications

## Additional Resources

- [Streamlit API Reference](https://docs.streamlit.io/library/api-reference) - Complete component list
- [Layout and Containers](https://docs.streamlit.io/library/api-reference/layout) - Layout options
