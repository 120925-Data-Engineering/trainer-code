# Charts and Visualization in Streamlit

## Learning Objectives
- Create built-in Streamlit charts
- Integrate Plotly for interactive visualizations
- Use Altair for declarative charting
- Choose the right charting approach

## Why This Matters

Data visualization is why you built this application. While Streamlit's quick charts work for simple cases, production dashboards often need the interactivity and customization of Plotly or Altair.

## Built-in Streamlit Charts

Fast, simple charts for quick visualization.

### st.line_chart()

```python
import streamlit as st
import pandas as pd
import numpy as np

# Simple line chart
chart_data = pd.DataFrame(
    np.random.randn(20, 3),
    columns=['A', 'B', 'C']
)
st.line_chart(chart_data)
```

### st.bar_chart()

```python
st.bar_chart(chart_data)
```

### st.area_chart()

```python
st.area_chart(chart_data)
```

### Limitations of Built-in Charts

- Limited customization
- No axis formatting
- No hover details
- Basic colors only

Use for quick exploration; switch to Plotly/Altair for production.

## Plotly Integration

Professional interactive charts.

### Installation

```bash
pip install plotly
```

### Basic Plotly Chart

```python
import streamlit as st
import plotly.express as px
import pandas as pd

df = pd.DataFrame({
    "Region": ["North", "South", "East", "West"],
    "Sales": [150, 120, 95, 85]
})

fig = px.bar(df, x="Region", y="Sales", title="Sales by Region")
st.plotly_chart(fig, use_container_width=True)
```

### Common Plotly Charts

**Line chart:**
```python
fig = px.line(df, x="Date", y="Sales", color="Region")
```

**Scatter plot:**
```python
fig = px.scatter(df, x="Price", y="Quantity", size="Revenue", color="Category")
```

**Pie chart:**
```python
fig = px.pie(df, values="Sales", names="Region")
```

**Histogram:**
```python
fig = px.histogram(df, x="Amount", nbins=20)
```

### Customizing Plotly Charts

```python
fig = px.bar(df, x="Region", y="Sales")

fig.update_layout(
    title="Regional Sales Performance",
    xaxis_title="Region",
    yaxis_title="Sales ($)",
    template="plotly_dark"
)

fig.update_traces(marker_color="steelblue")

st.plotly_chart(fig)
```

## Altair Integration

Declarative visualization with Vega-Lite.

### Installation

```bash
pip install altair
```

### Basic Altair Chart

```python
import streamlit as st
import altair as alt
import pandas as pd

df = pd.DataFrame({
    "Region": ["North", "South", "East", "West"],
    "Sales": [150, 120, 95, 85]
})

chart = alt.Chart(df).mark_bar().encode(
    x="Region",
    y="Sales"
)

st.altair_chart(chart, use_container_width=True)
```

### Interactive Altair

```python
chart = alt.Chart(df).mark_circle().encode(
    x="Price:Q",
    y="Quantity:Q",
    size="Revenue:Q",
    color="Category:N",
    tooltip=["Product", "Revenue"]
).interactive()

st.altair_chart(chart)
```

## Choosing the Right Approach

| Need | Recommendation |
|------|----------------|
| Quick exploration | Built-in charts |
| Interactive dashboards | Plotly |
| Declarative, layered charts | Altair |
| Complex statistical plots | Plotly |
| Small, clean visualizations | Altair |

## Summary

- Built-in charts (st.line_chart) are fast but limited
- Plotly provides rich interactivity and customization
- Altair offers declarative, grammar-of-graphics style
- Use `st.plotly_chart()` and `st.altair_chart()` for integration

## Additional Resources

- [Plotly Express](https://plotly.com/python/plotly-express/) - Plotly documentation
- [Altair Documentation](https://altair-viz.github.io/) - Altair guide
