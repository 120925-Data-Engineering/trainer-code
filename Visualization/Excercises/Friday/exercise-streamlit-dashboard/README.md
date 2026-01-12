# Exercise: Building a Streamlit Dashboard

## Overview
**Day:** 5-Friday
**Duration:** 2-3 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Streamlit setup exercise completed

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Streamlit Core Components | [streamlit-core-components.md](../../content/5-Friday/streamlit-core-components.md) | Layouts, widgets, containers |
| Streamlit Charts | [streamlit-charts.md](../../content/5-Friday/streamlit-charts.md) | Built-in charts, Plotly integration |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Build a multi-section dashboard with proper layout
2. Use columns, containers, and expanders for organization
3. Create interactive charts that respond to user input
4. Apply consistent styling and formatting

---

## The Scenario
Build a sales analytics dashboard that allows users to explore data through interactive filters and multiple visualization types.

---

## Core Tasks

### Task 1: Dashboard Structure (30 mins)

Create `dashboard.py` with the following structure:

```python
import streamlit as st
import pandas as pd
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Sales Analytics Dashboard",
    page_icon=":chart_with_upwards_trend:",
    layout="wide"
)

# Title
st.title("Sales Analytics Dashboard")
st.markdown("Interactive sales data exploration")

# Generate sample data
@st.cache_data
def load_data():
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=365, freq="D")
    
    data = {
        "date": dates,
        "revenue": np.random.uniform(8000, 15000, 365),
        "orders": np.random.randint(50, 150, 365),
        "region": np.random.choice(["North", "South", "East", "West"], 365),
        "category": np.random.choice(["Electronics", "Clothing", "Home", "Books"], 365)
    }
    return pd.DataFrame(data)

df = load_data()
```

**Checkpoint:** Basic structure created with sample data.

---

### Task 2: Sidebar Filters (25 mins)

Add sidebar with filters:

```python
# Sidebar
st.sidebar.header("Filters")

# Date range filter
st.sidebar.subheader("Date Range")
date_range = st.sidebar.date_input(
    "Select dates",
    value=(df["date"].min(), df["date"].max()),
    min_value=df["date"].min(),
    max_value=df["date"].max()
)

# Region filter
regions = st.sidebar.multiselect(
    "Select Regions",
    options=df["region"].unique(),
    default=df["region"].unique()
)

# Category filter
categories = st.sidebar.multiselect(
    "Select Categories",
    options=df["category"].unique(),
    default=df["category"].unique()
)

# Apply filters
filtered_df = df[
    (df["date"] >= pd.Timestamp(date_range[0])) &
    (df["date"] <= pd.Timestamp(date_range[1])) &
    (df["region"].isin(regions)) &
    (df["category"].isin(categories))
]

# Show filter stats
st.sidebar.divider()
st.sidebar.metric("Records Selected", len(filtered_df))
st.sidebar.metric("% of Total", f"{len(filtered_df)/len(df)*100:.1f}%")
```

**Checkpoint:** Sidebar filters working.

---

### Task 3: KPI Metrics Row (20 mins)

Add key metrics at the top:

```python
# KPI Metrics
st.subheader("Key Performance Indicators")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_revenue = filtered_df["revenue"].sum()
    st.metric("Total Revenue", f"${total_revenue:,.0f}")

with col2:
    total_orders = filtered_df["orders"].sum()
    st.metric("Total Orders", f"{total_orders:,}")

with col3:
    avg_order = total_revenue / total_orders if total_orders > 0 else 0
    st.metric("Avg Order Value", f"${avg_order:.2f}")

with col4:
    days = len(filtered_df["date"].unique())
    st.metric("Days", days)

st.divider()
```

**Checkpoint:** KPI row displaying correctly.

---

### Task 4: Charts Section (45 mins)

Add multiple chart types:

```python
# Charts Row 1
st.subheader("Revenue Analysis")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.markdown("**Revenue Trend**")
    daily_revenue = filtered_df.groupby("date")["revenue"].sum().reset_index()
    st.line_chart(daily_revenue.set_index("date"))

with chart_col2:
    st.markdown("**Revenue by Region**")
    region_revenue = filtered_df.groupby("region")["revenue"].sum()
    st.bar_chart(region_revenue)

st.divider()

# Charts Row 2
col1, col2 = st.columns(2)

with col1:
    st.markdown("**Revenue by Category**")
    category_revenue = filtered_df.groupby("category")["revenue"].sum()
    st.bar_chart(category_revenue)

with col2:
    st.markdown("**Orders by Region**")
    region_orders = filtered_df.groupby("region")["orders"].sum()
    st.bar_chart(region_orders)
```

**Checkpoint:** All charts rendering correctly.

---

### Task 5: Data Table with Expander (20 mins)

Add detailed data view:

```python
# Detailed Data Section
st.subheader("Detailed Data")

with st.expander("View Raw Data"):
    # Column selection
    columns_to_show = st.multiselect(
        "Select columns",
        options=filtered_df.columns.tolist(),
        default=filtered_df.columns.tolist()
    )
    
    # Display data
    st.dataframe(filtered_df[columns_to_show], use_container_width=True)
    
    # Download button
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="filtered_sales_data.csv",
        mime="text/csv"
    )
```

**Checkpoint:** Expander with data table and download working.

---

### Task 6: Tabs for Different Views (25 mins)

Add tabbed interface:

```python
# Tabbed Interface
st.subheader("Analysis Views")

tab1, tab2, tab3 = st.tabs(["Summary", "Trends", "Comparisons"])

with tab1:
    st.markdown("### Summary Statistics")
    summary_stats = filtered_df.describe()
    st.dataframe(summary_stats)

with tab2:
    st.markdown("### Monthly Trends")
    filtered_df["month"] = filtered_df["date"].dt.to_period("M").astype(str)
    monthly = filtered_df.groupby("month")[["revenue", "orders"]].sum()
    st.line_chart(monthly)

with tab3:
    st.markdown("### Region Comparison")
    comparison = filtered_df.groupby(["region", "category"])["revenue"].sum().unstack()
    st.bar_chart(comparison)
```

**Checkpoint:** Tabs switching correctly with different content.

---

### Task 7: Final Polish (15 mins)

Add finishing touches:

```python
# Footer
st.divider()
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>Sales Analytics Dashboard | Week 6 Visualization Training</p>
        <p style='color: gray; font-size: 12px;'>Last updated: Dynamic with each filter change</p>
    </div>
    """,
    unsafe_allow_html=True
)

# Sidebar footer
st.sidebar.divider()
st.sidebar.caption("Dashboard v1.0")
```

**Run and test:**
```bash
streamlit run dashboard.py
```

**Checkpoint:** Complete dashboard with all elements.

---

## Deliverables

Submit the following:

1. **dashboard.py:** Complete dashboard code
2. **Screenshot 1:** Full dashboard view
3. **Screenshot 2:** Dashboard with filters applied
4. **Screenshot 3:** Expanded data table with download

---

## Definition of Done

- [ ] Wide layout configured
- [ ] Sidebar with at least 3 filter types
- [ ] KPI metrics row with 4 metrics
- [ ] At least 4 different charts
- [ ] Expander with data table
- [ ] Download functionality working
- [ ] Tabs with different views
- [ ] Proper styling and organization

---

## Stretch Goals (Optional)

1. Add Plotly charts for more interactivity
2. Implement date comparison (this period vs last period)
3. Add a "Reset Filters" button
4. Create calculated metrics that update with filters

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Charts not updating | Verify filtered_df is used, not df |
| Date filter error | Ensure dates are converted with pd.Timestamp |
| Columns misaligned | Check equal column counts in st.columns |
| Download not working | Verify csv variable is properly formatted |
| Layout issues | Try different layout options (wide/centered) |
