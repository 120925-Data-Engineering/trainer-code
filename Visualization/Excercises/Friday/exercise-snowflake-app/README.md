# Exercise: Streamlit App with Snowflake Connection

## Overview
**Day:** 5-Friday
**Duration:** 2-3 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Streamlit dashboard exercise completed, Snowflake account from Week 5

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Streamlit Snowflake Connection | [streamlit-snowflake-connection.md](../../content/5-Friday/streamlit-snowflake-connection.md) | Connector, caching, secrets |
| Detecting Outliers | [detecting-outliers.md](../../content/5-Friday/detecting-outliers.md) | Statistical methods (optional) |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Configure Streamlit secrets for secure credential management
2. Connect Streamlit to Snowflake and execute queries
3. Implement query caching for performance
4. Build a data application that queries the same GOLD zone used in Power BI

---

## The Scenario
Build a Streamlit application that connects to the same Snowflake GOLD zone data warehouse used in your Power BI reports. This demonstrates Python-native access to enterprise data.

---

## Core Tasks

### Task 1: Install Snowflake Connector (15 mins)

**Install package:**
```bash
# Activate your virtual environment
# Windows: .\venv\Scripts\Activate.ps1
# macOS/Linux: source venv/bin/activate

# Install Snowflake connector
pip install snowflake-connector-python

# Update requirements
pip freeze > requirements.txt
```

**Verify installation:**
```python
python -c "import snowflake.connector; print('Success!')"
```

**Checkpoint:** Snowflake connector installed.

---

### Task 2: Configure Secrets (20 mins)

**Create secrets directory:**
```bash
mkdir .streamlit
```

**Create secrets file:**
Create `.streamlit/secrets.toml`:

```toml
[snowflake]
account = "your_account.region"
user = "your_username"
password = "your_password"
warehouse = "COMPUTE_WH"
database = "DEV_DB"
schema = "GOLD"
```

**IMPORTANT:** 
- Replace with your actual Snowflake credentials
- Add `.streamlit/` to `.gitignore`
- NEVER commit secrets.toml to version control

**Create .gitignore:**
```
.streamlit/secrets.toml
venv/
__pycache__/
*.pyc
```

**Checkpoint:** Secrets file created (not committed to git).

---

### Task 3: Create Connection Module (25 mins)

Create `snowflake_app.py`:

```python
import streamlit as st
import pandas as pd
from snowflake.connector import connect

# Page configuration
st.set_page_config(
    page_title="Snowflake Data Explorer",
    page_icon=":snowflake:",
    layout="wide"
)

st.title("Snowflake GOLD Zone Explorer")
st.write("Connected to the same data as Power BI!")

# ============================================================================
# SNOWFLAKE CONNECTION
# ============================================================================

@st.cache_resource
def get_connection():
    """Create a cached Snowflake connection."""
    try:
        conn = connect(
            account=st.secrets["snowflake"]["account"],
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"]
        )
        return conn
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None

@st.cache_data(ttl=600)  # Cache for 10 minutes
def run_query(query: str) -> pd.DataFrame:
    """Execute query and return DataFrame with caching."""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

# Test connection
conn = get_connection()
if conn:
    st.success("Connected to Snowflake!")
else:
    st.error("Connection failed. Check your credentials.")
    st.stop()
```

**Run to test:**
```bash
streamlit run snowflake_app.py
```

**Checkpoint:** Successfully connected to Snowflake.

---

### Task 4: Query GOLD Zone Tables (30 mins)

Add queries to your app:

```python
# ============================================================================
# SIDEBAR - TABLE SELECTION
# ============================================================================

st.sidebar.header("Data Explorer")

# Get available tables
tables_query = """
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'GOLD'
ORDER BY table_name
"""
tables_df = run_query(tables_query)

if not tables_df.empty:
    selected_table = st.sidebar.selectbox(
        "Select Table",
        tables_df["TABLE_NAME"].tolist()
    )
else:
    st.error("No tables found in GOLD schema")
    st.stop()

# ============================================================================
# TABLE PREVIEW
# ============================================================================

st.subheader(f"Preview: {selected_table}")

# Row limit
row_limit = st.sidebar.slider("Row Limit", 10, 1000, 100)

# Query selected table
preview_query = f"SELECT * FROM {selected_table} LIMIT {row_limit}"
preview_df = run_query(preview_query)

if not preview_df.empty:
    # Show schema
    with st.expander("Table Schema"):
        schema_df = pd.DataFrame({
            "Column": preview_df.columns,
            "Type": preview_df.dtypes.astype(str)
        })
        st.dataframe(schema_df)
    
    # Show data
    st.dataframe(preview_df, use_container_width=True)
    
    # Row count
    count_query = f"SELECT COUNT(*) as row_count FROM {selected_table}"
    count_df = run_query(count_query)
    total_rows = count_df["ROW_COUNT"].iloc[0] if not count_df.empty else "Unknown"
    st.sidebar.metric("Total Rows", f"{total_rows:,}" if isinstance(total_rows, int) else total_rows)
```

**Checkpoint:** Can browse GOLD zone tables.

---

### Task 5: Add Analytics Query (30 mins)

Add a custom analytics section:

```python
# ============================================================================
# ANALYTICS SECTION
# ============================================================================

st.divider()
st.subheader("Sales Analytics")

# Filters
col1, col2 = st.columns(2)

with col1:
    # Get years
    years_query = "SELECT DISTINCT year FROM DIM_DATE ORDER BY year"
    years_df = run_query(years_query)
    if not years_df.empty:
        selected_year = st.selectbox("Year", years_df["YEAR"].tolist())
    else:
        selected_year = None

with col2:
    # Get segments
    segments_query = "SELECT DISTINCT market_segment FROM DIM_CUSTOMER ORDER BY market_segment"
    segments_df = run_query(segments_query)
    if not segments_df.empty:
        selected_segments = st.multiselect(
            "Market Segments",
            segments_df["MARKET_SEGMENT"].tolist(),
            default=segments_df["MARKET_SEGMENT"].tolist()
        )
    else:
        selected_segments = []

# Build and run analytics query
if selected_year and selected_segments:
    segments_str = "'" + "','".join(selected_segments) + "'"
    
    analytics_query = f"""
    SELECT 
        d.year,
        d.quarter,
        c.market_segment,
        SUM(f.net_amount) as total_revenue,
        COUNT(DISTINCT f.order_key) as order_count,
        COUNT(DISTINCT f.customer_key) as customer_count
    FROM FCT_ORDER_LINES f
    JOIN DIM_DATE d ON f.date_key = d.date_key
    JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
    WHERE d.year = {selected_year}
      AND c.market_segment IN ({segments_str})
    GROUP BY d.year, d.quarter, c.market_segment
    ORDER BY d.quarter, c.market_segment
    """
    
    analytics_df = run_query(analytics_query)
    
    if not analytics_df.empty:
        # KPI Row
        kpi1, kpi2, kpi3 = st.columns(3)
        with kpi1:
            st.metric("Total Revenue", f"${analytics_df['TOTAL_REVENUE'].sum():,.2f}")
        with kpi2:
            st.metric("Total Orders", f"{analytics_df['ORDER_COUNT'].sum():,}")
        with kpi3:
            st.metric("Unique Customers", f"{analytics_df['CUSTOMER_COUNT'].sum():,}")
        
        # Charts
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("**Revenue by Quarter**")
            quarterly = analytics_df.groupby("QUARTER")["TOTAL_REVENUE"].sum()
            st.bar_chart(quarterly)
        
        with chart_col2:
            st.markdown("**Revenue by Segment**")
            by_segment = analytics_df.groupby("MARKET_SEGMENT")["TOTAL_REVENUE"].sum()
            st.bar_chart(by_segment)
        
        # Detailed data
        with st.expander("View Detailed Data"):
            st.dataframe(analytics_df)
            
            csv = analytics_df.to_csv(index=False)
            st.download_button("Download CSV", csv, "analytics_data.csv", "text/csv")
```

**Checkpoint:** Analytics query with filters working.

---

### Task 6: Add Caching Explanation (15 mins)

Add educational content about caching:

```python
# ============================================================================
# CACHING DEMONSTRATION
# ============================================================================

st.divider()
st.subheader("Understanding Caching")

with st.expander("How Caching Works"):
    st.markdown("""
    **@st.cache_resource**
    - Used for: Database connections, ML models
    - Persists across sessions
    - Example: Snowflake connection
    
    **@st.cache_data**
    - Used for: Query results, data transformations
    - Can expire with TTL (time to live)
    - Example: Query results cached for 10 minutes
    
    **Benefits:**
    - Faster response times
    - Reduced database load
    - Better user experience
    
    **Clear cache:**
    """)
    
    if st.button("Clear All Caches"):
        st.cache_data.clear()
        st.success("Cache cleared! Refresh to reload data.")
```

**Checkpoint:** Caching explanation added.

---

### Task 7: Test Complete App (15 mins)

**Full test:**
```bash
streamlit run snowflake_app.py
```

**Test scenarios:**
1. Connect to Snowflake
2. Browse different tables
3. Apply filters
4. View analytics
5. Download data
6. Clear cache and reload

**Checkpoint:** All functionality working.

---

## Deliverables

Submit the following:

1. **snowflake_app.py:** Complete application code
2. **requirements.txt:** Updated dependencies
3. **Screenshot 1:** App connected to Snowflake
4. **Screenshot 2:** Analytics with filters applied
5. **.gitignore:** Showing secrets protection

**DO NOT SUBMIT secrets.toml**

---

## Definition of Done

- [ ] Snowflake connector installed
- [ ] Secrets configured securely
- [ ] Connection cached with @st.cache_resource
- [ ] Queries cached with @st.cache_data
- [ ] Can browse GOLD zone tables
- [ ] Analytics query with filters working
- [ ] Export functionality working
- [ ] secrets.toml in .gitignore

---

## Stretch Goals (Optional)

1. Add SQL query input for custom queries
2. Implement query history
3. Add data visualization with Plotly
4. Create comparison between years/quarters

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Verify account format, check firewall |
| Authentication failed | Check username/password in secrets |
| "secrets" not found | Ensure .streamlit/secrets.toml exists |
| Query timeout | Add timeout parameter to connect() |
| Cache not updating | Clear cache with st.cache_data.clear() |
