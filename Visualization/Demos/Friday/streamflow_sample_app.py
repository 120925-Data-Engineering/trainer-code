"""
StreamFlow Phase 2 - Sample Analytics Dashboard
================================================
A Streamlit demo showcasing the enterprise analytics pipeline
for e-commerce data visualization.

This sample app demonstrates loading data from CSV files (simulating Gold Zone).
See the SNOWFLAKE_NOTES section below for how to connect to Snowflake tables.

Run: streamlit run streamflow_sample_app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path
import os

# =============================================================================
# SNOWFLAKE CONNECTION NOTES
# =============================================================================
#
# HOW TO CONNECT TO SNOWFLAKE INSTEAD OF CSV FILES
# ============================================================================
#
# 1. Install dependencies:
#    pip install snowflake-connector-python python-dotenv
#
# 2. Create a .env file in this directory with your credentials:
#    SNOWFLAKE_USER=your_username
#    SNOWFLAKE_PASSWORD=your_password
#    SNOWFLAKE_ACCOUNT=your_account.region
#    SNOWFLAKE_WAREHOUSE=STREAMFLOW_WH
#    SNOWFLAKE_DATABASE=STREAMFLOW_DW
#
# 3. Add these imports at the top of the file:
#    import snowflake.connector
#    from dotenv import load_dotenv
#    load_dotenv()  # Load .env file
#
# 4. Create a connection function:
#
# def get_snowflake_connection():
#     '''Create and return a Snowflake connection using .env credentials.'''
#     load_dotenv()  # Ensure .env is loaded
#     return snowflake.connector.connect(
#         user=os.environ.get('SNOWFLAKE_USER'),
#         password=os.environ.get('SNOWFLAKE_PASSWORD'),
#         account=os.environ.get('SNOWFLAKE_ACCOUNT'),
#         warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'STREAMFLOW_WH'),
#         database=os.environ.get('SNOWFLAKE_DATABASE', 'STREAMFLOW_DW'),
#         schema='GOLD'
#     )
#
# 5. Replace the CSV loading function with Snowflake queries:
#
# @st.cache_data(ttl=300)  # Cache for 5 minutes
# def load_data_from_snowflake():
#     '''Load all data from Snowflake GOLD schema tables.'''
#     conn = get_snowflake_connection()
#     
#     try:
#         # Daily metrics from agg_daily_revenue
#         df_daily = pd.read_sql('''
#             SELECT 
#                 date,
#                 total_revenue as revenue,
#                 total_transactions as transactions,
#                 unique_users,
#                 page_views,
#                 cart_adds,
#                 conversion_rate
#             FROM GOLD.agg_daily_revenue
#             ORDER BY date DESC
#             LIMIT 90
#         ''', conn)
#         
#         # Category revenue from fact_transactions + dim_product
#         df_category = pd.read_sql('''
#             SELECT 
#                 p.category,
#                 SUM(f.total_amount) as revenue,
#                 COUNT(DISTINCT f.transaction_id) as orders,
#                 AVG(f.total_amount) as avg_order_value,
#                 COUNT(DISTINCT f.product_id) as product_count
#             FROM GOLD.fact_transactions f
#             JOIN GOLD.dim_product p ON f.product_key = p.product_key
#             GROUP BY p.category
#             ORDER BY revenue DESC
#         ''', conn)
#         
#         # Event distribution from fact_user_activity
#         df_events = pd.read_sql('''
#             SELECT 
#                 event_type,
#                 COUNT(*) as count,
#                 ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
#             FROM GOLD.fact_user_activity
#             GROUP BY event_type
#             ORDER BY count DESC
#         ''', conn)
#         
#         # Device breakdown
#         df_device = pd.read_sql('''
#             SELECT 
#                 device_type as device,
#                 COUNT(DISTINCT user_id) as users,
#                 COUNT(*) as sessions,
#                 SUM(revenue) as revenue
#             FROM GOLD.fact_user_activity
#             GROUP BY device_type
#         ''', conn)
#         
#         # Top products
#         df_products = pd.read_sql('''
#             SELECT 
#                 ROW_NUMBER() OVER (ORDER BY SUM(quantity * unit_price) DESC) as rank,
#                 p.product_name,
#                 p.category,
#                 SUM(quantity) as units_sold,
#                 SUM(quantity * unit_price) as revenue,
#                 AVG(rating) as avg_rating
#             FROM GOLD.fact_transactions f
#             JOIN GOLD.dim_product p ON f.product_key = p.product_key
#             GROUP BY p.product_name, p.category
#             ORDER BY revenue DESC
#             LIMIT 10
#         ''', conn)
#         
#         # Funnel data
#         df_funnel = pd.read_sql('''
#             SELECT stage, users, conversion
#             FROM GOLD.vw_conversion_funnel
#         ''', conn)
#         
#         # Payment methods
#         df_payment = pd.read_sql('''
#             SELECT 
#                 payment_method as method,
#                 COUNT(*) as transactions,
#                 SUM(total_amount) as revenue
#             FROM GOLD.fact_transactions
#             GROUP BY payment_method
#             ORDER BY revenue DESC
#         ''', conn)
#         
#         # Customer segments
#         df_segments = pd.read_sql('''
#             SELECT 
#                 customer_segment as segment,
#                 COUNT(*) as count,
#                 SUM(lifetime_value) as revenue,
#                 AVG(avg_order_value) as avg_value
#             FROM GOLD.dim_customer
#             GROUP BY customer_segment
#         ''', conn)
#         
#         return {
#             'daily': df_daily,
#             'category': df_category,
#             'events': df_events,
#             'device': df_device,
#             'products': df_products,
#             'funnel': df_funnel,
#             'payment': df_payment,
#             'segments': df_segments
#         }
#     finally:
#         conn.close()
#
# 6. IMPORTANT: Add .env to your .gitignore to avoid committing credentials!
#
# =============================================================================

# =============================================================================
# Configuration & Styling
# =============================================================================

st.set_page_config(
    page_title="StreamFlow Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for premium styling
st.markdown("""
<style>
    /* Main background gradient */
    .stApp {
        background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 50%, #0d0d1a 100%);
    }
    
    /* Metric cards styling */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(99, 102, 241, 0.15) 0%, rgba(139, 92, 246, 0.1) 100%);
        border: 1px solid rgba(139, 92, 246, 0.3);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 8px 32px rgba(139, 92, 246, 0.15);
    }
    
    [data-testid="stMetric"] label {
        color: #a5b4fc !important;
        font-weight: 500;
    }
    
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-weight: 700;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e1e3f 0%, #0f0f23 100%);
        border-right: 1px solid rgba(139, 92, 246, 0.2);
    }
    
    /* Headers */
    h1, h2, h3 {
        background: linear-gradient(90deg, #a5b4fc 0%, #c4b5fd 50%, #f9a8d4 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 700;
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: rgba(30, 30, 63, 0.5);
        border-radius: 12px;
        padding: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 8px;
        color: #a5b4fc;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
        color: white;
    }
    
    /* DataFrame styling */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }
    
    /* Info box */
    .info-box {
        background: linear-gradient(135deg, rgba(59, 130, 246, 0.15) 0%, rgba(99, 102, 241, 0.1) 100%);
        border: 1px solid rgba(99, 102, 241, 0.3);
        border-radius: 12px;
        padding: 16px;
        margin: 16px 0;
    }
    
    /* Data source badge */
    .data-source {
        background: rgba(34, 197, 94, 0.2);
        border: 1px solid rgba(34, 197, 94, 0.4);
        border-radius: 8px;
        padding: 8px 12px;
        font-size: 0.85em;
        color: #4ade80;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# Data Loading (CSV Files - Simulating Snowflake Gold Layer)
# =============================================================================

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"


@st.cache_data
def load_data_from_csv():
    """
    Load analytics data from CSV files.
    
    These CSV files simulate the Snowflake GOLD schema tables:
    - daily_metrics.csv    → agg_daily_revenue
    - category_revenue.csv → dim_product + fact_transactions aggregation
    - events.csv           → fact_user_activity aggregation
    - devices.csv          → fact_user_activity by device
    - top_products.csv     → dim_product + fact_transactions top N
    - funnel.csv           → vw_conversion_funnel
    - payment_methods.csv  → fact_transactions by payment
    - customer_segments.csv → dim_customer aggregation
    """
    
    # Load all CSV files
    df_daily = pd.read_csv(DATA_DIR / "daily_metrics.csv", parse_dates=['date'])
    df_category = pd.read_csv(DATA_DIR / "category_revenue.csv")
    df_events = pd.read_csv(DATA_DIR / "events.csv")
    df_device = pd.read_csv(DATA_DIR / "devices.csv")
    df_products = pd.read_csv(DATA_DIR / "top_products.csv")
    df_funnel = pd.read_csv(DATA_DIR / "funnel.csv")
    df_payment = pd.read_csv(DATA_DIR / "payment_methods.csv")
    df_segments = pd.read_csv(DATA_DIR / "customer_segments.csv")
    
    return {
        'daily': df_daily,
        'category': df_category,
        'events': df_events,
        'device': df_device,
        'products': df_products,
        'funnel': df_funnel,
        'payment': df_payment,
        'segments': df_segments
    }


# =============================================================================
# Chart Theme
# =============================================================================

def get_chart_theme():
    """Return consistent dark theme for all charts."""
    return {
        'paper_bgcolor': 'rgba(0,0,0,0)',
        'plot_bgcolor': 'rgba(0,0,0,0)',
        'font': {'color': '#a5b4fc', 'family': 'Inter, sans-serif'},
        'colorway': ['#8b5cf6', '#6366f1', '#a78bfa', '#c4b5fd', '#818cf8', 
                     '#f472b6', '#38bdf8', '#34d399', '#fbbf24', '#fb7185']
    }


# =============================================================================
# Dashboard Pages
# =============================================================================

def executive_summary(data):
    """Executive Summary Dashboard - KPIs and trends."""
    st.header("📈 Executive Summary")
    
    daily = data['daily']
    
    # Calculate KPIs
    total_revenue = daily['revenue'].sum()
    total_transactions = daily['transactions'].sum()
    avg_order_value = total_revenue / total_transactions
    total_users = daily['unique_users'].sum()
    avg_conversion = daily['conversion_rate'].mean()
    
    # Last 7 days vs previous 7 days comparison
    last_7 = daily.tail(7)['revenue'].sum()
    prev_7 = daily.tail(14).head(7)['revenue'].sum()
    wow_change = ((last_7 - prev_7) / prev_7) * 100
    
    # KPI Cards
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Revenue", f"${total_revenue:,.0f}", f"{wow_change:+.1f}% WoW")
    with col2:
        st.metric("Transactions", f"{total_transactions:,}", "+5.2%")
    with col3:
        st.metric("Avg Order Value", f"${avg_order_value:.2f}", "+2.3%")
    with col4:
        st.metric("Unique Users", f"{total_users:,}", "+8.1%")
    with col5:
        st.metric("Conversion Rate", f"{avg_conversion:.1f}%", "+0.3%")
    
    st.markdown("---")
    
    # Revenue Trend Chart
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📊 Revenue Trend")
        fig = px.area(daily, x='date', y='revenue',
                      labels={'date': 'Date', 'revenue': 'Revenue ($)'},
                      template='plotly_dark')
        fig.update_traces(fill='tozeroy', 
                          fillcolor='rgba(139, 92, 246, 0.3)',
                          line_color='#8b5cf6')
        fig.update_layout(**get_chart_theme(), height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💳 Revenue by Category")
        category = data['category'].head(5)
        fig = px.pie(category, values='revenue', names='category',
                     hole=0.6, template='plotly_dark')
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(**get_chart_theme(), height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # Secondary metrics row
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📱 Device Performance")
        device = data['device']
        fig = px.bar(device, x='device', y='revenue',
                     color='device', template='plotly_dark',
                     labels={'device': 'Device', 'revenue': 'Revenue ($)'})
        fig.update_layout(**get_chart_theme(), height=300, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💰 Payment Methods")
        payment = data['payment']
        fig = px.bar(payment, x='method', y='revenue',
                     color='method', template='plotly_dark',
                     labels={'method': 'Method', 'revenue': 'Revenue ($)'})
        fig.update_layout(**get_chart_theme(), height=300, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


def user_engagement(data):
    """User Engagement Dashboard - Behavior analytics."""
    st.header("👥 User Engagement")
    
    daily = data['daily']
    events = data['events']
    device = data['device']
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Events", f"{events['count'].sum():,}", "+12.4%")
    with col2:
        st.metric("Daily Active Users", f"{daily['unique_users'].mean():,.0f}", "+6.8%")
    with col3:
        st.metric("Avg Session Duration", "4m 32s", "+15s")
    with col4:
        st.metric("Pages per Session", "4.7", "+0.3")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 Event Distribution")
        fig = px.bar(events, x='event_type', y='count',
                     color='event_type', template='plotly_dark',
                     labels={'event_type': 'Event Type', 'count': 'Count'})
        fig.update_layout(**get_chart_theme(), height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📲 Device Breakdown")
        fig = px.pie(device, values='users', names='device',
                     hole=0.5, template='plotly_dark')
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(**get_chart_theme(), height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # Activity trend
    st.subheader("📈 User Activity Trend")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily['date'], y=daily['unique_users'],
                             name='Unique Users', fill='tozeroy',
                             fillcolor='rgba(99, 102, 241, 0.3)',
                             line_color='#6366f1'))
    fig.add_trace(go.Scatter(x=daily['date'], y=daily['page_views'] / 10,
                             name='Page Views (÷10)', fill='tozeroy',
                             fillcolor='rgba(236, 72, 153, 0.2)',
                             line_color='#ec4899'))
    fig.update_layout(**get_chart_theme(), height=300,
                      legend=dict(orientation='h', yanchor='bottom', y=1.02))
    st.plotly_chart(fig, use_container_width=True)


def sales_performance(data):
    """Sales Performance Dashboard - Revenue analysis."""
    st.header("💰 Sales Performance")
    
    daily = data['daily']
    category = data['category']
    payment = data['payment']
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    total_rev = daily['revenue'].sum()
    total_trans = daily['transactions'].sum()
    
    with col1:
        st.metric("Total Revenue", f"${total_rev:,.0f}", "+8.5%")
    with col2:
        st.metric("Total Transactions", f"{total_trans:,}", "+6.2%")
    with col3:
        st.metric("Avg Transaction Value", f"${total_rev/total_trans:.2f}", "+2.1%")
    with col4:
        st.metric("Revenue per User", f"${total_rev/daily['unique_users'].sum():.2f}", "+3.8%")
    
    st.markdown("---")
    
    # Category Revenue Bar Chart
    st.subheader("🏷️ Revenue by Category")
    fig = px.bar(category, x='category', y='revenue',
                 color='revenue', color_continuous_scale='Purples',
                 template='plotly_dark',
                 labels={'category': 'Category', 'revenue': 'Revenue ($)'})
    fig.update_layout(**get_chart_theme(), height=400, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📅 Daily Revenue Trend")
        fig = px.line(daily, x='date', y='revenue',
                      template='plotly_dark',
                      labels={'date': 'Date', 'revenue': 'Revenue ($)'})
        fig.update_traces(line_color='#8b5cf6', line_width=2)
        # Add 7-day moving average
        daily_copy = daily.copy()
        daily_copy['ma7'] = daily_copy['revenue'].rolling(7).mean()
        fig.add_scatter(x=daily_copy['date'], y=daily_copy['ma7'],
                        name='7-Day MA', line=dict(color='#f472b6', dash='dash'))
        fig.update_layout(**get_chart_theme(), height=350,
                          legend=dict(orientation='h', yanchor='bottom', y=1.02))
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💳 Payment Method Distribution")
        fig = px.pie(payment, values='transactions', names='method',
                     template='plotly_dark')
        fig.update_traces(textposition='outside', textinfo='percent+label')
        fig.update_layout(**get_chart_theme(), height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


def product_analytics(data):
    """Product Analytics Dashboard - Product performance."""
    st.header("📦 Product Analytics")
    
    products = data['products']
    category = data['category']
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Products", "2,000", "+150 new")
    with col2:
        st.metric("Avg Product Rating", "4.3 ⭐", "+0.1")
    with col3:
        st.metric("Units Sold", f"{products['units_sold'].sum():,}", "+15.2%")
    with col4:
        st.metric("Cart Abandonment", "68.2%", "-2.1%")
    
    st.markdown("---")
    
    # Top Products Table
    st.subheader("🏆 Top 10 Products by Revenue")
    products_display = products.copy()
    products_display['revenue'] = products_display['revenue'].apply(lambda x: f"${x:,.0f}")
    products_display['avg_rating'] = products_display['avg_rating'].apply(lambda x: f"{x} ⭐")
    st.dataframe(products_display[['rank', 'product_name', 'category', 'units_sold', 'revenue', 'avg_rating']],
                 use_container_width=True, hide_index=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🛒 Category Distribution")
        fig = px.treemap(category, path=['category'], values='orders',
                         color='revenue', color_continuous_scale='Purples',
                         template='plotly_dark')
        fig.update_layout(**get_chart_theme(), height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Category Performance")
        fig = px.scatter(category, x='orders', y='revenue', size='avg_order_value',
                         color='category', template='plotly_dark',
                         labels={'orders': 'Orders', 'revenue': 'Revenue ($)'})
        fig.update_layout(**get_chart_theme(), height=350,
                          legend=dict(orientation='h', yanchor='bottom', y=1.02, font=dict(size=10)))
        st.plotly_chart(fig, use_container_width=True)


def funnel_analysis(data):
    """Funnel Analysis Dashboard - Conversion funnel."""
    st.header("🔄 Funnel Analysis")
    
    funnel = data['funnel']
    segments = data['segments']
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Funnel Start (Views)", "25,000", "+8.3%")
    with col2:
        st.metric("Add to Cart Rate", "24.0%", "+1.2%")
    with col3:
        st.metric("Checkout Rate", "40.0%", "+2.5%")
    with col4:
        st.metric("Purchase Conversion", "4.8%", "+0.3%")
    
    st.markdown("---")
    
    # Funnel Visualization
    st.subheader("📈 Conversion Funnel")
    
    fig = go.Figure(go.Funnel(
        y=funnel['stage'],
        x=funnel['users'],
        textinfo="value+percent initial",
        textposition="inside",
        marker=dict(color=['#8b5cf6', '#7c3aed', '#6d28d9', '#5b21b6', '#4c1d95']),
        connector=dict(line=dict(color='#6366f1', width=1))
    ))
    fig.update_layout(**get_chart_theme(), height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("👥 Customer Segments")
        fig = px.bar(segments, x='segment', y='count',
                     color='segment', template='plotly_dark',
                     labels={'segment': 'Segment', 'count': 'Customers'})
        fig.update_layout(**get_chart_theme(), height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("💵 Revenue by Segment")
        fig = px.pie(segments, values='revenue', names='segment',
                     hole=0.5, template='plotly_dark')
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(**get_chart_theme(), height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # Segment detail table
    st.subheader("📊 Segment Performance Detail")
    segments_display = segments.copy()
    segments_display['revenue'] = segments_display['revenue'].apply(lambda x: f"${x:,.0f}")
    segments_display['avg_value'] = segments_display['avg_value'].apply(lambda x: f"${x:.0f}")
    st.dataframe(segments_display, use_container_width=True, hide_index=True)


# =============================================================================
# Main App
# =============================================================================

def main():
    """Main application entry point."""
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/analytics.png", width=80)
        st.title("StreamFlow")
        st.caption("Enterprise Analytics Platform")
        
        st.markdown("---")
        
        # Data source indicator
        st.markdown("""
        <div class="data-source">
            📁 <strong>Data Source:</strong> CSV Files<br>
            <small>See code comments for Snowflake setup</small>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        st.markdown("""
        <div class="info-box">
            <strong>🔗 Data Pipeline</strong><br>
            <small>Kafka → Spark → Snowflake → Streamlit</small>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Date filter
        st.subheader("📅 Filters")
        date_range = st.selectbox(
            "Time Period",
            ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Custom"]
        )
        
        if date_range == "Custom":
            st.date_input("Start Date")
            st.date_input("End Date")
        
        st.markdown("---")
        
        # Refresh simulation
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.caption("Last Updated: " + datetime.now().strftime("%Y-%m-%d %H:%M"))
        st.caption("StreamFlow Phase 2 © 2025")
    
    # Load data from CSV files
    data = load_data_from_csv()
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Executive Summary",
        "👥 User Engagement", 
        "💰 Sales Performance",
        "📦 Product Analytics",
        "🔄 Funnel Analysis"
    ])
    
    with tab1:
        executive_summary(data)
    
    with tab2:
        user_engagement(data)
    
    with tab3:
        sales_performance(data)
    
    with tab4:
        product_analytics(data)
    
    with tab5:
        funnel_analysis(data)


if __name__ == "__main__":
    main()
