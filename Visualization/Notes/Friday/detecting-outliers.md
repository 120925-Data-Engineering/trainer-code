# Detecting Outliers

## Learning Objectives
- Understand statistical outlier detection methods
- Implement IQR and Z-score detection in Python
- Visualize outliers in Streamlit applications
- Build an interactive outlier detection tool

## Why This Matters

Outliers can indicate data quality issues, fraud, or genuine anomalies worth investigating. Building outlier detection into your Streamlit dashboards helps users identify unusual patterns without manual analysis.

## What Are Outliers?

Outliers are data points that differ significantly from other observations.

### Types of Outliers

| Type | Description | Example |
|------|-------------|---------|
| **Point outliers** | Single unusual value | Sale of $1M when average is $100 |
| **Contextual** | Unusual in context | High sales on a holiday |
| **Collective** | Group of unusual points | Repeated failed transactions |

## IQR Method

The Interquartile Range method is robust to non-normal distributions.

### Concept

1. Calculate Q1 (25th percentile) and Q3 (75th percentile)
2. IQR = Q3 - Q1
3. Lower bound = Q1 - 1.5 * IQR
4. Upper bound = Q3 + 1.5 * IQR
5. Values outside bounds are outliers

### Python Implementation

```python
import pandas as pd
import numpy as np

def detect_outliers_iqr(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    return outliers, lower_bound, upper_bound
```

## Z-Score Method

Assumes data follows a normal distribution.

### Concept

1. Calculate mean and standard deviation
2. Z-score = (value - mean) / std_dev
3. Values with |Z-score| > threshold (typically 3) are outliers

### Python Implementation

```python
from scipy import stats

def detect_outliers_zscore(df, column, threshold=3):
    z_scores = np.abs(stats.zscore(df[column].dropna()))
    outlier_mask = z_scores > threshold
    
    # Map back to original dataframe
    valid_indices = df[column].dropna().index
    outliers = df.loc[valid_indices[outlier_mask]]
    return outliers
```

## Streamlit Outlier Detection App

```python
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from scipy import stats

st.title("Outlier Detection Tool")

# File upload or sample data
use_sample = st.checkbox("Use sample data", value=True)

if use_sample:
    np.random.seed(42)
    df = pd.DataFrame({
        "Values": np.concatenate([
            np.random.normal(100, 15, 95),  # Normal data
            np.array([5, 10, 200, 250, 300])  # Outliers
        ])
    })
else:
    file = st.file_uploader("Upload CSV", type="csv")
    if file:
        df = pd.read_csv(file)
    else:
        st.stop()

# Column selection
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
column = st.selectbox("Select column", numeric_cols)

# Method selection
method = st.radio("Detection method", ["IQR", "Z-Score"])

# Parameters
if method == "IQR":
    multiplier = st.slider("IQR multiplier", 1.0, 3.0, 1.5)
else:
    threshold = st.slider("Z-Score threshold", 1.0, 5.0, 3.0)

# Detection
if method == "IQR":
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - multiplier * IQR
    upper = Q3 + multiplier * IQR
    df["Is_Outlier"] = (df[column] < lower) | (df[column] > upper)
else:
    z_scores = np.abs(stats.zscore(df[column].dropna()))
    df["Is_Outlier"] = False
    df.loc[df[column].dropna().index, "Is_Outlier"] = z_scores > threshold

# Results
outliers = df[df["Is_Outlier"]]
st.metric("Outliers Found", len(outliers))

# Visualization
fig = px.box(df, y=column, title=f"Box Plot: {column}")
st.plotly_chart(fig)

# Scatter with outliers highlighted
df["Index"] = range(len(df))
fig2 = px.scatter(
    df, x="Index", y=column,
    color="Is_Outlier",
    color_discrete_map={True: "red", False: "blue"},
    title="Data Points (Outliers in Red)"
)
st.plotly_chart(fig2)

# Show outlier table
with st.expander("View Outliers"):
    st.dataframe(outliers)
```

## Method Comparison

| Aspect | IQR | Z-Score |
|--------|-----|---------|
| **Distribution** | Works for any distribution | Assumes normal |
| **Sensitivity** | Less sensitive to extremes | More sensitive |
| **Parameter** | IQR multiplier (1.5 standard) | Z threshold (3 standard) |
| **Best for** | Skewed data | Normal data |

## Visualizing Outliers

### Box Plot

Shows quartiles and outliers naturally:

```python
fig = px.box(df, y="Amount", title="Sales Distribution")
st.plotly_chart(fig)
```

### Scatter with Highlighting

```python
fig = px.scatter(df, x="Date", y="Amount", color="Is_Outlier")
st.plotly_chart(fig)
```

### Histogram with Bounds

```python
fig = px.histogram(df, x="Amount", nbins=30)
fig.add_vline(x=lower_bound, line_dash="dash", line_color="red")
fig.add_vline(x=upper_bound, line_dash="dash", line_color="red")
st.plotly_chart(fig)
```

## Summary

- IQR method works for any distribution; uses quartile-based bounds
- Z-Score method assumes normality; uses standard deviation
- Visualize outliers with box plots or highlighted scatter plots
- Build interactive tools that let users adjust detection parameters

## Additional Resources

- [Outlier Detection](https://scikit-learn.org/stable/modules/outlier_detection.html) - Scikit-learn methods
- [Statistical Methods](https://www.itl.nist.gov/div898/handbook/eda/section3/eda35h.htm) - NIST handbook
