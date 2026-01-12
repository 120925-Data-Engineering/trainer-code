# Exercise: Streamlit Environment Setup and First App

## Overview
**Day:** 5-Friday
**Duration:** 1.5-2 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Python installed, basic Python knowledge

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Streamlit Introduction | [streamlit-introduction.md](../../content/5-Friday/streamlit-introduction.md) | What Streamlit is, architecture |
| Streamlit Setup | [streamlit-setup.md](../../content/5-Friday/streamlit-setup.md) | Installation, virtual environments |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Create and activate a Python virtual environment
2. Install Streamlit and verify the installation
3. Create and run your first Streamlit app
4. Understand the Streamlit development workflow

---

## The Scenario
Your team wants to prototype a quick data dashboard. Before building anything complex, you need to set up your Streamlit development environment and verify everything works correctly.

---

## Core Tasks

### Task 1: Create Virtual Environment (20 mins)

**Windows:**
```powershell
# Navigate to your project directory
cd C:\Users\YourName\Projects

# Create project folder
mkdir streamlit_training
cd streamlit_training

# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate.ps1

# Verify activation (prompt should show (venv))
```

**macOS/Linux:**
```bash
# Navigate to your project directory
cd ~/Projects

# Create project folder
mkdir streamlit_training
cd streamlit_training

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Verify activation (prompt should show (venv))
```

**Checkpoint:** Virtual environment activated (visible in prompt).

---

### Task 2: Install Streamlit (15 mins)

**Install packages:**
```bash
# Upgrade pip first
pip install --upgrade pip

# Install Streamlit
pip install streamlit

# Install additional dependencies
pip install pandas numpy

# Verify installation
streamlit --version
```

**Expected output:** Streamlit version number (e.g., `Streamlit, version 1.x.x`)

**Create requirements.txt:**
```bash
pip freeze > requirements.txt
```

**Checkpoint:** Streamlit installed and version confirmed.

---

### Task 3: Create Your First App (30 mins)

**Create app file:**
Create a file named `app.py` with the following content:

```python
import streamlit as st
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="My First Streamlit App",
    page_icon=":wave:",
    layout="centered"
)

# Title and introduction
st.title("Hello, Streamlit!")
st.write("This is my first Streamlit application.")

# Divider
st.divider()

# Interactive input
st.subheader("Interactive Greeting")
name = st.text_input("What is your name?")
if name:
    st.write(f"Welcome to Streamlit, {name}!")
    st.balloons()

# Divider
st.divider()

# Display some data
st.subheader("Sample Data Display")

# Create sample data
data = {
    "Product": ["Widget A", "Widget B", "Widget C", "Widget D"],
    "Sales": [1200, 1850, 950, 1400],
    "Region": ["North", "South", "East", "West"]
}
df = pd.DataFrame(data)

# Display as table
st.dataframe(df)

# Display metrics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Sales", f"${df['Sales'].sum():,}")
with col2:
    st.metric("Products", len(df))
with col3:
    st.metric("Avg Sale", f"${df['Sales'].mean():,.0f}")

# Simple chart
st.subheader("Sales by Product")
st.bar_chart(df.set_index("Product")["Sales"])

# Footer
st.divider()
st.caption("Week 6 Visualization - Streamlit Exercise 1")
```

**Run the app:**
```bash
streamlit run app.py
```

**Browser should open automatically to:** http://localhost:8501

**Checkpoint:** App running in browser.

---

### Task 4: Explore Streamlit Features (30 mins)

**Modify your app to include:**

1. **Sidebar:**
```python
st.sidebar.title("Settings")
show_data = st.sidebar.checkbox("Show raw data", value=True)
if show_data:
    st.dataframe(df)
```

2. **User input controls:**
```python
selected_region = st.selectbox("Select Region", df["Region"].unique())
filtered_df = df[df["Region"] == selected_region]
st.write(f"Showing data for: {selected_region}")
st.dataframe(filtered_df)
```

3. **Slider:**
```python
threshold = st.slider("Minimum Sales", 0, 2000, 1000)
above_threshold = df[df["Sales"] >= threshold]
st.write(f"Products above ${threshold}: {len(above_threshold)}")
```

4. **Different chart types:**
```python
chart_type = st.radio("Chart Type", ["Bar", "Line"])
if chart_type == "Bar":
    st.bar_chart(df.set_index("Product")["Sales"])
else:
    st.line_chart(df.set_index("Product")["Sales"])
```

**Observe hot reload:**
- Save changes to app.py
- Streamlit automatically detects changes
- Click "Rerun" or enable "Always rerun"

**Checkpoint:** Enhanced app with sidebar and interactivity.

---

### Task 5: Understand the Workflow (15 mins)

**Document your observations:**

1. **How does Streamlit execute code?**
   - Script runs top to bottom
   - Re-runs on every interaction
   - State is managed through session_state (advanced)

2. **Development experience:**
   - Hot reload on file save
   - Error messages in browser
   - Terminal shows logs

3. **Compare to Power BI:**

| Aspect | Power BI | Streamlit |
|--------|----------|-----------|
| Language | DAX/M | Python |
| Deployment | Power BI Service | Web server |
| Learning curve | | |
| Customization | | |
| Data size | | |

**Checkpoint:** Workflow documented and comparison completed.

---

## Deliverables

Submit the following:

1. **app.py:** Your completed Streamlit app file
2. **requirements.txt:** Dependencies file
3. **Screenshot 1:** App running in browser
4. **Screenshot 2:** App with sidebar visible
5. **Documentation:** Answers to workflow questions

---

## Definition of Done

- [ ] Virtual environment created and activated
- [ ] Streamlit installed and verified
- [ ] Basic app created and running
- [ ] Sidebar and interactive controls added
- [ ] Hot reload observed and understood
- [ ] Workflow documented

---

## Stretch Goals (Optional)

1. Add file upload functionality
2. Create multiple pages using st.pages
3. Add session state to persist selections
4. Explore st.cache_data for performance

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| streamlit command not found | Activate virtual environment |
| Port 8501 in use | Use `streamlit run app.py --server.port 8502` |
| Browser doesn't open | Manually navigate to http://localhost:8501 |
| Changes not appearing | Click Rerun or enable Always rerun |
| Import error | Verify package installed in active venv |
