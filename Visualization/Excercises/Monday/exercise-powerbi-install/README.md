# Exercise: Power BI Desktop Installation and Interface Exploration

## Overview
**Day:** 1-Monday
**Duration:** 1-2 hours
**Mode:** Individual (Exploration + Documentation)
**Prerequisites:** Windows 10/11 or Windows Server with internet access

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Power BI Introduction | [power-bi-introduction.md](../../content/1-Monday/power-bi-introduction.md) | Editions, ecosystem overview |
| Connecting to Data Sources | [connecting-to-data-sources.md](../../content/1-Monday/connecting-to-data-sources.md) | Connection types, authentication |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Install Power BI Desktop on your machine
2. Navigate the three main views (Report, Data, Model)
3. Identify key interface components and their purposes
4. Connect to a sample dataset

---

## The Scenario
You are starting a new role as a data visualization specialist. Your first task is to set up your Power BI development environment and familiarize yourself with the tool before connecting to production data sources.

---

## Core Tasks

### Task 1: Install Power BI Desktop (20 mins)

**Option A: Microsoft Store (Recommended)**
1. Open Microsoft Store
2. Search for "Power BI Desktop"
3. Click **Get** to install
4. Wait for installation to complete

**Option B: Direct Download**
1. Visit https://powerbi.microsoft.com/desktop
2. Download the installer (.exe)
3. Run the installer and follow prompts
4. Accept default settings

**Checkpoint:** Take a screenshot of Power BI Desktop opening successfully.

---

### Task 2: Interface Tour (30 mins)

Explore each of the three main views:

**Report View (default):**
1. Identify the canvas area (center)
2. Locate the Visualizations pane (right side)
3. Find the Fields pane (right side, lists tables/columns)
4. Locate the Filters pane (right side)
5. Find the Pages tabs (bottom)

**Data View:**
1. Click the table icon in the left sidebar
2. Note: This view shows data after import
3. Currently empty since no data loaded

**Model View:**
1. Click the diagram icon in the left sidebar
2. Note: This shows relationships between tables
3. Currently empty since no data loaded

**Checkpoint:** Draw or describe the layout of Report View with labeled components.

---

### Task 3: Connect to Sample Data (30 mins)

1. Click **Home** > **Get Data** > **More...**
2. Browse through available connector categories:
   - File (Excel, CSV, JSON)
   - Database (SQL Server, PostgreSQL)
   - Azure services
   - Online services
   
3. Select **Excel Workbook** or **Sample datasets** if available
4. If no sample file is available:
   - Click **Try a sample dataset** from the welcome screen
   - OR download sample data from the starter_code folder

5. Load the sample data
6. Explore the data in Data View
7. Check for auto-detected relationships in Model View

**Checkpoint:** Screenshot showing loaded tables in the Fields pane.

---

### Task 4: Document Your Observations (30 mins)

Create a documentation file (markdown or text) answering:

1. **Interface Components:** List 5 key areas of the Report View and their purposes

2. **Data Connectors:** Name 3 data sources you might connect to at your organization

3. **Three Views Comparison:**

| View | Purpose | When to Use |
|------|---------|-------------|
| Report | | |
| Data | | |
| Model | | |

4. **Settings to Note:** Where do you find:
   - Options menu?
   - Theme settings?
   - Publish button?

---

## Deliverables

Submit the following:

1. **Screenshot 1:** Power BI Desktop installed and launched
2. **Screenshot 2:** Report View with sample data loaded
3. **Documentation:** Interface observation notes
4. **Questions:** List 3 questions you have about Power BI

---

## Definition of Done

- [ ] Power BI Desktop installed successfully
- [ ] All three views explored
- [ ] Sample data loaded
- [ ] Documentation created with interface observations
- [ ] At least 3 questions formulated for instructor

---

## Stretch Goals (Optional)

1. Explore the **Format** options for a visual
2. Try the **Analyze** > **Quick insights** feature
3. Explore keyboard shortcuts (View > Keyboard shortcuts)

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Installation fails | Run as Administrator, check disk space |
| Black screen on launch | Update graphics drivers |
| Can't find sample data | Download from starter_code folder |
| Ribbon items grayed out | Load data first, some options require data |
