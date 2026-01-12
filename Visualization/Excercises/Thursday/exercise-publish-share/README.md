# Exercise: Publishing and Sharing in Power BI Service

## Overview
**Day:** 4-Thursday
**Duration:** 1-2 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Power BI report created from earlier exercises

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Power BI Service | [power-bi-service.md](../../content/4-Thursday/power-bi-service.md) | Publishing, workspaces, sharing |
| Dataset Refresh | [dataset-refresh.md](../../content/4-Thursday/dataset-refresh.md) | Refresh configuration |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Publish a report from Power BI Desktop to Power BI Service
2. Create and manage workspaces
3. Share reports with colleagues
4. Configure scheduled refresh
5. Understand the difference between reports, datasets, and dashboards

---

## The Scenario
Your sales report is ready for production. You need to publish it to Power BI Service where stakeholders can access it, then configure sharing and refresh settings.

---

## Core Tasks

### Task 1: Publish Your Report (15 mins)

1. Open your completed Power BI report (.pbix)
2. Ensure all changes are saved
3. Click **Home** > **Publish**
4. Sign in with your Power BI account (if prompted)
5. Select destination:
   - Choose "My Workspace" for personal use
   - OR create a new workspace for team sharing
6. Wait for publish confirmation
7. Click the link to view in Power BI Service

**Verify in Service:**
- Report appears in workspace
- Dataset appears separately
- All visuals render correctly

**Checkpoint:** Report visible in Power BI Service.

---

### Task 2: Create a Team Workspace (20 mins)

**Create workspace:**
1. In Power BI Service, click **Workspaces** > **Create a workspace**
2. Name it: "[Your Name] Training Workspace"
3. Description: "Week 6 Visualization training exercises"
4. Click **Save**

**Move or republish content:**
1. Return to Power BI Desktop
2. **Publish** again, selecting your new workspace
3. OR in Service, copy content to new workspace

**Configure workspace settings:**
1. Click workspace **Settings** (gear icon)
2. Explore License mode options
3. Note OneDrive connection option

**Checkpoint:** Team workspace created with content.

---

### Task 3: Share the Report (20 mins)

**Method 1: Direct Share**
1. Open the report in Service
2. Click **Share** button
3. Enter colleague email (or your own secondary email)
4. Choose permissions:
   - Allow recipients to share? (Yes/No)
   - Allow recipients to build content? (Yes/No)
5. Add optional message
6. Click **Share**

**Method 2: Workspace Access**
1. Go to workspace settings
2. Click **Access**
3. Add users with roles:
   - **Admin:** Full control
   - **Member:** Edit content
   - **Contributor:** Add content only
   - **Viewer:** View only

**Document your sharing configuration:**

| Recipient | Method | Permissions |
|-----------|--------|-------------|
| | | |

**Checkpoint:** Report shared with at least one user.

---

### Task 4: Configure Scheduled Refresh (25 mins)

**Access dataset settings:**
1. In workspace, find the **Dataset** (not report)
2. Click **...** > **Settings**

**Configure credentials:**
1. Expand **Data source credentials**
2. Click **Edit credentials** for Snowflake
3. Enter:
   - Username
   - Password
   - Privacy level: Organizational
4. Click **Sign in**

**Set up schedule:**
1. Expand **Scheduled refresh**
2. Toggle **Keep your data up to date** = ON
3. Configure:
   - Refresh frequency: Daily
   - Time zone: Your time zone
   - Time: 6:00 AM (or preferred time)
4. Add notification email for failures
5. Click **Apply**

**Trigger manual refresh:**
1. Click **Refresh now** (optional)
2. Check **Refresh history** for status

**Checkpoint:** Scheduled refresh configured and tested.

---

### Task 5: Explore Service Features (20 mins)

**Explore lineage view:**
1. In workspace, click **Lineage view** (if available)
2. See relationships between datasets, reports, dashboards

**Check usage metrics:**
1. Open a report
2. **View usage metrics report** (if available)
3. Note: May require usage data to accumulate

**Try web editing:**
1. Open report in Service
2. Click **Edit**
3. Make a minor change (e.g., visual title)
4. Save

**Explore content certification:**
1. Find dataset settings
2. Look for **Endorsement** options
3. Note: Certification requires admin permissions

**Checkpoint:** Service features explored and documented.

---

### Task 6: Document Your Deployment (15 mins)

Create a deployment summary:

| Item | Value |
|------|-------|
| Report Name | |
| Workspace Name | |
| Workspace URL | |
| Dataset Name | |
| Refresh Schedule | |
| Share Recipients | |
| Last Refresh Status | |

**Access information for stakeholders:**
- Direct report link: `app.powerbi.com/reports/[id]`
- Workspace location instructions

---

## Deliverables

Submit the following:

1. **Screenshot 1:** Report in Power BI Service
2. **Screenshot 2:** Workspace contents view
3. **Screenshot 3:** Refresh schedule configuration
4. **Deployment Document:** Summary table completed

---

## Definition of Done

- [ ] Report published to Power BI Service
- [ ] Team workspace created
- [ ] Report shared with at least one user
- [ ] Data source credentials configured
- [ ] Scheduled refresh set up
- [ ] Manual refresh tested
- [ ] Service features explored
- [ ] Deployment documented

---

## Stretch Goals (Optional)

1. Create a Power BI App from the workspace
2. Embed a report in a webpage or Teams
3. Set up multiple refresh times per day
4. Configure incremental refresh (if data supports it)

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Publish fails | Check internet, Power BI account status |
| Credentials error | Re-enter password, check account active |
| Refresh fails | Check credentials, data source accessibility |
| Share not working | Verify recipient has Power BI license |
| Workspace not found | Refresh browser, check permissions |
