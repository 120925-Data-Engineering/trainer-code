# Exercise: Implementing Row-Level Security (RLS)

## Overview
**Day:** 4-Thursday
**Duration:** 1.5-2 hours
**Mode:** Individual (Implementation)
**Prerequisites:** Report published to Power BI Service

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| Power BI Service | [power-bi-service.md](../../content/4-Thursday/power-bi-service.md) | Row-level security section |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Create RLS roles with DAX filters
2. Test roles using "View as" feature
3. Assign users to roles in Power BI Service
4. Understand dynamic RLS patterns

---

## The Scenario
Your organization has regional sales managers who should only see data for their assigned market segments. The CFO should see everything. You will implement row-level security to enforce these access restrictions.

---

## Core Tasks

### Task 1: Plan Your Security Model (15 mins)

**Define roles:**

| Role Name | Filter Condition | Users |
|-----------|------------------|-------|
| AUTOMOBILE_Manager | market_segment = "AUTOMOBILE" | sales1@company.com |
| MACHINERY_Manager | market_segment = "MACHINERY" | sales2@company.com |
| BUILDING_Manager | market_segment = "BUILDING" | sales3@company.com |
| Executive | No filter (sees all) | cfo@company.com |

**Identify filter table:**
- Filter will be on: `DIM_CUSTOMER[market_segment]`
- This propagates to `FCT_ORDER_LINES` through relationship

**Checkpoint:** Security model documented.

---

### Task 2: Create RLS Roles in Desktop (30 mins)

**Open role management:**
1. Open your Power BI file in Desktop
2. **Modeling** > **Manage roles**

**Create first role:**
1. Click **Create**
2. Name: `AUTOMOBILE_Manager`
3. Select table: `DIM_CUSTOMER`
4. Enter DAX filter:
   ```dax
   [market_segment] = "AUTOMOBILE"
   ```
5. Click checkmark to validate

**Create remaining roles:**

**MACHINERY_Manager:**
```dax
[market_segment] = "MACHINERY"
```

**BUILDING_Manager:**
```dax
[market_segment] = "BUILDING"
```

**Executive (optional - no filter):**
- Create role but leave filter blank
- OR don't assign executives to any role

6. Click **Save**

**Checkpoint:** All roles created in Manage roles dialog.

---

### Task 3: Test Roles in Desktop (20 mins)

**Test using View as:**
1. **Modeling** > **View as**
2. Check the role to test (e.g., AUTOMOBILE_Manager)
3. Click **OK**

**Verify filtering:**
1. Navigate through report
2. Check that:
   - Only AUTOMOBILE segment data shows
   - All measures recalculate correctly
   - Charts filter appropriately

3. Document observations:

| Role | Expected Data | Actual Data | Pass/Fail |
|------|---------------|-------------|-----------|
| AUTOMOBILE_Manager | Only AUTOMOBILE | | |
| MACHINERY_Manager | Only MACHINERY | | |
| BUILDING_Manager | Only BUILDING | | |

4. Click **Stop viewing** when done

**Test combined view:**
1. **View as** with role AND check **Other user**
2. Enter a test email
3. This simulates exactly what that user would see

**Checkpoint:** All roles tested and verified.

---

### Task 4: Publish and Configure in Service (25 mins)

**Republish the report:**
1. Save the file with roles
2. **Publish** to your workspace
3. Overwrite existing if prompted

**Assign users to roles:**
1. Open Power BI Service
2. Navigate to your workspace
3. Find the **Dataset** (not report)
4. Click **...** > **Security**

5. For each role:
   - Select the role name
   - Add member email addresses
   - These should be real Power BI accounts

6. Click **Save** for each role

**Note:** You need actual email addresses for users who have Power BI licenses.

**Checkpoint:** Users assigned to roles in Service.

---

### Task 5: Verify RLS in Service (15 mins)

**Test as role in Service:**
1. Click **...** next to dataset > **Security**
2. Select a role
3. Click **...** > **Test as role**
4. Verify filtering works in Service view

**Ask a colleague to verify:**
1. Have someone assigned to a role log in
2. They should only see their segment's data
3. Document their experience

**Checkpoint:** RLS verified working in Service.

---

### Task 6: Implement Dynamic RLS (Optional) (30 mins)

**Dynamic RLS Pattern:**
Instead of hardcoded roles, filter based on user's email.

**Step 1: Create user mapping table (in Snowflake or Power BI)**
```
UserEmail                 | MarketSegment
sales1@company.com        | AUTOMOBILE
sales2@company.com        | MACHINERY
sales3@company.com        | BUILDING
```

**Step 2: Import user mapping table into Power BI**

**Step 3: Create dynamic RLS role:**
```dax
[market_segment] = LOOKUPVALUE(
    UserMapping[MarketSegment],
    UserMapping[UserEmail],
    USERPRINCIPALNAME()
)
```

**Benefits:**
- Single role for all segment managers
- User assignments managed in data, not Power BI
- Easier maintenance at scale

**Checkpoint:** Dynamic RLS implemented (stretch goal).

---

## Deliverables

Submit the following:

1. **Power BI File (.pbix):** With RLS roles configured
2. **Screenshot 1:** Manage roles dialog showing roles
3. **Screenshot 2:** View as test showing filtered data
4. **Screenshot 3:** Security settings in Power BI Service
5. **Test Results:** Role testing documentation

---

## Definition of Done

- [ ] At least 3 RLS roles created
- [ ] DAX filter expressions correct
- [ ] All roles tested in Desktop
- [ ] Report republished to Service
- [ ] Users assigned to roles (or documented)
- [ ] RLS verified in Service
- [ ] Test results documented

---

## Stretch Goals (Optional)

1. Implement dynamic RLS with user lookup
2. Create a role that combines multiple segments
3. Test RLS with a DAX Studio query
4. Document RLS for handoff to admin team

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Role not visible in Service | Republish the file after creating roles |
| Filter shows all data | Check DAX syntax, table/column names |
| User sees nothing | Verify user assigned to role, check filter logic |
| USERPRINCIPALNAME() returns blank | Only works in Service, not Desktop |
| Relationship not filtering | Check relationship crosses filter to fact table |
