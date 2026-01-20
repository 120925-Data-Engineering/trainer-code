# Role-Based Access Control in Data Warehouses

## Learning Objectives
- Explain role-based access control (RBAC) concepts
- Implement RBAC in Snowflake using roles, users, and grants
- Design effective role hierarchies for data teams
- Apply access control best practices for data security

## Why This Matters

In Week 5, you worked with Snowflake and may have noticed that your account had access to certain schemas and tables. That access was not accidental---it was configured through role-based access control.

As a data engineer, you will often be responsible for implementing access controls that determine who can see what data. Get it wrong, and you might expose sensitive customer information to unauthorized users. Get it right, and you enable secure self-service analytics where the right people have the right access.

RBAC is the industry standard approach for managing data access at scale. Instead of granting permissions to individual users, you grant them to roles, and then assign users to roles. This makes access management tractable even with hundreds of users and thousands of data assets.

## The Concept

### What Is Role-Based Access Control?

RBAC is an access control mechanism where permissions are assigned to roles rather than individual users. Users are then assigned to roles, inheriting the permissions of those roles.

**Key Components:**

| Component | Definition | Example |
|-----------|------------|---------|
| User | An individual identity | alice@company.com |
| Role | A collection of permissions | DATA_ANALYST |
| Privilege | A specific permission | SELECT on a table |
| Grant | The act of assigning a privilege | GRANT SELECT TO ROLE |

**RBAC Simplifies Access Management:**

Without RBAC (individual grants):
```
GRANT SELECT ON customers TO alice;
GRANT SELECT ON customers TO bob;
GRANT SELECT ON customers TO carol;
-- ... repeated for every user and every table
```

With RBAC:
```
GRANT SELECT ON customers TO ROLE analyst;
GRANT ROLE analyst TO alice;
GRANT ROLE analyst TO bob;
GRANT ROLE analyst TO carol;
```

### Snowflake Access Control Model

Snowflake uses a hierarchical RBAC model with these key concepts:

#### Securable Objects

Everything in Snowflake that can be protected:
- Account-level: Warehouses, databases, users, roles
- Database-level: Schemas, tables, views
- Schema-level: Tables, views, functions, stages

#### Privileges

Actions that can be performed on securable objects:

| Privilege | Applicable To | Allows |
|-----------|---------------|--------|
| USAGE | Warehouses, databases, schemas | Use the object |
| SELECT | Tables, views | Query data |
| INSERT | Tables | Add data |
| UPDATE | Tables | Modify data |
| DELETE | Tables | Remove data |
| CREATE TABLE | Schemas | Create new tables |
| OWNERSHIP | Any object | Full control including drop |

#### Roles

Named collections of privileges. Snowflake includes system-defined roles:

- **ACCOUNTADMIN:** Full account access, use sparingly
- **SECURITYADMIN:** Manages users, roles, and grants
- **SYSADMIN:** Manages warehouses and databases
- **PUBLIC:** Default role, all users have it

### Role Hierarchy

Roles can be granted to other roles, creating a hierarchy:

```
ACCOUNTADMIN
      |
 SECURITYADMIN
      |
  SYSADMIN
      |
 DATA_ADMIN
    /    \
ANALYST  ENGINEER
```

When Role A is granted to Role B, Role B inherits all privileges of Role A. Users with Role B can access everything Role B can access plus everything Role A can access.

### Designing Role Hierarchies

#### Functional Roles

Roles based on job function:

```
DATA_ENGINEER    - Build and maintain pipelines
DATA_ANALYST     - Query and analyze data
DATA_SCIENTIST   - Access to ML datasets
BUSINESS_USER    - Access to curated reports
```

#### Data-Based Roles

Roles based on data domains:

```
FINANCE_DATA     - Access to financial data
CUSTOMER_DATA    - Access to customer data
HR_DATA          - Access to HR data (restricted)
```

#### Combined Approach

Most organizations use both:

```
ANALYST_FINANCE  - Analyst who works with finance data
ANALYST_CUSTOMER - Analyst who works with customer data
ENGINEER_ALL     - Engineer with access to all domains
```

### Principle of Least Privilege

Grant only the minimum access required for users to do their job:

- Start with no access
- Add specific permissions as needed
- Review and revoke unused access regularly
- Do not use ACCOUNTADMIN for routine work

### Access Control Best Practices

1. **Never grant to individuals directly.** Always use roles.
2. **Use role hierarchies.** Build inheritance to reduce duplication.
3. **Separate duties.** No one role should have unlimited power.
4. **Document access.** Maintain records of who has what access.
5. **Regular audits.** Review access periodically.
6. **Use functional roles.** Align with job functions.
7. **Time-bound access.** Revoke access when no longer needed.

## Code Example

### Creating Roles and Granting Access

```sql
-- =========================================
-- STEP 1: Create custom roles
-- =========================================

USE ROLE SECURITYADMIN;

-- Create functional roles
CREATE ROLE IF NOT EXISTS DATA_ENGINEER;
CREATE ROLE IF NOT EXISTS DATA_ANALYST;
CREATE ROLE IF NOT EXISTS DATA_SCIENTIST;
CREATE ROLE IF NOT EXISTS BUSINESS_USER;

-- Create domain-specific roles
CREATE ROLE IF NOT EXISTS FINANCE_READ;
CREATE ROLE IF NOT EXISTS CUSTOMER_READ;
CREATE ROLE IF NOT EXISTS PRODUCT_READ;

-- =========================================
-- STEP 2: Build role hierarchy
-- =========================================

-- DATA_ENGINEER can access all data domains
GRANT ROLE FINANCE_READ TO ROLE DATA_ENGINEER;
GRANT ROLE CUSTOMER_READ TO ROLE DATA_ENGINEER;
GRANT ROLE PRODUCT_READ TO ROLE DATA_ENGINEER;

-- DATA_ANALYST can access customer and product data
GRANT ROLE CUSTOMER_READ TO ROLE DATA_ANALYST;
GRANT ROLE PRODUCT_READ TO ROLE DATA_ANALYST;

-- DATA_SCIENTIST inherits from DATA_ANALYST
GRANT ROLE DATA_ANALYST TO ROLE DATA_SCIENTIST;

-- SYSADMIN owns the custom roles (for management)
GRANT ROLE DATA_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE DATA_ANALYST TO ROLE SYSADMIN;
GRANT ROLE DATA_SCIENTIST TO ROLE SYSADMIN;
GRANT ROLE BUSINESS_USER TO ROLE SYSADMIN;

-- =========================================
-- STEP 3: Grant warehouse access
-- =========================================

USE ROLE SYSADMIN;

-- Create warehouses for different workloads
CREATE WAREHOUSE IF NOT EXISTS ANALYST_WH 
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE WAREHOUSE IF NOT EXISTS ENGINEER_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE ANALYST_WH TO ROLE DATA_ANALYST;
GRANT USAGE ON WAREHOUSE ANALYST_WH TO ROLE DATA_SCIENTIST;
GRANT USAGE ON WAREHOUSE ANALYST_WH TO ROLE BUSINESS_USER;
GRANT USAGE ON WAREHOUSE ENGINEER_WH TO ROLE DATA_ENGINEER;

-- =========================================
-- STEP 4: Grant database and schema access
-- =========================================

-- Grant USAGE on database to roles that need it
GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE FINANCE_READ;
GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE CUSTOMER_READ;
GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE PRODUCT_READ;

-- Grant USAGE on schemas
GRANT USAGE ON SCHEMA ANALYTICS_DB.FINANCE TO ROLE FINANCE_READ;
GRANT USAGE ON SCHEMA ANALYTICS_DB.CUSTOMERS TO ROLE CUSTOMER_READ;
GRANT USAGE ON SCHEMA ANALYTICS_DB.PRODUCTS TO ROLE PRODUCT_READ;

-- =========================================
-- STEP 5: Grant table-level access
-- =========================================

-- Grant SELECT on all tables in each schema
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS_DB.FINANCE TO ROLE FINANCE_READ;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS_DB.CUSTOMERS TO ROLE CUSTOMER_READ;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYTICS_DB.PRODUCTS TO ROLE PRODUCT_READ;

-- Grant on future tables (for new tables created later)
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS_DB.FINANCE TO ROLE FINANCE_READ;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS_DB.CUSTOMERS TO ROLE CUSTOMER_READ;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS_DB.PRODUCTS TO ROLE PRODUCT_READ;

-- =========================================
-- STEP 6: Grant write access to engineers
-- =========================================

-- Engineers can create and modify tables
GRANT CREATE TABLE ON SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_ENGINEER;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_ENGINEER;
GRANT INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA ANALYTICS_DB.STAGING TO ROLE DATA_ENGINEER;

-- =========================================
-- STEP 7: Assign users to roles
-- =========================================

USE ROLE SECURITYADMIN;

-- Create users
CREATE USER IF NOT EXISTS alice
    PASSWORD = 'ChangeMe123!'
    DEFAULT_ROLE = DATA_ANALYST
    DEFAULT_WAREHOUSE = ANALYST_WH
    MUST_CHANGE_PASSWORD = TRUE;

CREATE USER IF NOT EXISTS bob
    PASSWORD = 'ChangeMe456!'
    DEFAULT_ROLE = DATA_ENGINEER
    DEFAULT_WAREHOUSE = ENGINEER_WH
    MUST_CHANGE_PASSWORD = TRUE;

-- Assign roles to users
GRANT ROLE DATA_ANALYST TO USER alice;
GRANT ROLE DATA_ENGINEER TO USER bob;
```

### Viewing Access Configuration

```sql
-- =========================================
-- AUDIT: View current access configuration
-- =========================================

-- Show all grants TO a role (what the role can do)
SHOW GRANTS TO ROLE DATA_ANALYST;

-- Show all grants OF a role (who has the role)
SHOW GRANTS OF ROLE DATA_ANALYST;

-- Show all grants ON an object (who can access it)
SHOW GRANTS ON TABLE ANALYTICS_DB.CUSTOMERS.DIM_CUSTOMERS;

-- Show role hierarchy
SHOW GRANTS OF ROLE DATA_ENGINEER;

-- Show all roles for a user
SHOW GRANTS TO USER alice;

-- =========================================
-- QUERY: Access audit from Account Usage
-- =========================================

-- Find all users with access to a specific table
SELECT 
    grantee_name,
    privilege,
    granted_on,
    name
FROM snowflake.account_usage.grants_to_roles
WHERE name = 'DIM_CUSTOMERS'
  AND deleted_on IS NULL
ORDER BY grantee_name;

-- Find all tables a role can access
SELECT 
    granted_on,
    name,
    privilege
FROM snowflake.account_usage.grants_to_roles
WHERE grantee_name = 'DATA_ANALYST'
  AND granted_on = 'TABLE'
  AND deleted_on IS NULL
ORDER BY name;
```

### Testing Access

```sql
-- =========================================
-- TEST: Verify access works as expected
-- =========================================

-- Switch to the analyst role
USE ROLE DATA_ANALYST;
USE WAREHOUSE ANALYST_WH;

-- This should work (customer data access)
SELECT * FROM ANALYTICS_DB.CUSTOMERS.DIM_CUSTOMERS LIMIT 10;

-- This should fail (no finance access for analysts)
SELECT * FROM ANALYTICS_DB.FINANCE.FCT_REVENUE LIMIT 10;
-- Error: Object 'ANALYTICS_DB.FINANCE.FCT_REVENUE' does not exist or not authorized.

-- Switch to engineer role
USE ROLE DATA_ENGINEER;

-- This should work (engineers have finance access)
SELECT * FROM ANALYTICS_DB.FINANCE.FCT_REVENUE LIMIT 10;
```

## Summary

- **RBAC** assigns permissions to roles, then users to roles, simplifying access management
- **Snowflake access control** uses a hierarchy of securable objects, privileges, and roles
- **System-defined roles** (ACCOUNTADMIN, SECURITYADMIN, SYSADMIN) provide baseline capabilities
- **Role hierarchies** allow roles to inherit permissions from other roles
- Design roles around **job functions** and **data domains**
- Apply the **principle of least privilege**: grant only what is needed
- Use **GRANT ON FUTURE** to automatically grant access to new objects
- Regularly **audit access** using SHOW GRANTS and Account Usage views
- **Never grant directly to users**; always use roles

## Additional Resources

- [Snowflake Access Control Documentation](https://docs.snowflake.com/en/user-guide/security-access-control) - Official Snowflake RBAC guide
- [Snowflake Role Hierarchy Best Practices](https://docs.snowflake.com/en/user-guide/security-access-control-considerations) - Design patterns for roles
- [NIST RBAC Model](https://csrc.nist.gov/projects/role-based-access-control) - Industry standard RBAC framework
