# Power BI Service

## Learning Objectives
- Publish reports from Desktop to Service
- Organize content using workspaces
- Configure sharing and permissions
- Implement row-level security (RLS)
- Understand embedding options

## Why This Matters

Power BI Desktop is for building; Power BI Service is for sharing. Without the Service, reports remain local files. Understanding workspace organization, sharing, and security enables you to deploy reports appropriately across your organization.

## Publishing to Power BI Service

### Publish from Desktop

1. Open your report in Power BI Desktop
2. Click Home > Publish
3. Sign in to your Power BI account
4. Select a destination workspace
5. Click "Select"
6. Wait for publishing to complete

### What Gets Published

- The .pbix file content (data model, reports)
- Dataset (data and model)
- Report (visualizations on pages)
- Both appear in the workspace

### Updating Published Reports

After making changes:
1. Click "Publish" again
2. Choose the same workspace
3. Click "Replace" when prompted
4. Changes overwrite the previous version

## Workspaces

Workspaces are containers for organizing content.

### Workspace Types

| Type | Purpose | Access |
|------|---------|--------|
| **My workspace** | Personal content | Only you |
| **Shared workspaces** | Team collaboration | Members you add |
| **App workspaces** | Distribution via apps | Members + app audiences |

### Creating a Workspace

1. In Power BI Service, click Workspaces in left nav
2. Click "Create a workspace"
3. Enter workspace name
4. Optionally upload a workspace image
5. Click "Save"

### Workspace Roles

| Role | Permissions |
|------|-------------|
| **Admin** | Full control, manage membership, delete workspace |
| **Member** | Create, edit, delete all content |
| **Contributor** | Create and edit, but cannot delete others' content |
| **Viewer** | View content only |

### Adding Members

1. Open the workspace
2. Click "Access" in the top bar
3. Enter email addresses
4. Select role
5. Click "Add"

## Sharing Reports and Dashboards

### Direct Sharing

Share specific items with colleagues:
1. Open the report or dashboard
2. Click "Share"
3. Enter recipient emails
4. Set permissions (allow reshare, build content)
5. Add optional message
6. Click "Grant access"

### Links

Generate shareable links:
- "People in your organization"
- "People with existing access"  
- "Specific people"

Control what recipients can do:
- View only
- Allow resharing
- Allow building on dataset

### Apps

Package workspace content for distribution:

1. In workspace, click "Create app"
2. Configure app name and description
3. Add content (reports, dashboards)
4. Set access (entire organization, specific groups)
5. Publish app

Apps provide:
- Curated content experience
- Simplified navigation
- Clear separation from development workspace

## Row-Level Security (RLS)

Control data access at the row level.

### RLS Concept

Different users see different subsets of data:
- Sales rep sees only their region
- Manager sees all regions
- Executive sees company-wide data

### Creating RLS Roles (Desktop)

1. In Power BI Desktop, go to Modeling > Manage roles
2. Click "Create"
3. Name the role (e.g., "North Region")
4. Select a table
5. Add DAX filter expression:
   ```dax
   [Region] = "North"
   ```
6. Click Save

### Dynamic RLS

Use USERNAME() or USERPRINCIPALNAME() for user-specific filtering:

```dax
[SalesRepEmail] = USERPRINCIPALNAME()
```

This returns rows where SalesRepEmail matches the logged-in user.

### Testing RLS (Desktop)

1. Go to Modeling > View as
2. Select roles to test
3. Optionally enter a username
4. View report as that user/role

### Assigning Users to Roles (Service)

After publishing:
1. Go to dataset settings in workspace
2. Expand "Row-level security"
3. Click ellipsis on a role > "Members"
4. Add users or security groups
5. Click "Add"

### RLS Best Practices

- Use security groups, not individual users
- Test thoroughly before production
- Document role definitions
- Consider performance impact of complex filters
- Use views in the database for simpler management

## Sharing Security Summary

| Method | Best For | RLS Applied |
|--------|----------|-------------|
| Direct share | Individual items | Yes |
| Workspace access | Team collaboration | Yes |
| Apps | Broad distribution | Yes |
| Publish to web | Public, anonymous | No (no security) |
| Embedded | Custom apps | Configurable |

## Embedding Power BI

Integrate Power BI into other applications.

### Embedding Options

| Option | Use Case | Licensing |
|--------|----------|-----------|
| **Publish to web** | Public websites | Free (no security) |
| **SharePoint Online** | Intranet integration | Pro/Premium |
| **Microsoft Teams** | Team collaboration | Pro/Premium |
| **Embed for organization** | Internal apps | Pro/Premium |
| **Embed for customers** | Customer-facing apps | Premium/Embedded |

### Publish to Web

Create public, anonymous embed:
1. Report ellipsis > Embed report > Publish to web
2. Accept warning about public access
3. Copy embed code or link

**Warning**: Anyone with the link can view. Never use for sensitive data.

### Secure Embedding

For secure embedding in custom apps:
- Use Power BI Embedded service
- Authenticate via Azure AD
- Row-level security applies
- Requires development work

## Content Certification

Mark trusted content for discovery.

### Certification Levels

| Level | Meaning |
|-------|---------|
| **Promoted** | Deemed valuable or ready for use |
| **Certified** | Formally reviewed and approved |

### Promoting Content

1. Dataset settings > Endorsement
2. Select "Promoted"
3. Add optional description
4. Save

### Certifying Content

Requires admin configuration:
1. Admin enables certification in admin portal
2. Designates certifiers
3. Certifiers add "Certified" endorsement to content

Certified content shows a badge in the workspace.

## Summary

- Publish from Desktop to share reports via Power BI Service
- Workspaces organize content with role-based access (Admin, Member, Contributor, Viewer)
- Share directly or create apps for broader distribution
- Row-level security filters data based on user identity
- Embedding options range from public (Publish to web) to secure (Power BI Embedded)
- Certification badges help users find trusted content

## Additional Resources

- [Power BI Service Overview](https://docs.microsoft.com/en-us/power-bi/fundamentals/power-bi-service-overview) - Service documentation
- [Row-Level Security](https://docs.microsoft.com/en-us/power-bi/admin/service-admin-rls) - RLS guide
- [Power BI Embedding](https://docs.microsoft.com/en-us/power-bi/developer/embedded/) - Embedding documentation
