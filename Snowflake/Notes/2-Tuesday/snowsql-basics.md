# SnowSQL Basics

## Learning Objectives

- Install and configure the SnowSQL command-line client
- Establish connections to Snowflake using SnowSQL
- Execute essential SnowSQL commands for data exploration
- Understand configuration options for efficient workflow

## Why This Matters

While the Snowflake web UI (Snowsight) provides a rich graphical interface, the SnowSQL command-line client enables automation, scripting, and integration with development workflows. As a data engineer, mastering SnowSQL allows you to incorporate Snowflake operations into CI/CD pipelines, automate data loading, and work efficiently in terminal-based environments.

## The Concept

### What is SnowSQL?

**SnowSQL** is Snowflake's official command-line client. It allows you to:
- Execute SQL queries and DDL statements
- Load and unload data files
- Run SQL scripts from files
- Automate Snowflake operations in shell scripts

### Installation

SnowSQL is available for Windows, macOS, and Linux.

**Windows (Installer):**
1. Download the installer from the [Snowflake Downloads page](https://developers.snowflake.com/snowsql/)
2. Run the .msi installer
3. Follow the installation wizard

**macOS (Homebrew):**
```bash
brew install --cask snowflake-snowsql
```

**Linux (Package):**
```bash
# Download the installer
curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/<version>/linux_x86_64/snowsql-<version>-linux_x86_64.bash

# Run the installer
bash snowsql-<version>-linux_x86_64.bash
```

**Verify Installation:**
```bash
snowsql --version
```

### Configuration

SnowSQL uses a configuration file for connection settings. The default location is:

- **Windows**: `%USERPROFILE%\.snowsql\config`
- **macOS/Linux**: `~/.snowsql/config`

**Example Configuration:**
```ini
[connections]
# Default connection
accountname = myaccount
username = myuser
password = mypassword

[connections.dev]
# Named connection for development
accountname = myaccount
username = dev_user
dbname = DEV_DB
schemaname = PUBLIC
warehousename = DEV_WH

[connections.prod]
# Named connection for production
accountname = myaccount
username = prod_user
dbname = PROD_DB
schemaname = ANALYTICS
warehousename = PROD_WH
```

**Security Note:** For production environments, avoid storing passwords in plain text. Use:
- Environment variables: `SNOWSQL_PWD`
- Key pair authentication
- External browser authentication (SSO)

### Connecting to Snowflake

**Basic Connection:**
```bash
# Interactive prompt for password
snowsql -a <account> -u <username>

# With password (not recommended for production)
snowsql -a myaccount -u myuser -p 'mypassword'
```

**Using Named Connections:**
```bash
# Use the 'dev' connection from config
snowsql -c dev

# Use the 'prod' connection
snowsql -c prod
```

**Account Identifier Format:**
```
# Format: <orgname>-<account_name> or <account_locator>.<region>.<cloud>
# Examples:
myorg-myaccount
xy12345.us-east-1.aws
ab12345.east-us-2.azure
```

**Connection with Database Context:**
```bash
snowsql -a myaccount -u myuser -d ANALYTICS_DB -s GOLD -w MY_WAREHOUSE
```

### Essential SnowSQL Commands

Once connected, you can execute SQL and SnowSQL-specific commands.

**SQL Execution:**
```sql
-- Standard SQL works as expected
SELECT CURRENT_TIMESTAMP();
SELECT * FROM orders LIMIT 10;
CREATE TABLE test (id INT, name STRING);
```

**SnowSQL Meta-Commands:**

| Command | Description |
|---------|-------------|
| `!help` | Display help information |
| `!exit` or `!quit` | Exit SnowSQL |
| `!set` | View or set session variables |
| `!source <file>` | Execute SQL from a file |
| `!print <message>` | Print a message |
| `!define <var>=<value>` | Define a variable |

**Context Commands:**
```sql
-- Set context
USE DATABASE analytics_db;
USE SCHEMA gold;
USE WAREHOUSE compute_wh;
USE ROLE analyst_role;

-- View current context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();
```

**Object Exploration:**
```sql
-- List databases
SHOW DATABASES;

-- List schemas in current database
SHOW SCHEMAS;

-- List tables in current schema
SHOW TABLES;

-- Describe table structure
DESCRIBE TABLE orders;
-- or
DESC TABLE orders;

-- List columns
SHOW COLUMNS IN TABLE orders;

-- List warehouses
SHOW WAREHOUSES;
```

### Running SQL Scripts

SnowSQL can execute SQL scripts from files, enabling automation and version control.

**Execute a Script:**
```bash
# From command line
snowsql -a myaccount -u myuser -f my_script.sql

# From within SnowSQL
!source my_script.sql
```

**Script with Variables:**
```bash
# Define variables on command line
snowsql -a myaccount -u myuser -D target_date='2024-01-01' -f load_data.sql
```

**In the SQL file (load_data.sql):**
```sql
-- Use the variable with &
SELECT * FROM orders WHERE order_date = '&target_date';
```

### Output Formatting

Control how results are displayed:

```bash
# Set output format
!set output_format=csv
!set output_format=json
!set output_format=psql  # Default table format

# Save output to file
!set output_file=/path/to/output.csv
SELECT * FROM orders;
!set output_file=  # Reset to console

# From command line, save results
snowsql -a myaccount -u myuser -o output_file=results.csv -o output_format=csv -q "SELECT * FROM orders"
```

### Common Options

| Option | Description |
|--------|-------------|
| `-a, --accountname` | Snowflake account identifier |
| `-u, --username` | Username |
| `-d, --dbname` | Database name |
| `-s, --schemaname` | Schema name |
| `-w, --warehouse` | Warehouse name |
| `-r, --rolename` | Role name |
| `-f, --filename` | SQL file to execute |
| `-q, --query` | SQL query to execute |
| `-o, --option` | Set option (key=value) |
| `-c, --connection` | Named connection from config |

### Practical Workflow Example

```bash
# 1. Connect to development environment
snowsql -c dev

# 2. Check current context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();

# 3. Explore available tables
SHOW TABLES LIKE '%order%';

# 4. Query data
SELECT order_date, COUNT(*) AS order_count
FROM orders
GROUP BY order_date
ORDER BY order_date DESC
LIMIT 10;

# 5. Run a maintenance script
!source /scripts/daily_cleanup.sql

# 6. Exit
!exit
```

## Summary

- **SnowSQL** is Snowflake's command-line client for SQL execution and automation
- Install via installer (Windows), Homebrew (macOS), or package (Linux)
- Configure connections in `~/.snowsql/config` using named connection profiles
- Use **meta-commands** (prefixed with `!`) for SnowSQL-specific operations
- Execute SQL scripts with `-f` flag or `!source` command
- Control output format and destination for automation workflows

## Additional Resources

- [Snowflake Documentation: SnowSQL](https://docs.snowflake.com/en/user-guide/snowsql)
- [Snowflake Documentation: SnowSQL Configuration](https://docs.snowflake.com/en/user-guide/snowsql-config)
- [SnowSQL Download Page](https://developers.snowflake.com/snowsql/)
