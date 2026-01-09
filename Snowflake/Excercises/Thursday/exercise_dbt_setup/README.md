# Lab: dbt Installation and Snowflake Configuration

## Overview
**Day:** 4-Thursday  
**Duration:** 1-1.5 hours  
**Mode:** Individual (Code Lab)  
**Prerequisites:** Python 3.8-3.12 installed (NOT 3.13 or 3.14), Snowflake account with credentials

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| dbt Introduction | [dbt-introduction.md](../../content/4-Thursday/dbt-introduction.md) | What dbt is, analytics engineering workflow |
| dbt Project Structure | [dbt-project-structure.md](../../content/4-Thursday/dbt-project-structure.md) | Project anatomy, dbt_project.yml, profiles.yml |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Install dbt Core using pip in a virtual environment
2. Configure profiles.yml for Snowflake connection
3. Securely manage Snowflake credentials using environment variables
4. Verify your dbt-Snowflake connection
5. Initialize and explore a dbt project structure

---

## The Scenario

Before you can build dbt models, you need a working dbt installation connected to Snowflake. This exercise walks you through the complete setup process on both Windows and macOS, ensuring you have a production-ready configuration with secure credential management.

---

## Part 1: Python Environment Setup

### Why a Virtual Environment?

dbt has specific dependency versions. A virtual environment isolates these from other Python projects, preventing conflicts.

### Windows Setup

```powershell
# Open PowerShell as Administrator (if needed for Python)

# Navigate to your projects directory
cd C:\Users\<YourUsername>\Projects

# Create a dbt project folder
mkdir dbt_setup_lab
cd dbt_setup_lab

# Create virtual environment
python -m venv dbt_env

# Activate virtual environment
.\dbt_env\Scripts\Activate.ps1

# If you get an execution policy error, run:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Verify activation (you should see (dbt_env) in your prompt)
```

### macOS/Linux Setup

```bash
# FIRST: Check your Python version
python3 --version
# Must be between 3.8 and 3.12. If you have 3.13 or 3.14, see troubleshooting below.

# If you have multiple Python versions, use a specific one:
# python3.11 -m venv dbt_env  (example)

# Navigate to your projects directory
cd ~/Projects

# Create a dbt project folder
mkdir dbt_setup_lab
cd dbt_setup_lab

# Create virtual environment (use python3.11 or python3.12 if needed)
python3 -m venv dbt_env

# Activate virtual environment
source dbt_env/bin/activate

# Verify activation (you should see (dbt_env) in your prompt)
# Also verify Python version in the venv:
python --version
```

**Checkpoint:** Your terminal prompt should show `(dbt_env)` indicating the virtual environment is active.

---

## Part 2: Install dbt-snowflake

With your virtual environment activated:

```bash
# Install dbt-snowflake (includes dbt-core)
pip install dbt-snowflake

# Verify installation
dbt --version
```

**Expected Output:**
```
Core:
  - installed: 1.7.x (or similar)
  - latest:    1.7.x

Plugins:
  - snowflake: 1.7.x
```

**Checkpoint:** `dbt --version` runs without errors and shows both Core and snowflake plugin.

---

## Part 3: Gather Snowflake Credentials

Before configuring dbt, collect these values from your Snowflake account:

| Parameter | Where to Find It | Example |
|-----------|------------------|---------|
| **Account** | See "Finding Your Account Identifier" below | `gbhqahj-gd07839` |
| **Username** | Your login username | `john_doe` |
| **Password** | Your login password | (keep secret!) |
| **Role** | Current role in Snowflake UI | `ACCOUNTADMIN` |
| **Warehouse** | Compute warehouse name | `COMPUTE_WH` |
| **Database** | Target database | `DEV_DB` |
| **Schema** | Target schema for dbt objects | `DBT_<YOUR_NAME>` |

### Finding Your Account Identifier

Snowflake has two URL formats. Your account identifier depends on which format you see:

**Modern Format (app.snowflake.com):**
```
URL: https://app.snowflake.com/gbhqakj/gd07839/#/homepage
                              └──org──┘ └─acct─┘

Account Identifier: gbhqakj-gd07839
                    (org-accountname, joined with hyphen)
```

**Legacy Format (snowflakecomputing.com):**
```
URL: https://abc12345.us-east-1.snowflakecomputing.com
           └───────account locator────────┘

Account Identifier: abc12345.us-east-1
                    (account locator with region)
```

**How to find it in Snowflake UI:**
1. Log into Snowflake
2. Click your username (bottom-left corner)
3. Hover over your account name
4. Click the copy icon to copy the account identifier

**Important:** Do NOT include `https://`, `app.snowflake.com`, or `.snowflakecomputing.com` in your account identifier.

---

## Part 4: Create profiles.yml

The `profiles.yml` file stores your connection settings. It lives OUTSIDE your dbt project (in your home directory) so credentials aren't committed to Git.

### Windows: Create profiles.yml

```powershell
# Create .dbt directory in your user folder
mkdir $HOME\.dbt

# Create the profiles.yml file
notepad $HOME\.dbt\profiles.yml
```

### macOS/Linux: Create profiles.yml

```bash
# Create .dbt directory in your home folder
mkdir -p ~/.dbt

# Create the profiles.yml file
nano ~/.dbt/profiles.yml
# Or use: code ~/.dbt/profiles.yml (VS Code)
```

### profiles.yml Content

Add this content, replacing placeholders with your values:

```yaml
snowflake_training:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <YOUR_ACCOUNT_IDENTIFIER>   # e.g., abc12345.us-east-1
      user: <YOUR_USERNAME>
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: ACCOUNTADMIN
      warehouse: COMPUTE_WH
      database: DEV_DB
      schema: DBT_<YOUR_NAME>              # e.g., DBT_JOHN
      threads: 4
      client_session_keep_alive: false
```

**Important:** Notice the password uses `{{ env_var('SNOWFLAKE_PASSWORD') }}` - this is dbt's Jinja syntax to read from an environment variable. Never hardcode passwords!

---

## Part 5: Set Environment Variable for Password

### Windows (PowerShell - Current Session)

```powershell
# Set for current session only
$env:SNOWFLAKE_PASSWORD = "your_actual_password"

# Verify it's set
echo $env:SNOWFLAKE_PASSWORD
```

### Windows (Permanent - User Environment Variable)

```powershell
# Set permanently for your user account
[Environment]::SetEnvironmentVariable("SNOWFLAKE_PASSWORD", "your_actual_password", "User")

# Restart PowerShell for it to take effect
```

### macOS/Linux (Current Session)

```bash
# Set for current session only
export SNOWFLAKE_PASSWORD="your_actual_password"

# Verify it's set
echo $SNOWFLAKE_PASSWORD
```

### macOS/Linux (Permanent)

Add to `~/.bashrc`, `~/.zshrc`, or `~/.bash_profile`:

```bash
export SNOWFLAKE_PASSWORD="your_actual_password"
```

Then reload:
```bash
source ~/.bashrc  # or ~/.zshrc
```

**Security Note:** For production, use a secrets manager or dbt Cloud's secrets feature. Environment variables are acceptable for development.

---

## Part 6: Initialize a dbt Project

Since we already created `profiles.yml` in Part 4, we'll tell dbt to **skip profile setup** and only create the project structure.

```bash
# Make sure you're in your lab directory with venv activated
cd dbt_setup_lab  # or wherever you created the folder

# Initialize project WITHOUT overwriting profiles.yml
dbt init snowflake_analytics --skip-profile-setup
```

When prompted for database type, enter `1` for Snowflake. dbt will create the project files but **not** touch your existing profiles.yml.

### Update dbt_project.yml Profile Name

The project needs to know which profile to use. Open `snowflake_analytics/dbt_project.yml` and verify the profile name matches your profiles.yml:

```yaml
# dbt_project.yml
name: 'snowflake_analytics'
profile: 'snowflake_training'  # Must match the name in ~/.dbt/profiles.yml
```

```bash
# Navigate into the project
cd snowflake_analytics
```

---

## Part 7: Test Your Connection

This is the moment of truth!

```bash
dbt debug
```

**Expected Output (Success):**
```
Running with dbt=1.7.x
dbt version: 1.7.x
...
Configuration:
  profiles.yml file [OK found and valid]
  dbt_project.yml file [OK found and valid]
...
Connection:
  account: abc12345.us-east-1
  user: john_doe
  database: DEV_DB
  schema: DBT_JOHN
  warehouse: COMPUTE_WH
  role: ACCOUNTADMIN
  ...
  Connection test: [OK connection ok]

All checks passed!
```

### Troubleshooting Common Errors

| Error | Likely Cause | Solution |
|-------|--------------|----------|
| `mashumaro.exceptions.UnserializableField` or similar traceback on `dbt --version` | Python 3.13 or 3.14 (unsupported) | Install Python 3.11 or 3.12, recreate venv with that version |
| `Could not find profile named 'snowflake_training'` | Profile name mismatch | Check `profile:` in dbt_project.yml matches profiles.yml |
| `Failed to connect to DB: 250001` | Wrong account identifier | Verify account format (no `.snowflakecomputing.com`) |
| `Incorrect username or password` | Wrong credentials or missing env var | Check `echo $env:SNOWFLAKE_PASSWORD` (Windows) or `echo $SNOWFLAKE_PASSWORD` (Mac) |
| `env_var('SNOWFLAKE_PASSWORD') is undefined` | Environment variable not set | Set the env var and restart terminal |
| `Role 'ACCOUNTADMIN' does not exist` | Wrong role | Use role from Snowflake UI dropdown |

### Python Version Issue (macOS)

If you see errors like `mashumaro.exceptions.UnserializableField` when running `dbt --version`, you have an unsupported Python version.

**dbt supports Python 3.8 - 3.12 only.** Python 3.13 and 3.14 are NOT supported.

**Fix:**
```bash
# Check current version
python3 --version

# If 3.13 or 3.14, install Python 3.11 or 3.12:
# Using Homebrew (recommended):
brew install python@3.11

# Deactivate current venv
deactivate

# Remove old venv
rm -rf dbt_env

# Create new venv with correct Python version
/opt/homebrew/bin/python3.11 -m venv dbt_env
# Or: /usr/local/bin/python3.11 -m venv dbt_env (Intel Mac)

# Activate and reinstall
source dbt_env/bin/activate
python --version  # Should show 3.11.x
pip install dbt-snowflake
dbt --version  # Should work now
```

---

## Part 8: Explore Project Structure

After successful connection, explore what dbt created:

```bash
# List project contents
ls -la  # macOS/Linux
dir     # Windows
```

**Key Files:**
```
snowflake_analytics/
├── dbt_project.yml      # Project configuration
├── models/              # Your SQL transformations go here
│   └── example/
├── seeds/               # CSV files for static data
├── snapshots/           # Slowly changing dimension logic
├── tests/               # Custom data tests
├── macros/              # Jinja macros (reusable code)
└── README.md            # Project documentation
```

### Quick Verification: Run Example Models

```bash
# Run the example models that dbt init created
dbt run

# Check Snowflake - you should see new objects in your schema
```

---

## Part 9: Create Your Schema in Snowflake (if needed)

If `dbt run` fails because the schema doesn't exist:

```sql
-- Run in Snowflake
USE ROLE ACCOUNTADMIN;
USE DATABASE DEV_DB;

CREATE SCHEMA IF NOT EXISTS DBT_<YOUR_NAME>;

GRANT ALL ON SCHEMA DBT_<YOUR_NAME> TO ROLE ACCOUNTADMIN;
```

Re-run `dbt run` and it should succeed.

---

## Deliverables

Submit the following:

1. **Screenshot of `dbt debug` output** showing "All checks passed!"
2. **Screenshot of Snowflake** showing your DBT_<name> schema with example models
3. **Answers to these questions:**
   - What is the purpose of profiles.yml vs dbt_project.yml?
   - Why do we use `{{ env_var('SNOWFLAKE_PASSWORD') }}` instead of the actual password?
   - What directory contains your transformation SQL files?

---

## Definition of Done

- [ ] Python virtual environment created and activated
- [ ] dbt-snowflake installed successfully
- [ ] profiles.yml created with Snowflake configuration
- [ ] SNOWFLAKE_PASSWORD environment variable set
- [ ] `dbt debug` shows "All checks passed!"
- [ ] dbt project initialized
- [ ] `dbt run` executes successfully
- [ ] Example models visible in Snowflake

---

## Bonus Challenges

1. **Multiple Environments:** Add a `prod` target to profiles.yml pointing to a different schema
2. **Alternative Authentication:** Research and document how to use key-pair authentication instead of password
3. **VS Code Integration:** Install the dbt Power User extension and connect it to your project
