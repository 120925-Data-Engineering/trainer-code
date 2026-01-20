# CI/CD for dbt Projects

## Learning Objectives
- Explain what CI/CD means in the context of dbt projects
- Configure GitHub Actions to run dbt tests automatically
- Implement a deployment strategy for dbt across environments
- Understand best practices for dbt project automation

## Why This Matters

In Week 5, you learned to build dbt models that transform raw data into analytics-ready tables. But how do those models get from your laptop to production? In enterprise environments, you cannot simply run `dbt run` on your local machine and call it a day.

Consider this scenario: A data engineer makes a change to a critical revenue model. Without proper guardrails, that change could break downstream dashboards that executives rely on for decision-making. The cost of such errors---in terms of lost trust, incorrect decisions, and engineer time spent firefighting---is enormous.

**CI/CD (Continuous Integration and Continuous Deployment)** for dbt solves this problem. By automating testing and deployment, you ensure that every change is validated before it reaches production, and deployments happen consistently and reliably.

## The Concept

### What Is CI/CD?

**Continuous Integration (CI)** is the practice of automatically testing code changes every time they are committed. For dbt, this means running your dbt tests, checking for compilation errors, and validating model logic whenever someone opens a pull request.

**Continuous Deployment (CD)** is the practice of automatically deploying validated changes to production. For dbt, this means running `dbt run` in your production environment after changes are merged to the main branch.

Together, CI/CD creates a pipeline that looks like this:

```
Developer makes change
        |
        v
Push to feature branch
        |
        v
CI runs dbt test (automated)
        |
        v
Code review by peers
        |
        v
Merge to main branch
        |
        v
CD deploys to production (automated)
        |
        v
Production data updated
```

### Environment Strategy for dbt

A well-structured dbt project runs in multiple environments:

| Environment | Purpose | When Changes Run |
|-------------|---------|------------------|
| Development | Local testing by individual engineers | On demand by engineer |
| CI/Test | Automated validation of pull requests | On every pull request |
| Staging | Pre-production validation | After merge to main |
| Production | Serves business users | After staging validation |

Each environment typically uses a separate database schema or even a separate Snowflake database to isolate changes.

### GitHub Actions for dbt CI

GitHub Actions is a popular choice for dbt CI/CD because it integrates directly with your Git repository. When you open a pull request, GitHub Actions automatically runs your defined workflows.

A typical dbt CI workflow includes these steps:

1. **Checkout code**: Get the latest version of your dbt project
2. **Set up Python**: Install Python and pip
3. **Install dbt**: Install dbt and required adapters
4. **Configure credentials**: Set up connection to your data warehouse
5. **Run dbt deps**: Install dbt packages
6. **Run dbt compile**: Check for syntax errors
7. **Run dbt test**: Execute all tests
8. **Report results**: Show pass/fail status on the pull request

### Deployment Strategies

There are several approaches to deploying dbt to production:

**Strategy 1: Merge and Deploy**
- Changes are deployed immediately after merging to main
- Simple but requires strong test coverage
- Suitable for smaller teams with mature testing practices

**Strategy 2: Scheduled Runs**
- Production runs on a schedule (e.g., daily at 6 AM)
- Merged code waits until the next scheduled run
- Provides a buffer for catching issues

**Strategy 3: Staged Promotion**
- Changes go through staging before production
- Staging environment is validated manually or with additional tests
- Most conservative approach for critical data

### Handling dbt Tests in CI

Your CI pipeline should run different types of dbt tests:

- **Schema tests**: Validate column properties (unique, not_null, accepted_values)
- **Data tests**: Custom SQL tests for business logic
- **Freshness tests**: Ensure source data is up to date
- **Contract tests**: Validate model schemas match expectations

## Code Example

### GitHub Actions Workflow for dbt

Create this file at `.github/workflows/dbt_ci.yml` in your repository:

```yaml
name: dbt CI

on:
  pull_request:
    branches:
      - main
    paths:
      - 'dbt_project/**'
      - '.github/workflows/dbt_ci.yml'

env:
  DBT_PROFILES_DIR: ./dbt_project

jobs:
  dbt-test:
    name: Run dbt Tests
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dbt
        run: |
          pip install --upgrade pip
          pip install dbt-snowflake==1.7.0

      - name: Configure dbt profile
        run: |
          mkdir -p ~/.dbt
          echo "${{ secrets.DBT_PROFILES_YML }}" > ~/.dbt/profiles.yml

      - name: Install dbt packages
        run: |
          cd dbt_project
          dbt deps

      - name: Compile dbt models
        run: |
          cd dbt_project
          dbt compile --target ci

      - name: Run dbt tests
        run: |
          cd dbt_project
          dbt test --target ci

      - name: Generate documentation
        run: |
          cd dbt_project
          dbt docs generate --target ci
```

### dbt Profile for CI Environment

Your `profiles.yml` should include a CI target:

```yaml
my_dbt_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: DATA_ENGINEER
      database: DEV_DB
      warehouse: DEV_WH
      schema: "{{ env_var('USER') }}_dev"
      threads: 4

    ci:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_CI_USER') }}"
      password: "{{ env_var('SNOWFLAKE_CI_PASSWORD') }}"
      role: CI_RUNNER
      database: CI_DB
      warehouse: CI_WH
      schema: ci_test
      threads: 4

    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_PROD_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PROD_PASSWORD') }}"
      role: DATA_ENGINEER_PROD
      database: PROD_DB
      warehouse: PROD_WH
      schema: analytics
      threads: 8
```

### Deployment Workflow

Create `.github/workflows/dbt_deploy.yml` for production deployment:

```yaml
name: dbt Deploy to Production

on:
  push:
    branches:
      - main
    paths:
      - 'dbt_project/**'

jobs:
  deploy:
    name: Deploy dbt to Production
    runs-on: ubuntu-latest
    environment: production
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dbt
        run: |
          pip install --upgrade pip
          pip install dbt-snowflake==1.7.0

      - name: Configure dbt profile
        run: |
          mkdir -p ~/.dbt
          echo "${{ secrets.DBT_PROD_PROFILES_YML }}" > ~/.dbt/profiles.yml

      - name: Install dbt packages
        run: |
          cd dbt_project
          dbt deps

      - name: Run dbt in production
        run: |
          cd dbt_project
          dbt run --target prod

      - name: Run dbt tests in production
        run: |
          cd dbt_project
          dbt test --target prod
```

### Slim CI with State Comparison

For larger projects, running all models on every PR is expensive. dbt supports "slim CI" which only runs modified models:

```yaml
- name: Run modified models only
  run: |
    cd dbt_project
    dbt run --select state:modified+ --defer --state ./prod_manifest --target ci
```

This compares your changes against the production manifest and only runs models that have changed, plus their downstream dependencies.

## Summary

- **CI** automatically tests dbt projects when pull requests are opened, catching errors before they reach production
- **CD** automatically deploys validated changes, ensuring consistent and reliable releases
- **GitHub Actions** integrates seamlessly with Git repositories to run dbt commands on each commit
- Use **separate environments** (dev, ci, staging, prod) with different database schemas for isolation
- **Slim CI** runs only modified models for faster feedback on large projects
- Store **credentials as secrets**, never in code

## Additional Resources

- [dbt Documentation: CI Jobs](https://docs.getdbt.com/docs/deploy/ci-jobs) - Official dbt guidance on CI configuration
- [GitHub Actions Documentation](https://docs.github.com/en/actions) - Complete reference for GitHub Actions
- [dbt Cloud CI Features](https://docs.getdbt.com/docs/deploy/cloud-ci-job) - If using dbt Cloud for managed CI/CD
