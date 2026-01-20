# CI/CD for Airflow DAGs

## Learning Objectives
- Explain the unique challenges of CI/CD for Airflow DAGs
- Implement DAG validation and testing before deployment
- Configure a CI pipeline that tests DAGs on every pull request
- Understand versioning and promotion strategies for Airflow

## Why This Matters

In Week 4, you built Airflow DAGs to orchestrate data pipelines. Those DAGs define when and how your data moves through the system. A broken DAG can halt your entire data platform---no fresh data for analysts, no updated dashboards for stakeholders, and frantic on-call engineers scrambling to fix the issue.

Unlike application code where errors might affect one user, a flawed Airflow DAG can corrupt data for an entire organization. Imagine a DAG that accidentally deletes production tables or runs transformations out of order. The consequences are severe and often difficult to reverse.

**CI/CD for Airflow** ensures that every DAG change is tested thoroughly before it runs in production. By catching syntax errors, import failures, and logic bugs early, you prevent costly outages and maintain trust in your data platform.

## The Concept

### Why Airflow CI/CD Is Different

Airflow DAGs present unique CI/CD challenges:

1. **DAGs are Python code** but also define schedules and dependencies
2. **DAG parsing happens at runtime** in the Airflow scheduler
3. **Testing requires an Airflow context** (or clever mocking)
4. **DAGs interact with external systems** that may not be available in CI
5. **Deployment means syncing files** to the DAGs folder, not deploying a server

### What to Test in Airflow CI

Your CI pipeline should validate DAGs at multiple levels:

| Test Type | What It Catches | When to Run |
|-----------|-----------------|-------------|
| DAG Import Test | Syntax errors, missing modules | Every commit |
| DAG Validity Test | Cycles, missing dependencies | Every commit |
| Task Unit Tests | Individual task logic errors | Every commit |
| Integration Tests | End-to-end pipeline failures | Before merge |

### DAG Validation Strategies

**Import Testing**: Ensure all DAGs can be imported without errors.

The Airflow scheduler imports every Python file in the DAGs folder. If any file has an import error, the scheduler will fail to load all DAGs. Import testing catches these issues before deployment.

**DAG Bag Validation**: Verify DAG structure and integrity.

Airflow provides a `DagBag` class that loads and validates DAGs. You can use this in tests to ensure:
- DAGs have no import errors
- DAGs have no cycles
- Required tasks exist
- Dependencies are correctly defined

**Task Testing**: Test individual task logic in isolation.

Each Airflow operator contains logic that can be tested independently. For PythonOperator tasks, you can test the callable function directly. For SQL operators, you can validate query syntax.

### Versioning Strategies for DAGs

There are two main approaches to DAG versioning:

**Approach 1: DAG Version in Code**
- Include a version number in the DAG ID
- Allows running old and new versions simultaneously
- Useful for gradual migrations

```python
dag_id = f"customer_pipeline_v{VERSION}"
```

**Approach 2: Git-Based Versioning**
- Use Git tags or branches to mark releases
- Deploy specific Git refs to production
- Simpler but requires coordination

### Deployment Strategies

**Direct Sync**: Copy DAG files directly to the Airflow DAGs folder.
- Simple but requires file system access
- Works well for single-server setups

**Git Sync**: Airflow pulls DAGs from a Git repository.
- Used by Airflow on Kubernetes (Git-Sync sidecar)
- Production reflects the main branch automatically

**Package Deployment**: Install DAGs as a Python package.
- DAGs are versioned like any Python library
- Provides dependency management
- More complex but robust for large deployments

### Environment Promotion

DAGs should progress through environments:

```
Development -> CI/Test -> Staging -> Production
```

Each environment should have:
- Separate Airflow instances or isolated DAG folders
- Environment-specific variables and connections
- Appropriate data (production-like for staging, synthetic for CI)

## Code Example

### DAG Validation Test Script

Create a test file `tests/test_dag_validation.py`:

```python
"""
DAG validation tests for CI pipeline.
Ensures all DAGs can be loaded without errors.
"""

import os
import sys
from pathlib import Path

import pytest
from airflow.models import DagBag


# Path to your DAGs folder
DAGS_FOLDER = Path(__file__).parent.parent / "dags"


@pytest.fixture(scope="module")
def dag_bag():
    """Load all DAGs into a DagBag for testing."""
    return DagBag(
        dag_folder=str(DAGS_FOLDER),
        include_examples=False
    )


def test_no_import_errors(dag_bag):
    """Verify that all DAGs can be imported without errors."""
    assert len(dag_bag.import_errors) == 0, (
        f"DAG import errors found: {dag_bag.import_errors}"
    )


def test_dags_loaded(dag_bag):
    """Verify that at least one DAG was loaded."""
    assert len(dag_bag.dags) > 0, "No DAGs found in DAGs folder"


@pytest.mark.parametrize("dag_id", [
    "customer_pipeline",
    "product_sync",
    "daily_analytics",
])
def test_expected_dags_exist(dag_bag, dag_id):
    """Verify that expected DAGs are present."""
    assert dag_id in dag_bag.dags, f"Expected DAG '{dag_id}' not found"


def test_no_cycles(dag_bag):
    """Verify that no DAG has circular dependencies."""
    for dag_id, dag in dag_bag.dags.items():
        # This raises an exception if a cycle exists
        dag.topological_sort()


def test_dags_have_tags(dag_bag):
    """Verify that all DAGs have at least one tag for organization."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' has no tags"


def test_dags_have_owners(dag_bag):
    """Verify that all DAGs have an owner specified."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.owner != "airflow", (
            f"DAG '{dag_id}' should have a specific owner, not 'airflow'"
        )


def test_task_count_reasonable(dag_bag):
    """Verify DAGs do not have an unreasonable number of tasks."""
    max_tasks = 100
    for dag_id, dag in dag_bag.dags.items():
        task_count = len(dag.tasks)
        assert task_count <= max_tasks, (
            f"DAG '{dag_id}' has {task_count} tasks, exceeding max of {max_tasks}"
        )
```

### Unit Testing a Python Task

```python
"""
Unit tests for task functions.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

# Import the function used in your PythonOperator
from dags.customer_pipeline import process_customer_data


class TestProcessCustomerData:
    """Tests for the process_customer_data function."""

    def test_valid_customer_data(self):
        """Test processing valid customer data."""
        input_data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]
        
        result = process_customer_data(input_data)
        
        assert len(result) == 2
        assert result[0]["processed"] is True

    def test_empty_input(self):
        """Test that empty input returns empty output."""
        result = process_customer_data([])
        assert result == []

    def test_invalid_email_filtered(self):
        """Test that records with invalid emails are filtered."""
        input_data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "invalid-email"},
        ]
        
        result = process_customer_data(input_data)
        
        assert len(result) == 1
        assert result[0]["id"] == 1
```

### GitHub Actions Workflow for Airflow

Create `.github/workflows/airflow_ci.yml`:

```yaml
name: Airflow DAG CI

on:
  pull_request:
    branches:
      - main
    paths:
      - 'dags/**'
      - 'tests/**'
      - '.github/workflows/airflow_ci.yml'

jobs:
  test-dags:
    name: Validate Airflow DAGs
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install apache-airflow==2.8.0
          pip install pytest pytest-cov
          pip install -r requirements.txt

      - name: Set Airflow home
        run: |
          echo "AIRFLOW_HOME=$GITHUB_WORKSPACE" >> $GITHUB_ENV

      - name: Initialize Airflow database
        run: |
          airflow db init

      - name: Run DAG validation tests
        run: |
          pytest tests/test_dag_validation.py -v

      - name: Run task unit tests
        run: |
          pytest tests/test_tasks.py -v --cov=dags --cov-report=xml

      - name: List loaded DAGs
        run: |
          airflow dags list

      - name: Check DAG for import errors
        run: |
          python -c "
          from airflow.models import DagBag
          bag = DagBag(dag_folder='./dags', include_examples=False)
          if bag.import_errors:
              for dag, error in bag.import_errors.items():
                  print(f'Error in {dag}: {error}')
              exit(1)
          print(f'Successfully loaded {len(bag.dags)} DAGs')
          "
```

### Pre-Commit Hook for Local Validation

Add a pre-commit hook in `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: local
    hooks:
      - id: validate-dags
        name: Validate Airflow DAGs
        entry: python scripts/validate_dags.py
        language: python
        files: ^dags/.*\.py$
        additional_dependencies:
          - apache-airflow==2.8.0
```

## Summary

- **Airflow CI/CD** prevents broken DAGs from reaching production where they can halt data flows
- **DAG validation** includes import testing, structure validation, and task unit testing
- **Use DagBag** in tests to catch import errors and validate DAG structure
- **Unit test task logic** independently by testing the Python functions directly
- **Version DAGs** either through DAG IDs or Git-based tagging
- **Deploy through environments** (dev, staging, prod) with appropriate testing at each stage
- **GitHub Actions** can run Airflow tests by installing Airflow and its dependencies in CI

## Additional Resources

- [Airflow Testing Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing-a-dag) - Official Airflow testing guidance
- [Astronomer CI/CD Guide](https://docs.astronomer.io/astro/ci-cd) - Enterprise-grade Airflow CI/CD patterns
- [pytest-airflow](https://pypi.org/project/pytest-airflow/) - Pytest plugin for Airflow testing
