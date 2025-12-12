# Using DataFrame Spark SQL

## Learning Objectives
- Learn how to run SQL queries directly on DataFrames
- Master creating and managing temporary views
- Understand when to use SQL vs. DataFrame API
- Combine SQL and DataFrame operations effectively

## Why This Matters

Spark SQL allows you to use familiar SQL syntax on DataFrames. This is powerful because:
- SQL is a universal language known by data professionals
- Complex queries may be more readable in SQL
- Existing SQL queries can be migrated easily
- Teams can collaborate across skill levels

This topic completes Thursday's performance-focused material by showing how to leverage SQL alongside the DataFrame API in our Weekly Epic.

## The Concept

### Registering DataFrames as Views

Before running SQL, you must register your DataFrame as a view:

| Method | Scope | Lifetime |
|--------|-------|----------|
| `createOrReplaceTempView()` | SparkSession | Until explicitly dropped or session ends |
| `createTempView()` | SparkSession | Error if view exists |
| `createOrReplaceGlobalTempView()` | Across sessions | Until cluster shutdown |
| `createGlobalTempView()` | Across sessions | Error if view exists |

### Using spark.sql()

Once registered, query with `spark.sql()`:

```python
df.createOrReplaceTempView("my_table")
result = spark.sql("SELECT * FROM my_table WHERE age > 30")
```

The result is a DataFrame you can continue to manipulate.

### SQL vs. DataFrame API

| Aspect | SQL | DataFrame API |
|--------|-----|---------------|
| Readability | Familiar to SQL users | More Pythonic |
| Type Safety | Runtime errors | Some IDE support |
| Complexity | Good for complex queries | Good for simple transformations |
| Composition | String building | Method chaining |
| Testing | Harder to unit test | Easier to test |

### When to Use Each

**Use SQL when:**
- Query logic is complex (multiple joins, subqueries, CTEs)
- Migrating existing SQL queries
- Team is SQL-focused
- Using SQL-specific features (window functions, CTEs)

**Use DataFrame API when:**
- Building dynamic transformations
- Composing reusable functions
- Unit testing transformations
- IDE autocompletion is valuable

## Code Example

### Basic SQL Queries

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark SQL").getOrCreate()

# Create DataFrame
data = [
    (1, "Alice", "Engineering", 75000),
    (2, "Bob", "Marketing", 65000),
    (3, "Charlie", "Engineering", 80000),
    (4, "Diana", "Sales", 55000),
    (5, "Eve", "Marketing", 70000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

# Register as temporary view
df.createOrReplaceTempView("employees")

# Run SQL queries
print("All employees:")
spark.sql("SELECT * FROM employees").show()

print("Engineers only:")
spark.sql("""
    SELECT name, salary 
    FROM employees 
    WHERE department = 'Engineering'
""").show()

print("Average salary by department:")
spark.sql("""
    SELECT department, 
           AVG(salary) as avg_salary,
           COUNT(*) as count
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""").show()
```

### Advanced SQL Features

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Advanced SQL").getOrCreate()

# Sample data
employees = spark.createDataFrame([
    (1, "Alice", 1, 75000),
    (2, "Bob", 2, 65000),
    (3, "Charlie", 1, 80000),
    (4, "Diana", 3, 55000)
], ["id", "name", "dept_id", "salary"])

departments = spark.createDataFrame([
    (1, "Engineering"),
    (2, "Marketing"),
    (3, "Sales")
], ["dept_id", "dept_name"])

employees.createOrReplaceTempView("employees")
departments.createOrReplaceTempView("departments")

# JOIN
print("JOIN:")
spark.sql("""
    SELECT e.name, d.dept_name, e.salary
    FROM employees e
    JOIN departments d ON e.dept_id = d.dept_id
""").show()

# Subquery
print("Subquery (employees earning above average):")
spark.sql("""
    SELECT name, salary
    FROM employees
    WHERE salary > (SELECT AVG(salary) FROM employees)
""").show()

# Window function
print("Window function (rank by salary):")
spark.sql("""
    SELECT name, dept_id, salary,
           RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rank
    FROM employees
""").show()

# Common Table Expression (CTE)
print("CTE:")
spark.sql("""
    WITH dept_stats AS (
        SELECT dept_id, 
               AVG(salary) as avg_salary,
               MAX(salary) as max_salary
        FROM employees
        GROUP BY dept_id
    )
    SELECT e.name, e.salary, ds.avg_salary
    FROM employees e
    JOIN dept_stats ds ON e.dept_id = ds.dept_id
    WHERE e.salary > ds.avg_salary
""").show()
```

### Mixing SQL and DataFrame API

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder.appName("Mixed Approach").getOrCreate()

data = [
    ("Alice", 34, 75000),
    ("Bob", 45, 65000),
    ("Charlie", 29, 80000)
]

df = spark.createDataFrame(data, ["name", "age", "salary"])
df.createOrReplaceTempView("people")

# Start with SQL for complex aggregation
sql_result = spark.sql("""
    SELECT 
        name,
        salary,
        salary - (SELECT AVG(salary) FROM people) as vs_avg
    FROM people
""")

# Continue with DataFrame API for transformations
final_result = sql_result \
    .withColumn("name_upper", upper(col("name"))) \
    .filter(col("vs_avg") > 0)

print("Combined approach:")
final_result.show()

# Or vice versa: DataFrame to SQL
df_transformed = df.withColumn("bonus", col("salary") * 0.1)
df_transformed.createOrReplaceTempView("people_with_bonus")

spark.sql("""
    SELECT name, salary, bonus, salary + bonus as total
    FROM people_with_bonus
    ORDER BY total DESC
""").show()
```

### Global Temporary Views

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Global Views").getOrCreate()

data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Create global temp view (accessible across sessions)
df.createOrReplaceGlobalTempView("global_people")

# Must use global_temp database prefix
spark.sql("SELECT * FROM global_temp.global_people").show()

# Local temp view (current session only)
df.createOrReplaceTempView("local_people")
spark.sql("SELECT * FROM local_people").show()

# List all tables/views
print("Views in catalog:")
for table in spark.catalog.listTables():
    print(f"  {table.name} (database: {table.database}, isTemporary: {table.isTemporary})")
```

### Dynamic SQL with Parameters

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Dynamic SQL").getOrCreate()

data = [
    ("Alice", "Engineering", 75000),
    ("Bob", "Marketing", 65000),
    ("Charlie", "Engineering", 80000)
]

df = spark.createDataFrame(data, ["name", "department", "salary"])
df.createOrReplaceTempView("employees")

# Method 1: f-string (be careful with SQL injection!)
def filter_by_department(dept_name):
    # Only use this with trusted inputs
    query = f"""
        SELECT * FROM employees 
        WHERE department = '{dept_name}'
    """
    return spark.sql(query)

print("Engineering team:")
filter_by_department("Engineering").show()

# Method 2: Use DataFrame API for dynamic parts
def filter_by_department_safe(df, dept_name):
    return df.filter(col("department") == dept_name)

# Method 3: Create parameterized view
from pyspark.sql.functions import lit

threshold = 70000
df.filter(col("salary") > lit(threshold)).createOrReplaceTempView("filtered")
spark.sql("SELECT * FROM filtered").show()
```

### Managing Views

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("View Management").getOrCreate()

data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Create view
df.createOrReplaceTempView("my_view")

# Check if view exists
print("Tables/Views:", [t.name for t in spark.catalog.listTables()])

# Drop view
spark.catalog.dropTempView("my_view")
print("After drop:", [t.name for t in spark.catalog.listTables()])

# Clear all cached tables
spark.catalog.clearCache()

# List columns in a view
df.createOrReplaceTempView("my_view")
print("Columns:", spark.catalog.listColumns("my_view"))
```

## Summary

- **createOrReplaceTempView()** registers a DataFrame as a queryable SQL view
- **spark.sql()** executes SQL queries and returns DataFrames
- SQL excels at complex queries with joins, subqueries, and window functions
- DataFrame API excels at dynamic transformations and testable code
- Combine both: use SQL for complex queries, DataFrame API for transformations
- **Global temp views** are accessible across SparkSessions via `global_temp.view_name`
- Always consider SQL injection risks when building dynamic SQL strings

## Additional Resources

- [Spark SQL Getting Started](https://spark.apache.org/docs/latest/sql-getting-started.html)
- [SQL Reference](https://spark.apache.org/docs/latest/sql-ref.html)
- [Views and Tables](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-view.html)
