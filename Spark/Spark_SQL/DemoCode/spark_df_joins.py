from pyspark.sql import SparkSession
from pyspark.sql.functions import col 

# Create a Spark Session

spark = SparkSession.builder \
    .appName("Dataframe Joins in Spark") \
    .master("local[*]") \
    .getOrCreate()
    
# Lets create two related dataframes, as if we were pulling data from a SQL
# database

# TWo tables - Employees and Department 
# Each employee has ONE department
# ONE department has (potentially) multiple employees 1->M (or M->1)

employees = spark.createDataFrame([
    (1, "Alice", 101), # EmployeeId, Name, DepartmentId
    (2, "Bob", 102),
    (3, "Charlie", 102), 
    (4, "Diana", None) # No department fk  
], ["emp_id", "emp_name", "dept_id"]) # createDataFrame(data, [columns]) 

departments = spark.createDataFrame([
    (101, "Engineering", "Building A"), # Department ID, Department name, Location
    (102, "Marketing", "Building C"),
    (104, "Sales", "Building B")
], ["dept_id", "dept_name", "location"])

employees.show()
departments.show()

# SparkSQL provides for SQL like querying without having to use actual SQL 
# This can make working with multiple data frames really easy, no need to set up
# data pipelines the RDD way where you chain function calls together

# It supports all the SQL joins we're used to

# Inner join 

inner_result = employees.join(
    departments, # the dataframe we're joining
    employees.dept_id == departments.dept_id, # the column we're joining on
    "inner" # What kind of join? Inner, left, right, etc. 
).select(employees["*"], departments.dept_name) # Selecting everything from employees who have a match

# Alternative syntax if your column names line up and you just want a faster inner join
alt_inner_result = employees.join(departments,'dept_id') \
    .select(employees["*"], departments.dept_name).explain(extended=True)

alt_inner_result.show()

inner_result.show()