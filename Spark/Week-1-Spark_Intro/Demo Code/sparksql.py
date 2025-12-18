from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .appName("DataFrame-Basics") \
    .master("local[*]") \
    .getOrCreate()

# Before we worry about pulling in any data, lets just create some demo data

# With RDDs, our data was unstructured - but we had special methods available
# for key:value Tuples. With DataFrames, we can just use normal tuples to create
# rows of data

data = [ # EmpId, Name, Department, Salary
    (1, "Alice", "Engineering", 80000),
    (2, "Bob", "Marketing", 60000),
    (3, "Charlie", "Engineering", 90000),
    (4, "Eve", "Sales", 40000)
]

# Creating the dataframe in Spark, using the SparkSession object
# ... looks alot like Pandas! 
emp_df = spark.createDataFrame(data, ["id", "name", "dept", "salary"])

emp_df.show()

# We can also create data frames with an explicit, type checked schema

# We construct a schema of StructType, which is a list of StructFields
# Each StructField contains name, type, isNullable (t or f)
emp_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True)
])

data_2 = [ # EmpId, Name, Department, Salary
    (1, "Alice", "Engineering", 80000.0),
    (2, "Bob", "Marketing", 60000.0),
    (3, "Charlie", "Engineering", 90000.0),
    (4, "Eve", "Sales", 40000.0)
]

emp_struct_df = spark.createDataFrame(data_2, emp_schema) # creating a dataframe with our schema

emp_struct_df.show()

# Just like Pandas, we can do things like select columns, add or modify columns... its basically 1:1
emp_df.select("id", "name").show()

# We can add a colimn...

# Lets give everyone a 10% bonus... with some really basic "feature engineering"
emp_df = emp_df.withColumn("bonus", emp_df["salary"] * 0.10)

emp_df.show()

# Pandas syntax for filtering off a column and its values
emp_df[emp_df["salary"] >= 50000.0].show()

# There is also a "PySpark way" to do this... with more functions
emp_df.filter(col("salary") >= 50000.0).show()

# Use whatever you prefer! 