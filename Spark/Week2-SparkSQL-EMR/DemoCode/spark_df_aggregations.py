from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, min, max, 
    countDistinct, collect_list, collect_set,
    round as spark_round, first, last
)

spark = SparkSession.builder \
    .appName("Demo: Aggregations") \
    .master("local[*]") \
    .getOrCreate()

# Sample sales data
sales_data = [
    ("2023-01", "Electronics", "Laptop", 1200, "NY"),
    ("2023-01", "Electronics", "Phone", 800, "NY"),
    ("2023-01", "Electronics", "Tablet", 500, "CA"),
    ("2023-01", "Clothing", "Jacket", 150, "NY"),
    ("2023-01", "Clothing", "Shoes", 100, "CA"),
    ("2023-02", "Electronics", "Laptop", 1300, "TX"),
    ("2023-02", "Electronics", "Phone", 850, "NY"),
    ("2023-02", "Clothing", "Jacket", 175, "CA"),
    ("2023-02", "Clothing", "Pants", 80, "TX")
]

df = spark.createDataFrame(
    sales_data, 
    ["month", "category", "product", "amount", "state"]
)

df.show()

# Now that we're away from RDDs... we can use SQL like aggregations
# There are ALOT of them. Over 150 different ways to aggregate built into 
# Spark SQL. 

# Lets look a a single aggregation GroupBy
# Group everything by its category column, and return the count for each
# distinct category
df.groupBy("category").count().show()

# Lets get back total sales (sum) per category
df.groupBy("category").sum("amount").show()

# Lets try the average sales by month 
df.groupBy("month").avg("amount").show()

# We can use .agg() to do multiple aggregations. Using .agg()
# we can alias columns on the return dataframe. 
# Remember, whenever we do dataframe operations, we are returning
# a new dataframe we can then store if we need to. Its more than
# a temporary view like in SQL

# Lets get back a short summary of info for each category of data
# count, sum, avg (rounded), min and max - all at once

category_overviews = df.groupBy("category").agg(
    count("*").alias("num_sales"),
    sum("amount").alias("total_revenue"),
    spark_round(avg("amount"), 2).alias("avg_sales"),
    min("amount").alias("min_sale"),
    max("amount").alias("max_sale")
)

category_overviews.show()

# Grouping by more than one column... like we can in SQL
df.groupBy("month", "category").agg(
    count("*").alias("num_sales"),
    sum("amount").alias("total_revenue")
).orderBy(["month", "category"], ascending=[True, False]).show()