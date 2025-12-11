
# We want to make sure that we can connect to spark from our code

# We need to import some stuff
from pyspark.sql import SparkSession
import sys

# So far we've been using SparkContext - here we will use a SparkSession
# SparkSession is a unified entry point that was created with Spark 2.0
print("Test")

# Creating my spark session
# MasterURL - points to a remote cluser OR your local machine
# Using slashes to avoid really long single lines here
spark = SparkSession.builder.appName("SparkConnectionDemo") \
    .master("local[*]")  \
    .getOrCreate()
    
# Now that we have our session, we can ask for the SparkContext within it
# If we just wanna do RDD stuff

# We don't create a new context, we ask our SparkSession object for it's context
sc = spark.sparkContext

data = [1, 2, 3, 4, 5, 6, 7]
test_rdd = sc.parallelize(data) # Turn our dummy data into an RDD

# Basic transformations (lazy)
squared_rdd = test_rdd.map(lambda x: x ** 2) # Squaring original numbers
evens_rdd = squared_rdd.filter(lambda x: x % 2 == 0) # Filtering for evens in the squared rdd

# Action - triggers actual computation 
result = evens_rdd.collect()

print(result)