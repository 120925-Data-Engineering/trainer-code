from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Spark Caching") \
    .master("local[*]") \
    .getOrCreate()

# With caching, what we're trying to do is save on data reads across
# our cluster. We have to be mindful, because caching takes time. 
# We should only cache when it benefits us, caching everything all the time
# will slow us down for no gain. 

# No caching - 3 reads, recomputing the dataframe each time
# df = spark.read.csv("data_file.csv") 
# df.count() # Reads the data, counts (remember, spark is lazy unlike Pandas)
# df.show(N) # reads the data AGAIN, shows N number of lines
# df.write.csv("new_file.csv") # Read it AGAIN, and write it to a file

# With caching
# df = spark.read.csv("data_file.csv").cache()
# df.count() # Reads the data, caches, then counts
# df.show(N) # Reads from the cache, then shows (no network traffic)
# df.write.csv("new_file.csv") # Read from the cache, then write (no traffic)

# .cache() is a shorthand, what it's actually doing is calling .persist()
# and using the default storage level, MEMORY_AND_DISK_DESER
# We can use .persist() to get granular control over our cacheing behavior
# Things like, only on the disk, only on memory, serialize before caching - etc

# First lets read from a CSV
# Like anything else in Pyspark there's an incredibly simple way 
# to read a csv file... followed by incrementaly more complex ways
# if you need more granular control

# At it's most basic, to read csv, this is all you need
# sales_data_df = spark.read.csv("./Data/sales_data.csv")
sales_data_df = spark.read.csv("./Data/sales_data.csv", \
    inferSchema=True, header=True) # We can give it more options to customize behavior

# Reading from a file is done in the laziest possible way.
# Pyspark opens the file, peeks to see the first line, and uses that to 
# grab column names, and infer the schema (data types of columns) 
# if we ask it to do that. No other data is read at this time. 
print(sales_data_df)

# While we're here, we can write to a json.
# sales_data_df.write.json("./Data/sales_data.json")

# Because this is pyspark, the above code writes each partition as its own .json
# and sticks it into a directory for us. If we want everything in one file, we have to
# coalesce
sales_data_df.coalesce(1).write.mode("overwrite").json("./Data/sales_data")


# Lets detour back to caching 
# If we know before we run any other actions, we will want to cache this dataframe
# we can just call .cache on our dataframe object
sales_data_df.cache() # note: this is also lazy - nothing is cached YET

sales_data_df.count() # This triggers our cache

# This operation below, is called on cached data
east_sales_data_df = sales_data_df.filter(sales_data_df["region"] == "East")

# east_sales_data_df.explain()

print("\n\n\n")
print(sales_data_df.rdd.getNumPartitions())
