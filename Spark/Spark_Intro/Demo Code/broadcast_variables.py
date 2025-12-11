from pyspark import SparkContext

sc = SparkContext("local[*]", "BroadcastDemo")

country_lookup = {
    "US": "United States",
    "UK": "United Kingdom",
    "DE": "Germany",
    "FR": "France",
    "JP": "Japan"
}

# First, we'll do this without a braodcast variable

# Some country codes in an RDD - but just the abbreviated codes
data_rdd = sc.parallelize(["US", "UK", "DE", "US", "FR", "JP", "MN"])

# Creating a method to lookup country codes - no broadcast
def lookup_country_bad(code):
    
    # Dictionary lookup - straight python 
    return country_lookup.get(code, "Unknown")

    # For every single record we run through in this RDD
    # The node (or nodes) have to have a copy of the lookup dictionary
    # sent to them over the network. Every. Single. Time.
    # If the data rdd is 100,000 items long, thats 100,000 transfers
    # of the same lookup dictionary for no reason.
    
# With a broadcast variable 
# Using a broadcast variable we let the Driver node know to send this data
# to every executor once. And then instruct the executor to cache it locally
# until its done with the transformation

# Lets create a broadcast variable
bc_country_lookup = sc.broadcast(country_lookup) 

def country_lookup_good(code):
    return bc_country_lookup.value.get(code, "Unknown") # Using our broadcast variable

results = data_rdd.map(country_lookup_good).collect()


# We want to get in the habit of cleaning up our broadcast variables whenever we use them
# Otherwise the executors will keep these cached on the worker node's memory

bc_country_lookup.destroy() # tells executors to dereference this variable to free worker node memory

print(results)