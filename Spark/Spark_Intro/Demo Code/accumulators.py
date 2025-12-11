from pyspark import SparkContext

sc = SparkContext("local[*]", "AccumulatorsDemo")

# Lets create some accumulators
# We will have some intentionally bad data, and we want a count
# Of both total records processed, and how many of those are invalid

# They are created like other python pyspark objects
total_records = sc.accumulator(0)
error_count = sc.accumulator(0)

# Sample data with bad records
data = [
    "valid",
    "valid",
    "bad",
    "invalid",
    "valid"
]

rdd = sc.parallelize(data) # Turn the data into an rdd

# We want to be able to give a map some more custom logic
# All a map does is apply a function to everything in a collection (rdd)

# Since this function will be passed into a map 
# And map runs on worker nodes (inside an executor)
# ... we can't create Accumulators within process_record()
# When working in PySpark we need to be mindful not just of python scoping
# But Driver vs Worker scoping

def process_record(record):
    # Whether a record is good or bad, we tried. So we increment
    # total_records
    total_records.add(1) 
    
    if (record != "valid"):
        # If the record isn't valid we increment error_count
        error_count.add(1)
        #If the record isn't valid, we simply return None
        return None
    
    return record # If the record is valid, we never hit out if-block
    
# Now that we wrote our filtering function we can use it in a transformation

# When doing transformations, we can chain them.
# Each transformation in the chain runs sequentially.
# We run process_record() for EVERY record in our initial RDD

# we start with our initial rdd... 
# ["valid", "valid", "bad", "invalid", "valid"]
# we run map(process_record).... and we get 
# ["valid", "valid", None, None, "valid"]
# then we run filter(lambda x: x is not None) on the above
# inside of results... we get
# ["valid", "valid", "valid"]


results = rdd.map(process_record).filter(lambda x: x is not None)

# Now we can finally trigger an action
valid_values = results.collect()
print(valid_values)


print(total_records)
print(error_count)
    