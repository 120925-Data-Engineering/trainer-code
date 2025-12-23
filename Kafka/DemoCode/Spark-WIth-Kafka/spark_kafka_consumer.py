# This will be our Spark consumer, using pyspark.sql 
# We'll see a good real world-ish usecase for a dataframe schema

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Lets create our spark session - same as before when we were running locally
spark = SparkSession.builder \
    .appName("Kafka Spark Integration") \
    .master("local[*]") \
    .getOrCreate()
    
# For our sanity, lets msee if we can cut down on console clutter
spark.sparkContext.setLogLevel("WARN") # This just makes it so INFO logs dont get written 

# We need to define a schema so that pyspark knows how to handle our incoming data
# By default - Kafka sends messages in a topic as a "stream" - "101000101101010101010001"
# PySpark needs us to define the structure of the data so we can meaningfully work with it
# in a dataframe

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# Now that we have our schema, we want to read the stream from Kafka
# This is going to take two steps, atleast for demo 
# NOTE: "kafka.bootstrap.servers" points to your locally running broker
raw_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", 'sensor-data') \
    .option("startingOffsets", "latest") \
    .load() # Once you've set your options, call .load()
    
# By default, Kafka stores data in 'key' and 'value' binary columns 
# We need to cast this thing as a string, then parse the JSON

# For fun, lets use sql expressions in pyspark

'''
processed_df = raw_stream_df.selectExpr("CAST(value AS STRING)") \
    .selectExpr("value as json_string") \
    .selectExpr("CAST(json_string as STRING)")
'''

processed_df = raw_stream_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") # This flattens the struct that the above select creates

# Without flattening, we would always find ourselves having to unpack a nested object
# to work with out data frames. Versus, having "sensor_id, "temperature", etc
# be our top level columns
# data - value
# 1 - {{'sensor_id': 's_03', 'temperature': 26.62, 'humidity': 47.23, 'timestamp': 1766505398},
# {'sensor_id': 's_03', 'temperature': 28.44, 'humidity': 36.79, 'timestamp': 1766505399}}

query_df = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start() # This write is not like our csv write, it will continuously output
    # We can use it to write back to a new kafka topic, or in our case, to the console

print("Streaming started, reading from Kafka: sensor_data topic")

query_df.awaitTermination() # If we dont do this, we probably won't see anything
# This is because, the writeStream.start() is asynchronous - it doesn't block