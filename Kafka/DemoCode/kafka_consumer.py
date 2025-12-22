# This is going to act as a simple consumer
from kafka import KafkaConsumer
import json

# Creating our consumer object
consumer = KafkaConsumer(
    'python-test', # what topic(s) are we consuming from
    bootstrap_servers=['localhost:9092'], # what server?
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) # deserializing
    # our json values 
)

print("Consuming messages:")

# Printing our messages to the console 
for message in consumer:
    print(f'Recieved: {message.value}')