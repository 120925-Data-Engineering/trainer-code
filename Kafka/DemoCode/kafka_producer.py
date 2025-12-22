# After having pip-installed kafka
# pip install kafka
from kafka import KafkaProducer
import json 
import time

# Lets create a producer
# For demo purposes, we're just going to use a python loop to create 
# some messages - remember, we're not in spark anymore (for now)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], #In our case, we just have that 9092 broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # Telling our producer to just serialize
                                                            # messages as json
)

# Name of our topic, just for convenient re-use later
topic = 'python-test'

# For demo purposes, we're going to just create some mock messages using a for loop
for i in range(1000):
    message = { 'id': i, 'value': f'Message {i} from my kafka_producer.py'} # Created a message
    producer.send(topic, message) # Sent the message to Kafka 
    print(f'Sent: {message}') # Printing to our console for debug - if we want
    time.sleep(1)

# These two almost always go hand in hand 
producer.flush() # Holds at line 27 until it's transmitted all of our messages 
producer.close() # Closes the connection to kafka
