import time
import json
import random
from kafka import KafkaProducer

# Configuration
topic = "sensor-data"

# Initialize Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_sensor_data():
    # Generates mock sensor data
    return {
        "sensor_id": random.choice(["s_01", "s_02", "s_03", "s_04"]),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 60.0), 2),
        "timestamp": int(time.time())
    }

try:
    while True:
        data = generate_sensor_data()
        # Send data to Kafka
        producer.send(topic, value=data)
        print(f"Sent: {data}")
        time.sleep(1) # Simulate 1 event per second
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.close()