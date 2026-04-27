import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'sensor_data'

print("Starting data generation...")
try:
    while True:
        data = {
            "id": random.randint(1, 5),
            "temperature": random.uniform(10, 60),
            "humidity": random.uniform(30, 90),
            "timestamp": datetime.now().isoformat()
        }
        producer.send(topic_name, data)
        print(f"Sent: {data}")
        time.sleep(1)
except KeyboardInterrupt:
    producer.close()