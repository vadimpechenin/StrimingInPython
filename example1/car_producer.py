import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

car_types = ['sedan', 'truck', 'bus', 'motorcycle', 'suv']

while True:
    car = {
        'type': random.choice(car_types),
        'timestamp': time.time()
    }
    producer.send('car_stream', value=car)
    time.sleep(random.uniform(0.1, 1))  # случайный интервал
