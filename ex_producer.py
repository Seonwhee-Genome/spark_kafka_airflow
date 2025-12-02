# file: ex_producer.py
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    data = {'event_time': time.time(), 'value': random.randint(0, 100)}
    producer.send('TestTopic1', value=data)
    time.sleep(1)

