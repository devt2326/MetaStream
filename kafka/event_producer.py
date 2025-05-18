# MetaStream/kafka/event_producer.py
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

event_types = ['post', 'like', 'comment', 'follow', 'share']
users = list(range(1001, 1020))

def generate_event():
    return {
        "user_id": random.choice(users),
        "event_type": random.choice(event_types),
        "target_id": random.randint(2000, 3000),
        "timestamp": datetime.utcnow().isoformat(),
        "device": random.choice(['iOS', 'Android', 'Web']),
        "location": random.choice(['NY', 'SF', 'London', 'Mumbai'])
    }

while True:
    event = generate_event()
    producer.send('social_events', value=event)
    print(f"Produced: {event}")
    time.sleep(1)  # simulate 1 event/sec
