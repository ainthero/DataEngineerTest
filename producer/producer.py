import os
import time
import csv
import json
import logging
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "input_topic")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    api_version=(0, 11, 5),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

delay = 1
message_count = 0

def convert_float_types(row):
    for key, value in row.items():
        try:
            row[key] = float(value)
        except ValueError:
            pass
    return row


with open('data/input_data.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        converted_row = convert_float_types(row)
        
        producer.send(TOPIC_NAME, converted_row)
        message_count += 1

        if message_count % 10 == 0:
            producer.flush()
            logging.info(f"Sent {message_count} messages to Kafka topic '{TOPIC_NAME}'.")
            time.sleep(delay)

producer.flush()
producer.close()
