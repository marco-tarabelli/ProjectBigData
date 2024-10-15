
from kafka import KafkaConsumer
import json

import paho.mqtt.client as mqtt

# KafkaTemperatureConsumer class: consumes temperature data from a Kafka topic
class KafkaTemperatureConsumer:
    # Constructor: initializes the Kafka consumer
    def __init__(self, topic, bootstrap_servers, group_id):
        # Initialize the Kafka consumer
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            request_timeout_ms=30000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Leggendo dati da topic Kafka: {topic}")
    # Method: reads temperature data from the Kafka topic
    def read_temperature(self):
        # Read temperature data from the Kafka topic
        for message in self.consumer:
            temperature = message.value['temperature']
            self.consumer.commit()
            return temperature
            

    # Method: closes the Kafka consumer connection
    def close(self):
        # Close the Kafka consumer connection
        self.consumer.close()