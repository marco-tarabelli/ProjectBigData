
from kafka import KafkaConsumer
import json

import paho.mqtt.client as mqtt


# MqttManager class: manages MQTT connections and publishes data
class MqttManager:
    # Constructor: initializes the MQTT client
    def __init__(self, host, port, topic_format):
        # Initialize the MQTT client
        self.client = mqtt.Client()
        self.client.connect(host, port, 60)
        self.topic_format = topic_format

    # Method: publishes data to an MQTT topic
    def publish(self, producer_id, temperature, name, fw_version, status, description):
        topic = self.topic_format.replace("${PRODUCER_ID}", producer_id)
        message = json.dumps({
            "producer_id": producer_id,
            "temperature": temperature,
            "name": name,
            "fw_version": fw_version,
            "status": status,
            "description": description
        })
        self.client.publish(topic, message)
        print(f"Published temperature {temperature}°C to topic {topic}")