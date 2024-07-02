import json
import logging
import threading
import time
import docker
from kafka import KafkaConsumer
import yaml
import paho.mqtt.client as mqtt
import random


#Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define a class to manage MQTT connections and messaging
class MQTTManager:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = mqtt.Client()
    #Connect to the MQTT broker
    def connect(self):
        self.client.connect(self.host, self.port)
    #Publish a message to the MQTT broker in a specific topic
    def publish_message(self, topic, message):
        self.client.publish(topic, message)
    #Disconnect from the MQTT broker
    def disconnect(self):
        self.client.disconnect()