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

# Define a class to manage Kafka consumer operations
class KafkaConsumerManager:
    def __init__(self, topic, mqtt_manager, inactivity_timeout=60, group_id='my_consumer_group'):
        self.topic = topic
        self.active = True
        self.last_message_time = time.time()
        self.inactivity_timeout = inactivity_timeout
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['127.0.0.1:9092'],
            auto_offset_reset='earliest',  #earliest offset so the consumer start from the begging value
            enable_auto_commit=False,  # Disable auto commit 
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.mqtt_manager = mqtt_manager
        self.temperatures = {}  # Dictionary used for saving the temperature extracted for each productor
        self.temperature_index = 0 

    #Start the kafka consumer
    def start_consumer(self):
        logger.info(f"Starting Kafka consumer for topic: {self.topic}")
        while self.active:
            messages = self.consumer.poll(timeout_ms=1000)  #
            if not messages:  # If no messagges, exit the consumer
                logger.info("No more messages to consume. Exiting Kafka consumer.")
                break
            for message in messages.values():
                for msg in message:
                    self.process_message(msg)
                    self.consumer.commit()  # Manual commit after message processing
            self.check_activity()

    #Process the message received from the kafka consumer
    def process_message(self, message):
        self.last_message_time = time.time()
        logger.info(f"Received message: {message.value}")
        try:
            temperature = message.value["temperature"]
            logger.info(f"Temperature read from Kafka: {temperature}")
            self.temperatures[self.temperature_index] = temperature  # Aggiungi la temperatura al dizionario
            self.temperature_index += 1 
        except KeyError:
            logger.warning("No temperature information found in Kafka message")

    #Check for consumer inactivity and stop if timeout is reached
    def check_activity(self):
        current_time = time.time()
        time_since_last_message = current_time - self.last_message_time
        if time_since_last_message >= self.inactivity_timeout:
            logger.info("Kafka consumer finished due to inactivity timeout.")
            self.active = False
        else:
            logger.info(f"Time since last message: {time_since_last_message} seconds")

    #Get all the temperature collected    
    def get_all_temperatures(self):
        return list(self.temperatures.values()) 