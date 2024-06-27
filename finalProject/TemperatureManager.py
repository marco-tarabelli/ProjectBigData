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

# Define a class to manage temperature generation and publication
class TemperatureManager:
    def __init__(self, mqtt_manager, mqtt_config, kafka_consumer_manager=None, config=None):
        self.mqtt_manager = mqtt_manager
        self.mqtt_config = mqtt_config
        self.kafka_consumer_manager = kafka_consumer_manager
        self.config = config
#Generate a random temperature and publish it on MQTT broker in a specific topic
    def generate_and_publish_temperature(producer_id, mqtt_manager, mqtt_config):
        temperature = random.uniform(10.1, 25.5)
        logger.info(f"Generated random temperature for {producer_id}: {temperature}")

        sensor_info = yaml.dump({"data": {"type": "temperature_sensor"}}, default_flow_style=True)
        message = f"Temperature for {producer_id}: {temperature}\nSensor Info:\n{sensor_info}"

        topic = mqtt_config["topic"].replace("${PRODUCER_ID}", producer_id)
        mqtt_manager.publish_message(topic, message)
        logger.info(f"Published MQTT message for {producer_id}")

#Handle temperatures from kafka and publish it on MQTT broker in a specific topic
    def handle_temperatures_and_publish_messages(kafka_consumer_manager, mqtt_manager, mqtt_config, config):
        all_temperatures = kafka_consumer_manager.get_all_temperatures()
        for index, temperature in enumerate(all_temperatures):
            logger.info(f"Temperature {index + 1}: {temperature}")
            for producer in config["producers"]:
                producer_id = producer["id"]
                producer_type = producer.get("data", {}).get("type")
                if producer_type == "docker sensor":
                    logger.info(f" {producer_id} has temperature: {temperature}")
                    sensor_info = yaml.dump(producer.get("data", {}), default_flow_style=True)
                    message = f"Temperature for {producer_id}: {temperature}\nSensor Info:\n{sensor_info}"

                    topic = mqtt_config["topic"].replace("${PRODUCER_ID}", producer_id)
                    mqtt_manager.publish_message(topic, message)
                    logger.info(f"Published MQTT message for {producer_id}")
