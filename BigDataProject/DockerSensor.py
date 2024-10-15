import time

from kafka import KafkaConsumer

import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod
from ISensor import ISensor

# DockerSensor class: simulates a Docker-based sensor
class DockerSensor(ISensor):
    # Constructor: initializes the sensor's properties
    def __init__(self, producer_id, docker_manager, kafka_consumer, mqtt_manager, config):
        # Initialize the sensor's properties
        self.producer_id = producer_id
        self.docker_manager = docker_manager
        self.kafka_consumer = kafka_consumer
        self.mqtt_manager = mqtt_manager
        self.config = config
        self.name = config['name']
        self.fw_version = config['fw_version']
        self.status = config['status']

    # Method: runs the iterations for the Docker sensor
    def run(self, iterations):
        # Run the iterations for the Docker sensor
        i = 0
        while True if iterations == float('inf') else i < iterations:
            i += 1
            start_time = time.time()
            print(f"Starting iteration {i} for Docker sensor: {self.producer_id}...")
            self.docker_manager.restart_container()
            time.sleep(5)  # Wait for container to generate data
            temperature = self.read_data()
            self.mqtt_manager.publish(self.producer_id, temperature, self.name, self.fw_version, self.status)
            print(f"Finished iteration {i} for Docker sensor: {self.producer_id}...")
            end_time = time.time()
            duration = end_time - start_time
            print(f"Iteration {i} for Docker sensor: {self.producer_id} took {duration:.2f} seconds")
            time.sleep(2)  # Delay to simulate processing time

    # Method: reads data from the Kafka topic
    def read_data(self):
        # Read data from the Kafka topic
        try:
            temperature = self.kafka_consumer.read_temperature()
            return temperature
        except Exception as e:
            print(f"Error reading temperature from Kafka: {e}")
            return None