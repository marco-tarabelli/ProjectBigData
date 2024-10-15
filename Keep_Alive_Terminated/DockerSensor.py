import time

from kafka import KafkaConsumer
import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod
from ISensor import ISensor


# DockerSensor class: simulates a Docker-based sensor
class DockerSensor(ISensor):
    # Constructor: initializes the sensor's properties
    
    def __init__(self, producer_id, docker_manager, kafka_consumer, mqtt_manager, config, keep_alive_ms):
        self.producer_id = producer_id
        self.docker_manager = docker_manager
        self.kafka_consumer = kafka_consumer
        self.mqtt_manager = mqtt_manager
        self.config = config
        self.name = config['name']
        self.fw_version = config['fw_version']
        self.status = config['status']
        self.keep_alive_ms = keep_alive_ms

    # Method: runs the iterations for the Docker sensor
    def run(self, iterations, description):
        i = 0
        self.docker_manager.start_container()
        start_time = time.time()
        container_running = True

        while True if iterations == float('inf') else i < iterations:
            i += 1
            current_time = time.time()
            print(f"Starting iteration {i} for Docker sensor: {self.producer_id}...")

            if container_running and current_time - start_time >= self.keep_alive_ms:
                print(f"Keep-alive period ended for Docker sensor: {self.producer_id}. Stopping container.")
                self.docker_manager.stop_container()
                container_running = False

            temperature = self.read_data()
            if temperature is not None:
                self.mqtt_manager.publish(self.producer_id, temperature, self.name, self.fw_version, self.status, description)
                print(f"Finished iteration {i} for Docker sensor: {self.producer_id}...")
            else:
                print("No more messages for docker sensor")

            time.sleep(1)  # Delay to simulate processing time

    def read_data(self):
        try:
            temperature = self.kafka_consumer.read_temperature()
            return temperature
        except Exception as e:
            print(f"Error reading temperature from Kafka: {e}")
            return None