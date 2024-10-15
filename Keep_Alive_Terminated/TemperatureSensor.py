import time

from kafka import KafkaConsumer

import random

import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod
from ISensor import ISensor

class TemperatureSensor(ISensor):
    # Constructor: initializes the sensor's properties
    def __init__(self, producer_id, min_temp, max_temp, mqtt_manager, config):
        self.producer_id = producer_id
        self.min_temp = min_temp
        self.max_temp = max_temp
        self.mqtt_manager = mqtt_manager
        self.config = config
        self.name = config['name']
        self.fw_version = config['fw_version']
        self.status = config['status']

    # Method: runs the iterations for the temperature sensor
    def run(self, iterations, description):
        # Start the iterations
        i = 0
        while True if iterations == float('inf') else i < iterations:
            i += 1
            start_time = time.time()
            print(f"Starting iteration {i} for temperature sensor: {self.producer_id}...")
            temperature = self.read_data()
            self.mqtt_manager.publish(self.producer_id, temperature, self.name, self.fw_version, self.status, description)
            print(f"Finished iteration {i} for temperature sensor: {self.producer_id}...")
            end_time = time.time()
            duration = end_time - start_time
            print(f"Iteration {i} for temperature sensor: {self.producer_id} took {duration:.2f} seconds")
            time.sleep(1)  # Delay to simulate processing time
    
    # Method: reads the minimum and maximum temperature for temperature sensor
    def read_data(self):
        return round(random.uniform(self.min_temp, self.max_temp), 2) 