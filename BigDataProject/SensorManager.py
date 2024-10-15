
from kafka import KafkaConsumer

import threading

import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod
from ISensorManager import ISensorManager


# SensorManager class: manages a list of sensors and runs their iterations
class SensorManager(ISensorManager):
    # Constructor: initializes the sensor list
    def __init__(self):
        # Initialize an empty list to store sensors
        self.sensors = []

    # Method: adds a sensor to the list
    def add_sensor(self, sensor):
        # Append the sensor to the list
        self.sensors.append(sensor)

    # Method: runs the iterations for all sensors in the list
    def run_sensors(self, iterations):
        # Create a list to store threads for each sensor
        threads = []
        # Iterate over each sensor in the list
        for sensor in self.sensors:
            # Create a new thread for the sensor's run method
            thread = threading.Thread(target=sensor.run, args=(iterations,))
            # Append the thread to the list
            threads.append(thread)
            # Start the thread
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            # Join the thread to wait for its completion
            thread.join()