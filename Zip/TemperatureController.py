import time
from kafka import KafkaConsumer
import uuid  # To generate a unique description
import paho.mqtt.client as mqtt



# TemperatureController class: controls the temperature sensors and runs their iterations
class TemperatureController:
    # Constructor: initializes the temperature controller
    def __init__(self, sensor_manager, iterations, config, kafka_consumer):
        self.sensor_manager = sensor_manager
        self.iterations = iterations
        self.config = config
        self.kafka_consumer = kafka_consumer  # Ora utilizzi KafkaTemperatureConsumer
        self.description = self.generate_unique_description()
  
  
    # Method: generates a unique description for the temperature controller
    def generate_unique_description(self):
        return f"Run_{uuid.uuid4()}"

    # Method: runs the iterations for the temperature sensors
    def run(self):
        print(f"Generated unique description: {self.description}")
        time.sleep(self.config['frequency'])  #delay for iteration
        self.sensor_manager.run_sensors(self.iterations, self.description)  
        # Close the kafka consumer when iterations are finished
        self.kafka_consumer.close()
        