import os
import json
import logging
import random
import time
from kafka import KafkaConsumer, KafkaProducer

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TemperatureGenerator:
    """
    TemperatureGenerator class: generates random temperature data and publishes it to a Kafka topic.
    """

    def __init__(self, kafka_broker, input_topic, output_topic):
        """
        Constructor: initializes the Kafka producer.

        :param kafka_broker: Kafka broker URL
        :param input_topic: Input Kafka topic for temperature range
        :param output_topic: Output Kafka topic for generated temperature data
        """
        self.kafka_broker = kafka_broker
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)

    def generate_random_temperature(self, min_val, max_val):
        """
        Generates a random temperature value within the given range.

        :param min_val: Minimum temperature value
        :param max_val: Maximum temperature value
        :return: Random temperature value
        """
        return round(random.uniform(min_val, max_val), 2)

    def send_data_to_kafka(self, topic, data):
        """
        Sends data (either temperature or temperature range) to a Kafka topic.

        :param topic: Kafka topic to send data to
        :param data: Data to send (temperature or range)
        """
        message_data = json.dumps(data).encode('utf-8')
        self.producer.send(topic, value=message_data)
        self.producer.flush()

    def generate_and_publish_temperature_range(self):
        """
        Generates a random temperature range and publishes it to the input Kafka topic.
        """
        min_temp = random.randint(10, 20)
        max_temp = random.randint(min_temp + 1, 30)
        temperature_range = {"min_temp": min_temp, "max_temp": max_temp}
        logger.info(f"Publishing temperature range: {temperature_range}")
        self.send_data_to_kafka(self.input_topic, temperature_range)
        return temperature_range

    def generate_and_publish_temperature(self, min_temp, max_temp):
        """
        Generates a temperature based on the given range and publishes it to the output Kafka topic.
        """
        temperature = self.generate_random_temperature(min_temp, max_temp)
        logger.info(f"Generated random temperature: {temperature}")
        self.send_data_to_kafka(self.output_topic, {"temperature": temperature})

    def start_temperature_generation(self, delay=5):
        """
        Starts the continuous process of generating temperature ranges and temperatures indefinitely.

        :param delay: Delay between temperature generation iterations (in seconds)
        """
        logger.info("Starting temperature generation process...")

        while True:
            try:
                # Generate and publish a new temperature range
                temperature_range = self.generate_and_publish_temperature_range()
                min_temp = temperature_range["min_temp"]
                max_temp = temperature_range["max_temp"]

                # Generate and publish a temperature within the range
                self.generate_and_publish_temperature(min_temp, max_temp)

                # Wait for the specified delay before generating the next temperature range
                time.sleep(delay)

            except Exception as e:
                logger.error(f"Error in temperature generation process: {e}")


if __name__ == "__main__":
    kafka_broker = os.environ.get('KAFKA_BROKER', 'broker:9092')
    input_topic = os.environ.get('INPUT_TOPIC')
    output_topic = os.environ.get('OUTPUT_TOPIC')

    logger.info(f"INPUT_TOPIC: {input_topic}")
    logger.info(f"OUTPUT_TOPIC: {output_topic}")

    if not input_topic or not output_topic:
        raise ValueError("INPUT_TOPIC and OUTPUT_TOPIC must be provided as environment variables")

    temperature_generator = TemperatureGenerator(kafka_broker, input_topic, output_topic)

    logger.info("Temperature generator started.")
    temperature_generator.start_temperature_generation(delay=5)  # You can adjust the delay as needed
    logger.info("Temperature generator finished.")
