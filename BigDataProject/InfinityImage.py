import os
import json
import logging
import random
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
        Constructor: initializes the Kafka producer and consumer.

        :param kafka_broker: Kafka broker URL
        :param input_topic: Input Kafka topic for temperature range
        :param output_topic: Output Kafka topic for generated temperature data
        """
        self.kafka_broker = kafka_broker
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)
        self.consumer = KafkaConsumer(
            bootstrap_servers=kafka_broker,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id='temperature-generator'
        )
        self.consumer.subscribe(topics=[input_topic])

    def generate_random_temperature(self, min_val, max_val):
        """
        Generates a random temperature value within the given range.

        :param min_val: Minimum temperature value
        :param max_val: Maximum temperature value
        :return: Random temperature value
        """
        return round(random.uniform(min_val, max_val), 2)

    def send_temperature_to_kafka(self, topic, data):
        """
        Sends temperature data to a Kafka topic.

        :param topic: Kafka topic to send data to
        :param data: Temperature data to send
        """
        message_data = json.dumps(data).encode('utf-8')
        self.producer.send(topic, value=message_data)
        self.producer.flush()

    def publish_initial_temperature_range(self):
        """
        Publishes an initial temperature range to the input Kafka topic.
        """
        min_temp = random.randint(10, 20)
        max_temp = random.randint(min_temp + 1, 30)
        initial_data = {"min_temp": min_temp, "max_temp": max_temp}
        logger.info(f"Publishing initial temperature range: {initial_data}")
        self.send_temperature_to_kafka(self.input_topic, initial_data)

    def process_messages(self):
        """
        Processes incoming messages from the input Kafka topic and generates temperature data.
        """
        logger.info("Waiting for a message from Kafka...")
        self.publish_initial_temperature_range()

        # Loop indefinitely to process incoming messages
        while True:
            try:
                # Wait for a message from the input Kafka topic
                for message in self.consumer:
                    input_data = json.loads(message.value)
                    min_temp = input_data.get('min_temp')
                    max_temp = input_data.get('max_temp')

                    if min_temp is None or max_temp is None:
                        raise ValueError("Message does not contain 'min_temp' or 'max_temp'")

                    logger.info(f"Received min_temp: {min_temp}, max_temp: {max_temp}")

                    temperature = self.generate_random_temperature(min_temp, max_temp)
                    logger.info(f"Generated random temperature: {temperature}")

                    self.send_temperature_to_kafka(self.output_topic, {"temperature": temperature})
                    logger.info("Temperature sent to output topic.")

                    # Commit the offset manually after processing the message
                    self.consumer.commit()
                    break  # Exit the inner loop to start waiting for the next message

            except Exception as e:
                logger.error(f"Error processing message: {e}")

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
    temperature_generator.process_messages()
    logger.info("Temperature generator finished.")