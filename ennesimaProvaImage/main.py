import os  # To access environment variables.
import json  # To handle JSON data.
import logging #  For event logging.
import random # To generate random numbers.
from kafka import KafkaConsumer, KafkaProducer # To consume and produce messages in Kafka.

# Configures logging at the INFO level.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TemperatureGenerator:
    # Initializes the class with Kafka brokers and the input and output topics. Sets up the Kafka consumer and producer.
    def __init__(self, kafka_broker, input_topic, output_topic):
        self.kafka_broker = kafka_broker
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.consumer = KafkaConsumer(
            bootstrap_servers=kafka_broker,
            auto_offset_reset='earliest',
            enable_auto_commit=False  # Disable automatic offset commit
        )
        self.consumer.subscribe(topics=[input_topic])
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)

    # function that generates a random temperature between specified minimum and maximum values.
    def generate_random_temperature(self, min_val, max_val):
        return round(random.uniform(min_val, max_val), 2)

    # Encodes the temperature as JSON and sends it to the output Kafka topic.
    def send_temperature_to_kafka(self, temperature):
        temperature_data = json.dumps({"temperature": temperature}).encode('utf-8')
        self.producer.send(self.output_topic, value=temperature_data)
        self.producer.flush()

    # Reads messages from the input Kafka topic, extracts the minimum and maximum temperature values,
    # generates a random temperature, and sends it to the output topic.
    # If there are no more messages, it closes the Kafka consumer and producer.
    def process_input_from_kafka(self):
        while True:
            message = next(self.consumer)  # Read one message at a time
            try:
                input_data = json.loads(message.value)
                min_temp = input_data.get('min_temp')
                max_temp = input_data.get('max_temp')
                logger.info(f"Received min_temp: {min_temp}, max_temp: {max_temp}")

                temperature = self.generate_random_temperature(min_temp, max_temp)
                logger.info(f"Generated random temperature: {temperature}")

                self.send_temperature_to_kafka(temperature)
                logger.info("Temperature sent to output topic.")

                # Commit the offset manually after processing the message
                self.consumer.commit()
            except Exception as e:
                logger.error(f"Error processing message: {e}")

        self.consumer.close()
        self.producer.close()

if __name__ == "__main__":
    #Retrieves environment variables to configure the Kafka broker and the input and output topics.
    kafka_broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')
    input_topic = os.environ.get('INPUT_TOPIC')
    output_topic = os.environ.get('OUTPUT_TOPIC')

    logger.info(f"KAFKA_BROKER: {kafka_broker}")
    logger.info(f"INPUT_TOPIC: {input_topic}")
    logger.info(f"OUTPUT_TOPIC: {output_topic}")

    # Checks that the input and output topics are specified; otherwise, it raises an error.
    if not input_topic or not output_topic:
        raise ValueError("INPUT_TOPIC and OUTPUT_TOPIC must be provided as environment variables")
    # Initializes an instance of TemperatureGenerator and starts the temperature generation process.
    temperature_generator = TemperatureGenerator(kafka_broker, input_topic, output_topic)

    logger.info("Temperature generator started.")
    temperature_generator.process_input_from_kafka()