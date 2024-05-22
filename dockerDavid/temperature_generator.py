import os
import json
import logging
import random
from kafka import KafkaConsumer, KafkaProducer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TemperatureGenerator:
    def __init__(self, kafka_broker, input_topic, output_topic):
        self.kafka_broker = kafka_broker
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.consumer = KafkaConsumer(input_topic, bootstrap_servers=kafka_broker)
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)

    def generate_random_temperature(self, min_val, max_val):
        temperature = round(random.uniform(min_val, max_val), 2)
        logger.info(f"Generated random temperature: {temperature}")
        return temperature

    def send_output_to_kafka(self, data):
        self.producer.send(self.output_topic, json.dumps(data).encode('utf-8'))
        self.producer.flush()

    def process_input_from_kafka(self):
        for message in self.consumer:
            logger.info(f"Received message: {message.value}")
            input_data = json.loads(message.value)
            min_temp = input_data.get('min_temp', 0)
            max_temp = input_data.get('max_temp', 100)
            logger.info(f"min_temp: {min_temp}, max_temp: {max_temp}")
            temperature = self.generate_random_temperature(min_temp, max_temp)
            output_data = {'temperature': temperature}
            try:
                self.send_output_to_kafka(output_data)
                logger.info("Temperature sent to output topic.")
            except Exception as e:
                logger.error(f"Error sending temperature to output topic: {e}")

if __name__ == "__main__":
    # Impostazioni di configurazione per Kafka e il generatore di temperature
    kafka_broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')  # Indirizzo e porta del broker Kafka
    input_topic = os.environ.get('INPUT_TOPIC', 'temperature_input')  # Argomento Kafka da cui ricevere i dati di input
    output_topic = os.environ.get('OUTPUT_TOPIC', 'temperature_output')  # Argomento Kafka su cui inviare i dati di output

    # Inizializza il generatore di temperature
    temperature_generator = TemperatureGenerator(kafka_broker, input_topic, output_topic)

    logger.info("Temperature generator started.")
    # Elabora continuamente i dati di input da Kafka e invia i dati di output generati a Kafka
    temperature_generator.process_input_from_kafka()
