import json
import logging
import random
import os
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TemperatureGenerator:
    def __init__(self, kafka_broker, input_topic, output_topic):
        self.kafka_broker = kafka_broker
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.consumer = KafkaConsumer(
            bootstrap_servers=kafka_broker,
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # Disabilita il commit automatico degli offset
        )
        self.consumer.subscribe(topics=[input_topic])
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)

    def generate_random_temperature(self, min_val, max_val):
        return round(random.uniform(min_val, max_val), 2)

    def send_temperature_to_kafka(self, temperature):
        temperature_data = json.dumps({"temperature": temperature}).encode('utf-8')
        self.producer.send(self.output_topic, value=temperature_data)
        self.producer.flush()

    def send_min_max_to_kafka(self, min_temp, max_temp):
        min_max_data = json.dumps({"min_temp": min_temp, "max_temp": max_temp}).encode('utf-8')
        self.producer.send(self.input_topic, value=min_max_data)
        self.producer.flush()

    def process_input_from_kafka(self):
        try:
            # Leggi un solo messaggio dal topic di input
            message = next(self.consumer)
            input_data = json.loads(message.value)
            min_temp = input_data.get('min_temp')
            max_temp = input_data.get('max_temp')
            logger.info(f"Received min_temp: {min_temp}, max_temp: {max_temp}")

            temperature = self.generate_random_temperature(min_temp, max_temp)
            logger.info(f"Generated random temperature: {temperature}")

            self.send_temperature_to_kafka(temperature)
            logger.info("Temperature sent to output topic.")
            self.consumer.commit()
        except StopIteration:
            logger.info("No more messages in input topic.")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    kafka_broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')
    input_topic = os.environ.get('INPUT_TOPIC')
    output_topic = os.environ.get('OUTPUT_TOPIC')

    logger.info(f"KAFKA_BROKER: {kafka_broker}")
    logger.info(f"INPUT_TOPIC: {input_topic}")
    logger.info(f"OUTPUT_TOPIC: {output_topic}")

    if not input_topic or not output_topic:
        raise ValueError("INPUT_TOPIC and OUTPUT_TOPIC must be provided as environment variables")

    temperature_generator = TemperatureGenerator(kafka_broker, input_topic, output_topic)

    logger.info("Temperature generator started.")

    # Inizia a processare un solo messaggio dall'input_topic
    temperature_generator.process_input_from_kafka()
