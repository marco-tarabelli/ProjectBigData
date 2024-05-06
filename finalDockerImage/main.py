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
        self.consumer = KafkaConsumer(bootstrap_servers=kafka_broker, auto_offset_reset='earliest')
        self.consumer.subscribe(topics=[input_topic])
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)

    def generate_random_temperature(self, min_val, max_val):
        return round(random.uniform(min_val, max_val), 2)

    def send_temperature_to_kafka(self, temperature):
        temperature_data = json.dumps({"temperature": temperature}).encode('utf-8')
        self.producer.send(self.output_topic, value=temperature_data)
        self.producer.flush()

    def process_input_from_kafka(self):
        while True:
            messages = self.consumer.poll(timeout_ms=1000)
            if not messages:
                logger.info("No more messages in the topic. Stopping the temperature generator.")
                break
            
            for message in messages.values():
                for msg in message:
                    try:
                        input_data = json.loads(msg.value)
                        min_temp = input_data.get('min_temp')
                        max_temp = input_data.get('max_temp')
                        logger.info(f"Received min_temp: {min_temp}, max_temp: {max_temp}")
                        
                        temperature = self.generate_random_temperature(min_temp, max_temp)
                        logger.info(f"Generated random temperature: {temperature}")
                        
                        self.send_temperature_to_kafka(temperature)
                        logger.info("Temperature sent to output topic.")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
        
        self.consumer.close()
        self.producer.close()

if __name__ == "__main__":
    kafka_broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')
    input_topic = os.environ.get('INPUT_TOPIC')
    output_topic = os.environ.get('OUTPUT_TOPIC')

    if not input_topic or not output_topic:
        raise ValueError("INPUT_TOPIC and OUTPUT_TOPIC must be provided as environment variables")

    temperature_generator = TemperatureGenerator(kafka_broker, input_topic, output_topic)

    logger.info("Temperature generator started.")
    temperature_generator.process_input_from_kafka()
