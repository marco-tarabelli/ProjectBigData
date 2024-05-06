import os
import json
import logging
import random
from kafka import KafkaProducer
#SU KAFKA STAMPA BENE OUTPUT


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TemperatureGenerator:
    def __init__(self, kafka_broker, output_topic):
        self.kafka_broker = kafka_broker
        self.output_topic = output_topic
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)
        self.num_cycles = 0

    def generate_random_temperature(self, min_val, max_val):
        return round(random.uniform(min_val, max_val), 2)

    def send_output_to_kafka(self, data):
        self.producer.send(self.output_topic, json.dumps(data).encode('utf-8'))
        self.producer.flush()

    def generate_and_send_temperature(self, min_temp, max_temp):
        temperature = self.generate_random_temperature(min_temp, max_temp)
        output_data = {'temperature': temperature}
        self.send_output_to_kafka(output_data)
        logger.info(f"Temperature generated: {temperature}")

if __name__ == "__main__":
    # Impostazioni di configurazione per Kafka e il generatore di temperature
    kafka_broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')  # Indirizzo e porta del broker Kafka
    output_topic = os.environ.get('OUTPUT_TOPIC', 'temperature_output')  # Argomento Kafka su cui inviare i dati di output

    # Inizializza il generatore di temperature
    temperature_generator = TemperatureGenerator(kafka_broker, output_topic)

    logger.info("Temperature generator started.")
    
    # Genera e invia dati di temperatura casuali a Kafka per un numero fisso di cicli
    min_temp = 0
    max_temp = 100
    num_cycles = 3  # Numero di cicli da eseguire
    for _ in range(num_cycles):
        temperature_generator.generate_and_send_temperature(min_temp, max_temp)
        temperature_generator.num_cycles += 1
