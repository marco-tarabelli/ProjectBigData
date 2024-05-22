import os
import json
import logging
import random
from kafka import KafkaConsumer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TemperatureGenerator:
    def __init__(self, kafka_broker, input_topic):
        self.kafka_broker = kafka_broker
        self.input_topic = input_topic
        logger.info(f"PRENDE INPUT DA KAFKA")
        self.consumer = KafkaConsumer(bootstrap_servers=kafka_broker, auto_offset_reset='earliest')

        # Assegna tutte le partizioni del topic al consumatore
        self.consumer.subscribe(topics=[input_topic])

    def generate_random_temperature(self, min_val, max_val):
        return round(random.uniform(min_val, max_val), 2)

    def process_input_from_kafka(self):
        logger.info("Consumer assigned partitions: %s" % self.consumer.assignment())
        while True:
            messages = self.consumer.poll(timeout_ms=1000)  # Poll per controllare se ci sono messaggi disponibili
            if not messages:
                logger.info("No more messages in the topic. Stopping the temperature generator.")
                break  # Esci dal ciclo se non ci sono messaggi disponibili
            
            for message in messages.values():
                for msg in message:
                    try:
                        input_data = json.loads(msg.value)
                        min_temp = input_data.get('min_temp')
                        max_temp = input_data.get('max_temp')
                        logger.info(f"Received min_temp: {min_temp}, max_temp: {max_temp}")
                        
                        temperature = self.generate_random_temperature(min_temp, max_temp)
                        logger.info(f"Generated random temperature: {temperature}")
                        
                        print(f"Generated Temperature: {temperature}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
        
        self.consumer.close()        

if __name__ == "__main__":
    # Impostazioni di configurazione per Kafka e il generatore di temperature
    kafka_broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')  # Indirizzo e porta del broker Kafka
    input_topic = os.environ.get('INPUT_TOPIC', 'temperature_input')  # Argomento Kafka da cui ricevere i dati di input

    # Inizializza il generatore di temperature
    temperature_generator = TemperatureGenerator(kafka_broker, input_topic)

    logger.info("Temperature generator started.")
    # Elabora continuamente i dati di input da Kafka e stampa le temperature lette sul terminale
    temperature_generator.process_input_from_kafka()
