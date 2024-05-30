import docker
import json
import logging
import threading
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaConsumerManager:
    def __init__(self, topic):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['127.0.0.1:9092'],  #specifico server (indirizzo ip e indirizzo broker)
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            #group_id="consumer"
        )

    def start_consumer(self):
        logger.info(f"Starting Kafka consumer for topic: {self.topic}")
        for message in self.consumer:
            print("Messaggio ricevuto:", message.value)
            #print("Valore del messaggio:", message.value['result'])
            break  # Interrompe il loop dopo aver ricevuto il primo messaggio


class DockerStartManager:
    def __init__(self, network, environment):
        self.network = network
        self.environment = environment
        self.client = docker.from_env()
        self.container = None

    def start_container(self, image_name):
        logger.info(f"Starting Docker container for image: {image_name}")
        self.container = self.client.containers.run(
            image_name,
            detach=True,
            network=self.network,
            environment=self.environment
        )
        logger.info(f"Container status: {self.container.status}")

    def stop_container(self):
        if self.container:
            logger.info("Stopping Docker container")
            self.container.stop()
            logger.info("Container stopped")


def main():
    network = "rmoff_kafka"
    environment = {
        'KAFKA_BROKER': 'broker:9092',
        'INPUT_TOPIC': 'input_topic',
        'OUTPUT_TOPIC': 'output_topic'
    }

    # Avvio del consumatore Kafka in un thread separato
    consumer_manager = KafkaConsumerManager(topic='output_topic')
    consumer_thread = threading.Thread(target=consumer_manager.start_consumer)
    consumer_thread.start()
    logger.info("Kafka consumer started")

    # Avvio del generatore di temperature
    docker_manager = DockerStartManager(network=network, environment=environment)
    docker_manager.start_container(image_name='marcotarabelli/temperature-generator')
    logger.info("Temperature generator container started")
    
    # Attendere il completamento del thread del consumatore Kafka
    consumer_thread.join()

    # Interrompe il container al termine
    docker_manager.stop_container()
    logger.info("Temperature generator container stopped")


if __name__ == "__main__":
    main()
