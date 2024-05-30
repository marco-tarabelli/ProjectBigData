import docker
import json
import logging
import threading
from kafka import KafkaConsumer
import yaml
import paho.mqtt.client as mqtt
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MQTTManager:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = mqtt.Client()

    def connect(self):
        self.client.connect(self.host, self.port)

    def publish_message(self, topic, message):
        self.client.publish(topic, message)

    def disconnect(self):
        self.client.disconnect()

class KafkaConsumerManager:
    def __init__(self, topic):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['127.0.0.1:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def start_consumer(self):
        logger.info(f"Starting Kafka consumer for topic: {self.topic}")
        for message in self.consumer:
            logger.info(f"Received message: {message.value}")
            try:
                temperature = message.value["temperature"]
                logger.info(f"Temperature read from Kafka: {temperature}")
            except KeyError:
                logger.warning("No temperature information found in Kafka message")
            break

class DockerStartManager:
    def __init__(self, network, environment):
        self.network = network
        self.environment = environment
        self.client = docker.from_env()
        self.container = None

    def start_container(self, image_name):
        logger.info(f"Starting Docker container for image: {image_name}")
        try:
            self.environment.update({
                "KAFKA_BROKER": self.environment.get("kafka_broker"),
                "INPUT_TOPIC": self.environment.get("input_topic"),
                "OUTPUT_TOPIC": self.environment.get("output_topic")
            })

            self.container = self.client.containers.run(
                image_name,
                detach=True,
                network=self.network,
                environment=self.environment
            )
            logger.info(f"Container ID: {self.container.id}")
            logger.info(f"Container status: {self.container.status}")

            # Avvia il consumer Kafka su un altro thread
            kafka_consumer_thread = threading.Thread(target=self.start_kafka_consumer)
            kafka_consumer_thread.start()
        except docker.errors.ImageNotFound:
            logger.error(f"Docker image '{image_name}' not found")
        except docker.errors.APIError as e:
            logger.error(f"Failed to start Docker container: {str(e)}")

    def start_kafka_consumer(self):
        kafka_topic = "output_topic"
        kafka_consumer_manager = KafkaConsumerManager(kafka_topic)
        kafka_consumer_manager.start_consumer()

    def stop_container(self):
        if self.container:
            logger.info("Stopping Docker container")
            self.container.stop()
            logger.info("Container stopped")


def main():
    def read_configuration(file_path):
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    config = read_configuration("final/config.yaml")

    mqtt_config = config["outputs"][0]
    mqtt_manager = MQTTManager(host=mqtt_config["host"], port=mqtt_config["port"])
    mqtt_manager.connect()

    network = "rmoff_kafka"
    docker_managers = []

    for producer in config["producers"]:
        producer_type = producer.get("data", {}).get("type")
        logger.info(f"Producer {producer['id']} has type: {producer_type}")
        if "type" in producer.get("data", {}) and producer["data"]["type"] == "docker sensor":
            logger.info(f"Found docker sensor producer: {producer['id']}")
            docker_manager = DockerStartManager(network=network, environment=producer["config"])
            docker_manager.start_container(image_name=producer["config"]["image"])
            docker_managers.append(docker_manager)
            logger.info(f"Attempted to start temperature generator Docker container for {producer['id']}")
        else:
            logger.info(f"Ignored producer {producer['id']}, not a docker sensor")
            temperature = random.uniform(10.1, 25.5)
            logger.info(f"Generated random temperature for {producer['id']}: {temperature}")
            sensor_info = yaml.dump(producer["data"], default_flow_style=True)
            message = f"Temperature for {producer['id']}: {temperature}\nSensor Info:\n{sensor_info}"
            topic = mqtt_config["topic"].replace("${PRODUCER_ID}", producer["id"])
            mqtt_manager.publish_message(topic, message)
            logger.info(f"Published MQTT message for {producer['id']}")

    for docker_manager in docker_managers:
        docker_manager.stop_container()
        logger.info("Temperature generator container stopped")

    mqtt_manager.disconnect()

if __name__ == "__main__":
    main()
