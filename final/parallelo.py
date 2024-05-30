import docker
import json
import logging
import threading
import time  # Added to get execution start time
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
    def __init__(self, topic, num_messages_to_read):
        self.topic = topic
        self.num_messages_to_read = num_messages_to_read
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['127.0.0.1:9092'],
            auto_offset_reset='earliest',
            #enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def start_consumer(self):
        logger.info(f"Starting Kafka consumer for topic: {self.topic}")
        num_messages_read = 0
        for message in self.consumer:
            logger.info(f"Received message: {message.value}")
            try:
                temperature = message.value["temperature"]
                logger.info(f"Temperature read from Kafka: {temperature}")
            except KeyError:
                logger.warning("No temperature information found in Kafka message")
            
            num_messages_read += 1
            if num_messages_read >= self.num_messages_to_read:
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
        except docker.errors.ImageNotFound:
            logger.error(f"Docker image '{image_name}' not found")
        except docker.errors.APIError as e:
            logger.error(f"Failed to start Docker container: {str(e)}")

    def stop_container(self):
        if self.container:
            logger.info("Stopping Docker container")
            self.container.stop()
            logger.info("Container stopped")


def main():
    start_time = time.time()  # Get the execution start time

    def read_configuration(file_path):
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    config = read_configuration("final/config.yaml")

    mqtt_config = config["outputs"][0]
    mqtt_manager = MQTTManager(host=mqtt_config["host"], port=mqtt_config["port"])
    mqtt_manager.connect()

    network = "rmoff_kafka"
    docker_managers = []  # List to store Docker handlers
    num_docker_sensors = 0 
    kafka_topic = "output_topic"  # Replace with the correct Kafka topic
    kafka_consumer_manager = KafkaConsumerManager(kafka_topic, num_messages_to_read=num_docker_sensors)
    kafka_consumer_thread = threading.Thread(target=kafka_consumer_manager.start_consumer)
    kafka_consumer_thread.start()  # Start the Kafka consumer on a separate thread

    # Add a log message for the start of execution
    logger.info("Main execution started")

    # Scroll through the list of producers
    for producer in config["producers"]:
        producer_type = producer.get("data", {}).get("type")
        logger.info(f"Producer {producer['id']} has type: {producer_type}")
        if "type" in producer.get("data", {}) and producer["data"]["type"] == "docker sensor":
            logger.info(f"Docker container thread started for sensor {producer['id']} at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
            num_docker_sensors += 1
            logger.info(f"Found docker sensor producer: {producer['id']}")
            # Starting the Docker temp generator on a separate thread
            docker_manager = DockerStartManager(network=network, environment=producer["config"])
            docker_thread = threading.Thread(target=docker_manager.start_container, args=(producer["config"]["image"],))
            docker_thread.start()
            docker_managers.append(docker_manager)
            logger.info(f"Attempted to start temperature generator Docker container for {producer['id']}")
        else:
            logger.info(f"Docker container thread started for sensor {producer['id']} at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))}")
            logger.info(f"Ignored producer {producer['id']}, not a docker sensor")
            # Generate random temperature and publish MQTT message
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

    end_time = time.time()  # Get the execution end time
    execution_time = end_time - start_time
    logger.info(f"Main execution finished in {execution_time} seconds")

if __name__ == "__main__":
    main()
