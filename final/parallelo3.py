import json
import logging
import threading
import time
import docker
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
    def __init__(self, topic, inactivity_timeout=10, group_id='my_consumer_group'):
        self.topic = topic
        self.active = True
        self.last_message_time = time.time()
        self.inactivity_timeout = inactivity_timeout
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['127.0.0.1:9092'],
            auto_offset_reset='earliest',  # Impostato su 'latest' per leggere solo i nuovi messaggi
            enable_auto_commit=False,  # Disattiva auto commit
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def start_consumer(self):
        logger.info(f"Starting Kafka consumer for topic: {self.topic}")
        while self.active:
            messages = self.consumer.poll(timeout_ms=1000)  # Poll per i messaggi per un secondo
            if not messages:  # Se non ci sono messaggi, termina il consumatore
                logger.info("No more messages to consume. Exiting Kafka consumer.")
                break
            for message in messages.values():
                for msg in message:
                    self.process_message(msg)
                    self.consumer.commit()  # Commit manuale dopo l'elaborazione del messaggio
            self.check_activity()

    def process_message(self, message):
        self.last_message_time = time.time()
        logger.info(f"Received message: {message.value}")
        try:
            temperature = message.value["temperature"]
            logger.info(f"Temperature read from Kafka: {temperature}")
        except KeyError:
            logger.warning("No temperature information found in Kafka message")

  
    
    def check_activity(self):
        current_time = time.time()
        time_since_last_message = current_time - self.last_message_time
        if time_since_last_message >= self.inactivity_timeout:
            logger.info("Kafka consumer finished due to inactivity timeout.")
            self.active = False
        else:
            logger.info(f"Time since last message: {time_since_last_message} seconds")

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
    start_time = time.time()

    def read_configuration(file_path):
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    config = read_configuration("final\config.yaml")

    mqtt_config = config["outputs"][0]
    mqtt_manager = MQTTManager(host=mqtt_config["host"], port=mqtt_config["port"])
    mqtt_manager.connect()

    network = "rmoff_kafka"
    docker_managers = []
    kafka_topic = "output_topic"
    kafka_consumer_manager = KafkaConsumerManager(kafka_topic)

    logger.info("Main execution started")

    kafka_consumer_thread = threading.Thread(target=kafka_consumer_manager.start_consumer)
    kafka_consumer_thread.start()

    for producer in config["producers"]:
        producer_id = producer["id"]
        producer_type = producer.get("data", {}).get("type")
        logger.info(f"Producer {producer_id} has type: {producer_type}")

        if producer_type == "docker sensor":
            logger.info(f"Found docker sensor producer: {producer_id}")
            docker_manager = DockerStartManager(network=network, environment=producer.get("config", {}))
            docker_thread = threading.Thread(target=docker_manager.start_container, args=(producer.get("config", {}).get("image"),))
            docker_thread.start()
            docker_managers.append(docker_manager)
            logger.info(f"Attempted to start temperature generator Docker container for {producer_id}")
            

        else:
            logger.info(f"Ignored producer {producer_id}, not a docker sensor")
        
        # Aggiungi la logica per la pubblicazione MQTT di sens_temp_1
        if producer_type == "temperature_sensor":
            temperature = random.uniform(10.1, 25.5)
            logger.info(f"Generated random temperature for {producer_id}: {temperature}")

            sensor_info = yaml.dump(producer.get("data", {}), default_flow_style=True)
            message = f"Temperature for {producer_id}: {temperature}\nSensor Info:\n{sensor_info}"

            topic = mqtt_config["topic"].replace("${PRODUCER_ID}", producer_id)
            mqtt_manager.publish_message(topic, message)
            logger.info(f"Published MQTT message for {producer_id}")

    time.sleep(20)


    for docker_manager in docker_managers:
        docker_manager.stop_container()
        logger.info("Temperature generator container stopped")


    kafka_consumer_thread.join()
    mqtt_manager.disconnect()

    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Main execution finished in {execution_time} seconds")

if __name__ == "__main__":
    main()
