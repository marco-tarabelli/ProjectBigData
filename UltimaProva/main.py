import time
import docker
import yaml
import threading
import logging
from kafka import KafkaConsumer
import paho.mqtt.client as mqtt
import random
from TemperatureManager import TemperatureManager
from DockerStartManager import DockerStartManager
from KafkaConsumerManager import KafkaConsumerManager
from MQTTManager import MQTTManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_configuration(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def main():
    start_time = time.time()

    config = read_configuration("config.yaml")

    mqtt_config = config["outputs"][0]
    mqtt_manager = MQTTManager(host=mqtt_config["host"], port=mqtt_config["port"])
    mqtt_manager.connect()
    frequency = mqtt_config["frequency"]
    frequency_count = mqtt_config["frequency_count"]

    if frequency_count == -1:
        frequency_count = float('inf')  # Run indefinitely if frequency_count is -1

    # docker network
    network = "rmoff_kafka"
    docker_managers = []

    # topic where we publish the result of Docker container
    kafka_topic = "output_topic"
    kafka_consumer_manager = KafkaConsumerManager(kafka_topic, mqtt_manager)

    logger.info("Main execution started")

    # Start kafka consumer in a separate thread
    kafka_consumer_thread = threading.Thread(target=kafka_consumer_manager.start_consumer)
    kafka_consumer_thread.start()

    # List to keep track of Docker Manager and temperature generation threads
    threads = []

    temperature_manager = TemperatureManager(mqtt_manager, mqtt_config, kafka_consumer_manager, config)

    loop_count = 0
    while loop_count < frequency_count:
        for producer in config["producers"]:
            producer_id = producer["id"]
            producer_type = producer.get("data", {}).get("type")
            logger.info(f"Producer {producer_id} has type: {producer_type}")
            if producer_type == "docker sensor":
                logger.info(f"Found docker sensor producer: {producer_id}")
                docker_manager = DockerStartManager(network=network, environment=producer.get("config", {}))
                docker_thread = threading.Thread(target=docker_manager.start_container,
                                                 args=(producer.get("config", {}).get("image"),))
                docker_thread.start()
                docker_managers.append(docker_manager)
                threads.append(docker_thread)
                logger.info(
                    f"Attempted to start temperature generator Docker container for {producer_id} at {time.time() - start_time:.2f} seconds")

            # Handle logic for MQTT publication of temperature sensor that are not docker sensor
            elif producer_type == "temperature_sensor":
                temp_thread = threading.Thread(target=temperature_manager.generate_and_publish_temperature,
                                               args=(producer_id, mqtt_manager, mqtt_config))
                logger.info(
                    f"Starting temperature sensor thread for {producer_id} at {time.time() - start_time:.2f} seconds")
                temp_thread.start()
                threads.append(temp_thread)
            else:
                logger.info(f"Ignored producer {producer_id}, not a docker or temperature sensor")

        # Wait a certain amount of time to allow threads to execute
        time.sleep(20)

        # Process and publish temperature collected from kafka
        temperature_manager.handle_temperatures_and_publish_messages(kafka_consumer_manager, mqtt_manager, mqtt_config,
                                                                     config)

        loop_count += 1
        time.sleep(frequency / 10)

    # Stop all docker containers
    for docker_manager in docker_managers:
        docker_manager.stop_container()
        logger.info("Temperature generator container stopped")

    # Wait for all other threads
    for thread in threads:
        thread.join()
        logger.info(f"Thread {thread.name} finished execution at {time.time() - start_time:.2f} seconds")

    # Disconnect from the MQTT broker
    mqtt_manager.disconnect()

    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Main execution finished in {execution_time} seconds")

if __name__ == "__main__":
    main()