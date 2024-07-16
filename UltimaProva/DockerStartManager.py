import json
import logging
import threading
import time
import docker
from kafka import KafkaConsumer
import yaml
import paho.mqtt.client as mqtt
import random


#Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#This class manages the operations of Docker containers, including starting and stopping containers.
class DockerStartManager:
    # Initializes the class with the Docker network and environment variables, sets up the Docker client, and initializes the container attribute to None.
    def __init__(self, network, environment):
        self.network = network
        self.environment = environment
        self.client = docker.from_env()
        self.container = None

    #Start a Docker container
    #Starts a Docker container with the specified image name.
    # It updates the environment variables to include Kafka broker and topic information,
    # runs the container in detached mode,
    # and logs the container ID and status.
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
            #Logs errors if the Docker image is not found or if there is an API error.
        except docker.errors.ImageNotFound:
            logger.error(f"Docker image '{image_name}' not found")
        except docker.errors.APIError as e:
            logger.error(f"Failed to start Docker container: {str(e)}")

    #Stop the docker container
    def stop_container(self):
        if self.container:
            logger.info("Stopping Docker container")
            self.container.stop()
            logger.info("Container stopped")