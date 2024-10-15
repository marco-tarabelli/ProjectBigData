
import docker
from kafka import KafkaConsumer
import paho.mqtt.client as mqtt
from IDockerManager import IDockerManager


# DockerManager class: manages a Docker container
class DockerManager(IDockerManager):
    # Constructor: initializes the Docker manager
    def __init__(self, docker_config):
        # Initialize the Docker manager
        self.client = docker.from_env()
        self.image_name = docker_config.get('image')
        self.network_name = docker_config.get('docker_net')
        self.environment_vars = {
            'KAFKA_BROKER': docker_config.get('kafka_broker'),
            'INPUT_TOPIC': docker_config.get('input_topic'),
            'OUTPUT_TOPIC': docker_config.get('output_topic')
        }
        self.container_name = 'temperature_generator_container'  # Container name

    # Method: starts the Docker container
    def start_container(self):
        # Start the Docker container
        print("Checking if the container is already running...")
        existing_container = self.get_existing_container()

        if existing_container:
            print(f"Container {self.container_name} found.")
            if existing_container.status != 'running':
                print(f"Container {self.container_name} is not running. Starting...")
                existing_container.start()
            else:
                print(f"Container {self.container_name} is already running.")
            return existing_container

        print(f"Starting new container {self.container_name}")
        container = self.client.containers.run(
            self.image_name,
            network =self.network_name,
            detach=True,
            environment=self.environment_vars,
            name=self.container_name
        )
        return container

    # Method: gets the existing Docker contain
    def get_existing_container(self):
        try:
            container = self.client.containers.get(self.container_name)
            print(f"Found existing container: {self.container_name}")
            return container
        except docker.errors.NotFound:
            print(f"Container {self.container_name} not found.")
            return None

   
    # Method: stops the Docker container
    def stop_container(self):
        # Stop the Docker container
        print(f"Stopping container {self.container_name}")
        container = self.get_existing_container()
        if container:
            container.stop()
        else:
            print(f"Container {self.container_name} not found.")

    # Method: remove the Docker container
    def remove_container(self):
        container = self.get_existing_container()
        if container:
            container.remove()
            print(f"Removed container {self.container_name}")


    # Method: restarts the Docker container
    def restart_container(self):
    #""" Restart the existing container or start it if it's stopped. """
        container = self.get_existing_container()
        if container:
            print(f"Container status before restart: {container.status}")
            if container.status == 'running':
                print(f"Restarting container {self.container_name}...")
                container.restart()
                print(f"Container {self.container_name} restarted.")
            elif container.status == 'exited':
                print(f"Container {self.container_name} is exited. Starting it again...")
                container.start()
                print(f"Container {self.container_name} started.")
            else:
                print(f"Container {self.container_name} is in state {container.status}. Cannot restart or start.")
        else:
            print(f"No existing container to restart.")
            return self.start_container()
    