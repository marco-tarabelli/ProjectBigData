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


# Define a class to manage MQTT connections and messaging
class MQTTManager:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = mqtt.Client()
    #Connect to the MQTT broker
    def connect(self):
        self.client.connect(self.host, self.port)
    #Publish a message to the MQTT broker in a specific topic
    def publish_message(self, topic, message):
        self.client.publish(topic, message)
    #Disconnect from the MQTT broker
    def disconnect(self):
        self.client.disconnect()

# Define a class to manage Kafka consumer operations
class KafkaConsumerManager:
    def __init__(self, topic, mqtt_manager, inactivity_timeout=60, group_id='my_consumer_group'):
        self.topic = topic
        self.active = True
        self.last_message_time = time.time()
        self.inactivity_timeout = inactivity_timeout
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['127.0.0.1:9092'],
            auto_offset_reset='earliest',  #earliest offset so the consumer start from the begging value
            enable_auto_commit=False,  # Disable auto commit 
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.mqtt_manager = mqtt_manager
        self.temperatures = {}  # Dictionary used for saving the temperature extracted for each productor
        self.temperature_index = 0 

    #Start the kafka consumer
    def start_consumer(self):
        logger.info(f"Starting Kafka consumer for topic: {self.topic}")
        while self.active:
            messages = self.consumer.poll(timeout_ms=1000)  #
            if not messages:  # If no messagges, exit the consumer
                logger.info("No more messages to consume. Exiting Kafka consumer.")
                break
            for message in messages.values():
                for msg in message:
                    self.process_message(msg)
                    self.consumer.commit()  # Manual commit after message processing
            self.check_activity()

    #Process the message received from the kafka consumer
    def process_message(self, message):
        self.last_message_time = time.time()
        logger.info(f"Received message: {message.value}")
        try:
            temperature = message.value["temperature"]
            logger.info(f"Temperature read from Kafka: {temperature}")
            self.temperatures[self.temperature_index] = temperature  # Aggiungi la temperatura al dizionario
            self.temperature_index += 1 
        except KeyError:
            logger.warning("No temperature information found in Kafka message")

    #Check for consumer inactivity and stop if timeout is reached
    def check_activity(self):
        current_time = time.time()
        time_since_last_message = current_time - self.last_message_time
        if time_since_last_message >= self.inactivity_timeout:
            logger.info("Kafka consumer finished due to inactivity timeout.")
            self.active = False
        else:
            logger.info(f"Time since last message: {time_since_last_message} seconds")

    #Get all the temperature collected    
    def get_all_temperatures(self):
        return list(self.temperatures.values()) 
    
#Define a class to manage Docker container operations
class DockerStartManager:
    def __init__(self, network, environment):
        self.network = network
        self.environment = environment
        self.client = docker.from_env()
        self.container = None

    #Start a Docker container
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

    #Stop the docker container
    def stop_container(self):
        if self.container:
            logger.info("Stopping Docker container")
            self.container.stop()
            logger.info("Container stopped")

#fare classe per questi due metodi
#Generate a random temperature and publish it on MQTT broker in a specific topic
def generate_and_publish_temperature(producer_id, mqtt_manager, mqtt_config):
    temperature = random.uniform(10.1, 25.5)
    logger.info(f"Generated random temperature for {producer_id}: {temperature}")

    sensor_info = yaml.dump({"data": {"type": "temperature_sensor"}}, default_flow_style=True)
    message = f"Temperature for {producer_id}: {temperature}\nSensor Info:\n{sensor_info}"

    topic = mqtt_config["topic"].replace("${PRODUCER_ID}", producer_id)
    mqtt_manager.publish_message(topic, message)
    logger.info(f"Published MQTT message for {producer_id}")

#Handle temperatures from kafka and publish it on MQTT broker in a specific topic
def handle_temperatures_and_publish_messages(kafka_consumer_manager, mqtt_manager, mqtt_config, config):
    all_temperatures = kafka_consumer_manager.get_all_temperatures()
    for index, temperature in enumerate(all_temperatures):
        logger.info(f"Temperature {index + 1}: {temperature}")
        for producer in config["producers"]:
            producer_id = producer["id"]
            producer_type = producer.get("data", {}).get("type")
            if producer_type == "docker sensor":
                logger.info(f" {producer_id} has temperature: {temperature}")
                sensor_info = yaml.dump(producer.get("data", {}), default_flow_style=True)
                message = f"Temperature for {producer_id}: {temperature}\nSensor Info:\n{sensor_info}"

                topic = mqtt_config["topic"].replace("${PRODUCER_ID}", producer_id)
                mqtt_manager.publish_message(topic, message)
                logger.info(f"Published MQTT message for {producer_id}")


#Main funcition
def main():
    start_time = time.time()

    #Read the configuration file
    def read_configuration(file_path):
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    config = read_configuration("config.yaml")

    mqtt_config = config["outputs"][0]
    mqtt_manager = MQTTManager(host=mqtt_config["host"], port=mqtt_config["port"])
    mqtt_manager.connect()

    network = "rmoff_kafka"
    docker_managers = []
    kafka_topic = "output_topic"
    kafka_consumer_manager = KafkaConsumerManager(kafka_topic,mqtt_manager)

    logger.info("Main execution started")

    #Start kafka consumer in a separate thread
    kafka_consumer_thread = threading.Thread(target=kafka_consumer_manager.start_consumer)
    kafka_consumer_thread.start()

    # List to keep track of Docker Manager and temperature generation threads
    threads = []

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
            threads.append(docker_thread)
            logger.info(f"Attempted to start temperature generator Docker container for {producer_id} at {time.time() - start_time:.2f} seconds")
            
               
        
        
        # Hnadle logic for MQTT publication of temperature sensor that are not docker sensor
        elif producer_type == "temperature_sensor":
                temp_thread = threading.Thread(target=generate_and_publish_temperature, args=(producer_id, mqtt_manager, mqtt_config))
                logger.info(f"Starting temperature sensor thread for {producer_id} at {time.time() - start_time:.2f} seconds")
                temp_thread.start()
                threads.append(temp_thread)        
        else:
            logger.info(f"Ignored producer {producer_id}, not a docker or temperature sensor")        

    
   
    

    
    #Wait a certain amount of time to allow threads to execute
    time.sleep(20)

    #all_temperatures = kafka_consumer_manager.get_all_temperatures()
    #for index, temperature in enumerate(all_temperatures):
    #    logger.info(f"Temperature {index + 1}: {temperature}")
    #    for producer in config["producers"]:
    #        producer_id = producer["id"]
    #        producer_type = producer.get("data", {}).get("type")
     #       if producer_type == "docker sensor":
      #          logger.info(f" {producer_id} has temperature: {temperature}")
       #         sensor_info = yaml.dump(producer.get("data", {}), default_flow_style=True)
        #        message = f"Temperature for {producer_id}: {temperature}\nSensor Info:\n{sensor_info}"
#
 #              topic = mqtt_config["topic"].replace("${PRODUCER_ID}", producer_id)
  #             mqtt_manager.publish_message(topic, message)
   #             logger.info(f"Published MQTT message for {producer_id}")
            
    #Stop all docker container
    for docker_manager in docker_managers:
        docker_manager.stop_container()
        logger.info("Temperature generator container stopped")
    


   

    #Wait for Kafka consumer thread to finish
    kafka_consumer_thread.join()

    #Process and publish temperature collected from kafka
    handle_temperatures_and_publish_messages(kafka_consumer_manager, mqtt_manager, mqtt_config, config)

    #Wait for all other threads
    for thread in threads:
        thread.join()
        logger.info(f"Thread {thread.name} finished execution at {time.time() - start_time:.2f} seconds")

    #Disconnect from the MQTT broker
    mqtt_manager.disconnect()
    
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"Main execution finished in {execution_time} seconds")

if __name__ == "__main__":
    main()
