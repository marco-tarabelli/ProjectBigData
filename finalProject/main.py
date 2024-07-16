import logging
import threading
import time
import yaml
from UltimaProva.TemperatureManager import TemperatureManager
from UltimaProva.DockerStartManager import DockerStartManager
from UltimaProva.KafkaConsumerManager import KafkaConsumerManager
from UltimaProva.MQTTManager import MQTTManager


#Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Main function
def main():
    # timer that tells us the duration of threads execution
    start_time = time.time()

    #Read the configuration file
    def read_configuration(file_path):
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    config = read_configuration("../UltimaProva/config.yaml")
    # set the mqtt variables in order to connect to the server
    mqtt_config = config["outputs"][0]
    mqtt_manager = MQTTManager(host=mqtt_config["host"], port=mqtt_config["port"])
    mqtt_manager.connect()


    #  Define the Docker network to use
    network = "rmoff_kafka"
    docker_managers = []

    # Define the Kafka topic to publish the result of Docker containers
    kafka_topic = "output_topic"
    # start the kafka consumer
    kafka_consumer_manager = KafkaConsumerManager(kafka_topic,mqtt_manager)

    logger.info("Main execution started")

    # Start kafka consumer in a separate thread
    kafka_consumer_thread = threading.Thread(target=kafka_consumer_manager.start_consumer)
    kafka_consumer_thread.start()

    # List to keep track of Docker Manager and temperature generation threads
    threads = []

    # create the temperature manager
    temperature_manager = TemperatureManager(mqtt_manager, mqtt_config, kafka_consumer_manager, config)
    # iterate through the list of producers and obtain the relevant information
    for producer in config["producers"]:
        producer_id = producer["id"]
        producer_type = producer.get("data", {}).get("type")
        logger.info(f"Producer {producer_id} has type: {producer_type}")
        # if prducer_type is equal to docker sensor
        if producer_type == "docker sensor":
            logger.info(f"Found docker sensor producer: {producer_id}")
            docker_manager = DockerStartManager(network=network, environment=producer.get("config", {}))
            docker_thread = threading.Thread(target=docker_manager.start_container, args=(producer.get("config", {}).get("image"),))
            docker_thread.start()
            docker_managers.append(docker_manager)
            threads.append(docker_thread)
            logger.info(f"Attempted to start temperature generator Docker container for {producer_id} at {time.time() - start_time:.2f} seconds")
            
               
        
        
        # Handle logic for MQTT publication of temperature sensor that are not docker sensor
        elif producer_type == "temperature_sensor":
                temp_thread = threading.Thread(target=temperature_manager.generate_and_publish_temperature, args=(producer_id, mqtt_manager, mqtt_config))
                logger.info(f"Starting temperature sensor thread for {producer_id} at {time.time() - start_time:.2f} seconds")
                temp_thread.start()
                threads.append(temp_thread)        
        else:
            logger.info(f"Ignored producer {producer_id}, not a docker or temperature sensor")   

    # Wait a certain amount of time to allow threads to execute
    time.sleep(20)


            
    # Stop all docker container
    for docker_manager in docker_managers:
        docker_manager.stop_container()
        logger.info("Temperature generator container stopped")
    


   

    # Wait for Kafka consumer thread to finish
    kafka_consumer_thread.join()

    # Process and publish temperature collected from kafka
    temperature_manager.handle_temperatures_and_publish_messages(kafka_consumer_manager, mqtt_manager, mqtt_config, config)

    # Wait for all other threads
    for thread in threads:
        thread.join()
        logger.info(f"Thread {thread.name} finished execution at {time.time() - start_time:.2f} seconds")

    time.sleep(20)
    # Disconnect from the MQTT broker after waiting for all threads
    mqtt_manager.disconnect()
    
    end_time = time.time()
    #execution time
    execution_time = end_time - start_time
    logger.info(f"Main execution finished in {execution_time} seconds")

if __name__ == "__main__":
    main()

