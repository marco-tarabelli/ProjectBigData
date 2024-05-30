import docker
import yaml
import os
import time
from kafka import KafkaConsumer
#NON FUNZIONA
def start_docker_container(config):
    client = docker.from_env()
    image = config.get('image','')
    
    # Imposta le variabili d'ambiente per il container Docker
    environment = {
        'KAFKA_BROKER': config.get('kafka_broker', ''),  # Accedi in modo sicuro alla chiave 'kafka_broker'
        'INPUT_TOPIC': config.get('input_topic', ''),  # Accedi in modo sicuro alla chiave 'input_topic'
        'OUTPUT_TOPIC': config.get('output_topic', '')  # Accedi in modo sicuro alla chiave 'output_topic'
    }
    
    container = client.containers.run(image, detach=True, environment=environment, network="rmoff_kafka", command="python ./main.py")
    return container

def stop_docker_container(container):
    container.stop()
    container.remove()

def read_kafka_output(output_topic):
    consumer = KafkaConsumer(output_topic)
    for message in consumer:
        print(f"Received message: {message.value.decode('utf-8')}")

def main():
    with open('config.yaml', 'r') as file:
        config_data = yaml.safe_load(file)
        
    producers = config_data.get('producers', [])
    for producer in producers:
        print("Producer from YAML:", producer)
        if 'data' in producer and 'temperature' in producer['data']:
            temperature = producer['data']['temperature']
            if isinstance(temperature, dict) and temperature.get('type') == '@docker':
                config = producer.get('config', {})
                if not config:
                    print("Config not found for", producer['id'])
                    continue

                container = start_docker_container(config)
                print(f"Started Docker container '{container.name}' for producer {producer['id']}")
                time.sleep(config.get('keep-alive-ms', 10000) / 1000)
                stop_docker_container(container)
                print(f"Stopped Docker container '{container.name}' for producer {producer['id']}")
                
                # Stampiamo i valori presenti nel topic di output
                output_topic = config.get('output_topic')
                if output_topic:
                    print(f"Reading messages from {output_topic}:")
                    read_kafka_output(output_topic)
            else:
                print(f"Producer {producer['id']} does not require a Docker container.")
        else:
            print(f"No temperature data found for producer {producer['id']}")

if __name__ == "__main__":
    main()