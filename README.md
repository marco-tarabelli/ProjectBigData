

---

# Sensor Simulation Project

## Overview

This project simulates temperature sensors and Docker-based sensors, generating random temperature data and publishing it to Kafka topics and MQTT brokers. The system uses Docker containers to simulate a real-world environment with a Kafka message broker, a Zookeeper coordinator, and MQTT messaging.

The application is designed to:
- Load sensor configuration from a YAML file.
- Set up Docker containers for Kafka and Zookeeper.
- Manage sensors using Python classes.
- Publish temperature data to Kafka topics and MQTT brokers.

## Prerequisites

Before running the project, ensure you have installed the following:

- Docker
- Docker Compose
- Python 3.x
- Required Python packages (`kafka-python`, `paho-mqtt`, `PyYAML`, etc.)

## Running the Project

### Steps to Run

1. **Build Docker Image**:
   To build the required Docker image, use the following command:
   ```bash
   docker build -t finalimage .
   ```

2. **Set Up Docker Compose**:
   Build and start the containers using Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. **Create Kafka Topics**:
   Kafka topics must be created before starting the application. You can do this by entering the Kafka broker container and running the following commands:
   ```bash
   docker exec -it broker /bin/bash
   kafka-topics --create --topic temperature_input --bootstrap-server broker:9092 --partitions 1 --replication-factor 1
   kafka-topics --create --topic temperature_output --bootstrap-server broker:9092 --partitions 1 --replication-factor 1
   ```

4. **Start MQTT Broker**:
   Ensure your MQTT broker is running on the default port (1883). You can use a service like Mosquitto.

5. **Run the Application**:
   Once everything is set up, run the application:
   ```bash
   python application.py
   ```

6. **Listen to MQTT Topics**:
   Use an MQTT client to subscribe to the right topic (e.g., `/mytopic/sens_temp_1`), and you will start receiving the sensor data.

## Project Structure

### Configuration

The configuration is managed using a `config.yaml` file.

#### config.yaml

```yaml
description: a description   # This should be changed for each run

outputs:
  - type: mqtt  
    host: localhost
    port: 1883
    topic: "/mytopic/${PRODUCER_ID}" 
    frequency: 2
    frequency_count: 500

docker_config:
  image: finalimage
  docker_net: bigdataproject_bigdata_net
  kafka_broker: "broker:9092"
  input_topic: "temperature_input"
  output_topic: "temperature_output"
  keep-alive-ms: 1
  status: "OK"

producers:
  - id: sens_temp_1
    type: "temperature_sensor"
    data:
      name: "TM-873"
      fw_version: "1.0.0v1"
      temperature: "$_random.uniform(10.1, 25.5)"
      status: "OK"
  
  - id: sens_temp_2
    type: "docker_sensor"
    data:
      name: "TM-874"
      fw_version: "1.0.0v1"
      status: "OK"
```

This file defines the outputs, Docker configuration, and sensor details, such as their IDs and types.

### Docker Compose

#### docker-compose.yml

```yaml
version: '3.7'

networks:
  bigdata_net:
    driver: bridge

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.0.1
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    networks:
      - bigdata_net

  broker:
    image: confluentinc/cp-kafka:7.0.1
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "19092:19092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:9092,CONNECTIONS_FROM_HOST://localhost:19092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONNECTIONS_FROM_HOST:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://broker:9092,CONNECTIONS_FROM_HOST://0.0.0.0:19092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks:
      - bigdata_net
```

### Application Code

#### application.py

The main entry point of the project is the `application.py` file. The `Application` class loads the configuration, initializes Docker containers, and sets up Kafka consumers and MQTT publishers.

```python
from ConfigLoader import ConfigLoader
from DockerManager import DockerManager
from KafkaTemperatureConsumer import KafkaTemperatureConsumer
from DockerSensor import DockerSensor
from TemperatureController import TemperatureController
from MqttManager import MqttManager
from SensorManager import SensorManager
from TemperatureSensor import TemperatureSensor

class Application:
    def __init__(self):
        self.config = ConfigLoader.load_config()

        self.docker_manager = DockerManager(docker_config=self.config['docker_config'])
        self.kafka_consumer = KafkaTemperatureConsumer(
            topic=self.config['docker_config'].get('output_topic', 'default_topic'),
            bootstrap_servers=['localhost:19092'],
            group_id='temperature_reader'
        )
        self.mqtt_manager = MqttManager(
            host=self.config['outputs'][0]['host'],
            port=self.config['outputs'][0]['port'],
            topic_format=self.config['outputs'][0]['topic']
        )
        self.sensor_manager = SensorManager()

        for producer in self.config['producers']:
            if producer['type'] == 'temperature_sensor':
                self.sensor_manager.add_sensor(
                    TemperatureSensor(
                        producer_id=producer['id'],
                        min_temp=self.config['min_temp'],
                        max_temp=self.config['max_temp'],
                        mqtt_manager=self.mqtt_manager,
                        config=producer['data']
                    )
                )
            elif producer['type'] == 'docker_sensor':
                self.sensor_manager.add_sensor(
                    DockerSensor(
                        producer_id=producer['id'],
                        docker_manager=self.docker_manager,
                        kafka_consumer=self.kafka_consumer,
                        mqtt_manager=self.mqtt_manager,
                        config=producer['data'],
                        keep_alive_ms=self.config['keep_alive_ms']
                    )
                )

        iterations = self.config['frequency_count']
        if iterations == -1:
            iterations = float('inf')

        self.controller = TemperatureController(
            sensor_manager=self.sensor_manager,
            iterations=iterations,
            config=self.config,
            kafka_consumer=self.kafka_consumer
        )

    def run(self):
        self.controller.run()

if __name__ == "__main__":
    app = Application()
    app.run()
```

---



