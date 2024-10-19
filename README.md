

---

# Data Simulator

## Overview

The **Data Simulator** project aims to simulate realistic IoT data, capturing patterns that reflect actual phenomena. For example, when simulating an IoT temperature sensor, the data should show values that follow regular temperature fluctuations over time. The simulator generates these data streams and sends them to an IoT broker, such as MQTT, for further processing.

## Project Objective

The primary goal is to develop a **data-simulation engine** that reads from an **input configuration** file and generates data messages. These messages can then be sent to a broker like MQTT or consumed by a system that manages IoT sensors or devices.

## Features

- **Configurable Input**: The engine reads sensor data configuration from a YAML file, allowing flexible and customizable simulation of various sensors.
- **Docker Integration**: Docker containers are used to simulate the sensors, facilitating scalable deployment.
- **MQTT Broker Communication**: Simulated sensor data is sent via MQTT, a popular messaging protocol for IoT.
- **Kafka Integration**: The system also supports Kafka for message brokering and ensures scalable data ingestion and processing.
- **Realistic Sensor Behavior**: The simulator reproduces realistic data behavior, such as fluctuating temperature readings from IoT sensors.

## Components

### Input Configuration
The configuration is written in **YAML** format and specifies the behavior of the sensors and the connections between the services.

```yaml
description: a description   # This must change for every run
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

### Engine
The engine handles:

- Reading the configuration file.
- Instantiating Docker images.
- Passing input to containers via Kafka and receiving output.
- Publishing the output to MQTT topics.

### Docker Compose
Docker Compose is used to orchestrate the Kafka and Zookeeper services, necessary for handling the communication between containers and the simulator.

#### `docker-compose.yml`

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

### Sensor Simulation

#### `image.py`

```python
import os
import json
import logging
import random
import time
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TemperatureGenerator:
    def __init__(self, kafka_broker, input_topic, output_topic):
        self.kafka_broker = kafka_broker
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.producer = KafkaProducer(bootstrap_servers=kafka_broker)

    def generate_random_temperature(self, min_val, max_val):
        return round(random.uniform(min_val, max_val), 2)

    def send_data_to_kafka(self, topic, data):
        message_data = json.dumps(data).encode('utf-8')
        self.producer.send(topic, value=message_data)
        self.producer.flush()

    def generate_and_publish_temperature_range(self):
        min_temp = random.randint(10, 20)
        max_temp = random.randint(min_temp + 1, 30)
        temperature_range = {"min_temp": min_temp, "max_temp": max_temp}
        logger.info(f"Publishing temperature range: {temperature_range}")
        self.send_data_to_kafka(self.input_topic, temperature_range)
        return temperature_range

    def generate_and_publish_temperature(self, min_temp, max_temp):
        temperature = self.generate_random_temperature(min_temp, max_temp)
        logger.info(f"Generated random temperature: {temperature}")
        self.send_data_to_kafka(self.output_topic, {"temperature": temperature})

    def start_temperature_generation(self, delay=5):
        logger.info("Starting temperature generation process...")

        while True:
            try:
                temperature_range = self.generate_and_publish_temperature_range()
                min_temp = temperature_range["min_temp"]
                max_temp = temperature_range["max_temp"]

                self.generate_and_publish_temperature(min_temp, max_temp)
                time.sleep(delay)

            except Exception as e:
                logger.error(f"Error in temperature generation process: {e}")


if __name__ == "__main__":
    kafka_broker = os.environ.get('KAFKA_BROKER', 'broker:9092')
    input_topic = os.environ.get('INPUT_TOPIC')
    output_topic = os.environ.get('OUTPUT_TOPIC')

    logger.info(f"INPUT_TOPIC: {input_topic}")
    logger.info(f"OUTPUT_TOPIC: {output_topic}")

    if not input_topic or not output_topic:
        raise ValueError("INPUT_TOPIC and OUTPUT_TOPIC must be provided as environment variables")

    temperature_generator = TemperatureGenerator(kafka_broker, input_topic, output_topic)
    logger.info("Temperature generator started.")
    temperature_generator.start_temperature_generation(delay=5)
```

## Installation

### Prerequisites

To run the **Data Simulator**, you will need the following:

- Docker
- Docker Compose
- Kafka (inside Docker)
- Python 3.x
- MQTT Broker (e.g., Mosquitto)

### Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/your-repo/data-simulator.git
   cd data-simulator
   ```

2. **Build the Docker Image**

   ```bash
   docker build -t sensor-simulator .
   ```

3. **Build and Start Docker Compose**

   ```bash
   docker-compose up --build
   ```

4. **Create Kafka Topics**

   ```bash
   docker exec -it kafka-container bash
   kafka-topics --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

5. **Start the MQTT Broker**

   Ensure the MQTT broker (e.g., Mosquitto) is running and listening on the appropriate port.

6. **Run the Simulation Engine**

   ```bash
   python simulator.py --config input_config.yaml
   ```

---




