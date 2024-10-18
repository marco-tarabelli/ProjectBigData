

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

## Architecture

The architecture is divided into several key components:

1. **Engine**: The core simulation engine that reads input configurations and manages data production.
2. **Input Configuration**: YAML-based configuration file that defines the behavior and characteristics of each simulated sensor (e.g., temperature range, frequency of data production).
3. **Docker Hub**: Docker images for the sensors are fetched from a public repository and instantiated as required.
4. **Kafka**: The engine interfaces with Kafka for publishing and subscribing to sensor data messages.
5. **MQTT Broker**: Data generated by the sensors is sent to an MQTT broker for further processing or real-time monitoring.

### Diagram

The following diagram illustrates the system's architecture:

![Screenshot (657)](https://github.com/user-attachments/assets/70aaa7da-1019-43cf-9dd6-ddf3e26de7d5)


## Input Configuration

The configuration file defines how the sensors behave and how the data is generated. A sample configuration may look like this:

```yaml
producers:
  - id: sens_temp_1
    data:
      name: TM-873
      type: temperature sensor
      fw_version: 1.0.0v1
      temperature:
        type: "@docker"
        image: iofog/temperature-sensor-simulator
        input:
          min-temp: 10.1
          max-temp: 25.5
        retain-value: true
```

This configuration defines two temperature sensors, specifying their data ranges, Docker images to simulate their behavior, and additional parameters.

## Installation

### Prerequisites

To run the **Data Simulator**, you will need the following:

- Docker
- Kafka
- Python 3.x
- MQTT Broker (e.g., Mosquitto)

### Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/your-repo/data-simulator.git
   cd data-simulator
   ```

2. **Install Dependencies**

   Install required Python libraries:

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Engine**

   Start the simulation engine by providing the input configuration file:

   ```bash
   python simulator.py --config input_config.yaml
   ```

4. **Set Up Kafka and MQTT**

   - Configure Kafka and MQTT to listen for incoming data from the sensors.
   - Ensure Docker is running to instantiate the sensor containers.

## Usage

Once installed, you can:

- Simulate various types of IoT sensors using predefined YAML configurations.
- Modify the configuration files to simulate different types of sensor data (e.g., temperature, humidity).
- Send data to MQTT brokers and view real-time sensor data.

## Example

Here’s an example of running the simulator for a temperature sensor:

```bash
python simulator.py --config input_config.yaml
```

This will start the simulation based on the input configuration provided, generating temperature data and publishing it to the MQTT broker.

## Contributing

We welcome contributions! To contribute:

1. Fork the repository
2. Create a new branch (`git checkout -b feature-branch`)
3. Commit your changes (`git commit -m 'Add a new feature'`)
4. Push to the branch (`git push origin feature-branch`)
5. Open a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

