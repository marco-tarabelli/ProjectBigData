

from kafka import KafkaConsumer

import yaml

import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod


# ConfigLoader class: loads configuration from a YAML file
class ConfigLoader:
    # Method: loads configuration from a YAML file
    @staticmethod
    def load_config(filename='BigDataProject/config.yaml'):
        # Load configuration from a YAML file
        with open(filename, 'r') as file:
            config = yaml.safe_load(file)
        print("Loaded config:", config)

        # Extract min_temp and max_temp based on producers
        min_temp = None
        max_temp = None
        docker_config = config.get('docker_config', {})

        for producer in config.get('producers', []):
            if producer['id'] == 'sens_temp_1':
                data = producer.get('data', {})
                temperature_str = data.get('temperature', '')
                if "$_random.uniform" in temperature_str:
                    min_temp, max_temp = map(float, temperature_str.strip('$_random.uniform()').split(','))

        # Read 'frequency' and 'frequency_count' from outputs
        frequency = config['outputs'][0].get('frequency', 1000) / 1000  # in seconds
        frequency_count = config['outputs'][0].get('frequency_count', 10)

        return {
            'min_temp': min_temp,
            'max_temp': max_temp,
            'docker_config': docker_config,
            'frequency': frequency,
            'frequency_count': frequency_count,
            'outputs': config.get('outputs', []),
            'producers': config.get('producers', [])
        }
