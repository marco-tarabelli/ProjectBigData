from ConfigLoader import ConfigLoader
from DockerManager import DockerManager
from KafkaTemperatureConsumer import KafkaTemperatureConsumer
from DockerSensor import DockerSensor
from TemperatureController import TemperatureController
from MqttManager import MqttManager
from SensorManager import SensorManager
from TemperatureSensor import TemperatureSensor


# Application class: main application class that sets up and runs the temperature sensors
class Application:
    # Constructor: initializes the application
    def __init__(self):
        # Load configuration from file
        self.config = ConfigLoader.load_config()

        # Set up Docker and Kafka environment from the configuration
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

        # Create and add sensors from configuration
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
                        config=producer['data']
                    )
                )

        # Handle infinite iteration if frequency_count is -1
        iterations = self.config['frequency_count']
        if iterations == -1:
            iterations = float('inf')  # Infinite iterations

        # Set up the controller
        self.controller = TemperatureController(
            sensor_manager=self.sensor_manager,
            iterations=iterations,
            config=self.config
        )
        
    # Method: runs the application
    def run(self):
        # Run the application
        self.controller.run()

if __name__ == "__main__":
    app = Application()
    app.run()
