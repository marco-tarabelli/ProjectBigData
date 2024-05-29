import subprocess
import yaml
import uuid
import random
import paho.mqtt.client as mqtt
import docker
import logging
import time
import os

# Configura il logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DockerManager:
    def __init__(self):
        self.client = docker.from_env()

    def run_container(self, params):
        # Avvia il container Docker con i parametri desiderati
        os.environ['SELFNAME'] = 'sesamo_container'
        logger.info("Avvio del container Docker...")
        image = params['image']
        retain_value = params.get('retain_value')
        input_topic = params['config']['input_topic']
        output_topic = params['config']['output_topic']
        keep_alive_ms = params['config']['keep_alive_ms']
        network = params.get('network')

        environment = {
            'INPUT_TOPIC': input_topic,
            'OUTPUT_TOPIC': output_topic,
            'RETAIN_VALUE': str(retain_value),
            'KEEP_ALIVE_MS': str(keep_alive_ms)
        }

        try:
            container = self.client.containers.run(
                image,
                detach=True,
                environment=environment,
                network=network  # Specifica la rete Docker
            )
            logger.info("Container Docker avviato con ID: {}".format(container.id))

            # Attendi keep_alive_ms millisecondi (convertiti in secondi) prima di controllare lo stato del container
            time.sleep(keep_alive_ms / 1000)

            # Controllo dello stato del container dopo keep_alive_ms dall'avvio
            container.reload()  # Ricarica le informazioni sul container
            if container.status == 'running':
                logger.info("Il container è in esecuzione correttamente dopo {} millisecondi.".format(keep_alive_ms))
            else:
                logger.error("Il container non è in esecuzione dopo {} millisecondi.".format(keep_alive_ms))

            return container.id
        except docker.errors.APIError as e:
            logger.error("Errore durante l'avvio del container Docker: {}".format(e))
            return None

class MQTTPublisher:
    def __init__(self, host, port):
        self.client = mqtt.Client()
        self.client.connect(host, port)
        self.client.loop_start()

    def publish(self, topic, message):
        result = self.client.publish(topic, message, qos=1)  # Imposta il livello di QoS a 1 per abilitare la conferma di pubblicazione
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print("Messaggio pubblicato correttamente su", topic)
        else:
            print("Errore durante la pubblicazione del messaggio su", topic)

    def disconnect(self):
        self.client.disconnect()
        self.client.loop_stop()

# Define host and port
host = 'localhost'
port = 1883

# Create MQTTPublisher instance
mqtt_publisher = MQTTPublisher(host, port)

# Example usage
mqtt_publisher.publish('mytopic', 'Hello, MQTT!')

# Don't forget to disconnect from the MQTT broker
mqtt_publisher.disconnect()

class Loader:
    @staticmethod
    def read_config(file_yaml):
        with open("finalProject/config.yaml", "r") as file:
            configurazione = yaml.safe_load(file)
        return configurazione

    @staticmethod
    def genera_uuid():
        return str(uuid.uuid4())

class Simulation:
    def __init__(self, configurazione):
        self.configurazione = configurazione
        mqtt_host = 'localhost'  # Inserisci l'indirizzo IP del server Mosquitto
        mqtt_port = 1883  # Inserisci la porta su cui Mosquitto è in ascolto
        self.docker_manager = DockerManager()
        self.mqtt_publisher = MQTTPublisher(mqtt_host, mqtt_port)

    def genera_temperatura_casuale(self, min_val, max_val):
        return round(random.uniform(min_val, max_val), 2)

    def publish_output_to_mqtt(self):
        # Implementazione del metodo per pubblicare l'output su MQTT
        for output in self.configurazione['outputs']:
            topic = output['topic']
            message = "Messaggio di output da pubblicare su MQTT"
            self.mqtt_publisher.publish(topic, message)

    def get_simulated_temperature(self):
        container_name = 'temperature-sensor'  # Modifica con il nome corretto del container Docker
        temperature_file_path = '/path/to/temperature_file'  # Modifica con il percorso corretto del file di temperatura nel container Docker

        command = f"docker exec {container_name} cat {temperature_file_path}"
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            try:
                temperature = float(result.stdout.strip())
                return temperature
            except ValueError:
                logging.error("Impossibile convertire la temperatura in float")
                return None
        else:
            logging.error("Errore durante l'esecuzione del comando per ottenere la temperatura")
            return None

    def stampa_configurazione(self):
        print("ID della simulazione:", self.configurazione['id'])
        print("Descrizione:", self.configurazione['description'])

        for output in self.configurazione['outputs']:
            print("Tipo di output:", output['type'])
            print("Host:", output['host'])
            print("Porta:", output['port'])
            print("Topic:", output['topic'])

        for produttore in self.configurazione['producers']:
            topic = f"mytopic/{produttore['id']}"
            message = f"ID della simulazione: {self.configurazione['id']}\n"
            message += f"Descrizione: {self.configurazione['description']}\n"
            message += f"ID del produttore: {produttore['id']}\n"
            message += f"Dati del produttore: {produttore['data']}\n"

            # Pubblica il messaggio sul canale MQTT
            self.mqtt_publisher.publish(topic, message)
            # Aggiungi la temperatura al messaggio
            if produttore['id'] == 'sens_temp_2':
                temperatura = produttore['data']['temperature']
                message += f"Temperatura: {temperatura}\n"

    def esegui_simulazione(self):
        id_simulazione = Loader.genera_uuid()
        self.configurazione['id'] = id_simulazione

        for produttore in self.configurazione['producers']:
            if produttore['id'] == 'sens_temp_1':
                min_temp = produttore['data']['temperature']['min-temp']
                max_temp = produttore['data']['temperature']['max-temp']
                temperatura_casuale = self.genera_temperatura_casuale(min_temp, max_temp)
                produttore['data']['temperature'] = temperatura_casuale
                topic = self.configurazione['outputs'][0]['topic']
                message = {
                    'ID della simulazione': self.configurazione['id'],
                    'Descrizione': self.configurazione['description'],
                    'ID del produttore': produttore['id'],
                    'Dati del produttore': produttore['data']
                }
                self.mqtt_publisher.publish(topic, str(message))
            elif produttore['id'] == 'sens_temp_2':
                params = {
                    'image': produttore['data']['temperature']['image'],
                    'input_topic': 'input_topic_for_container',
                    'output_topic': 'output_topic_from_container',
                    'retain_value': produttore['data']['temperature']['retain_value'],
                    'keep_alive_ms': produttore['config']['keep_alive_ms'],
                    'network': produttore['config']['network']  # Specifica il nome della rete Docker
                }
                self.docker_manager.run_container(params)

    def clean_up(self):
        self.mqtt_publisher.disconnect()

# Utilizzo delle classi e delle funzioni
configurazione = Loader.read_config('finalProject/config.yaml')
simulatore = Simulation(configurazione)
simulatore.esegui_simulazione()
simulatore.stampa_configurazione()
simulatore.publish_output_to_mqtt()
simulatore.clean_up()
