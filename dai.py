import yaml
import uuid
import random
import paho.mqtt.client as mqtt
import docker
from kafka import KafkaProducer, KafkaConsumer
import json


class DockerManager:
    def __init__(self):
        self.client = docker.from_env()

    def run_container(self, image, min_temp, max_temp, retain_value, keep_alive_ms):
        environment = {
            'MIN_TEMP': str(min_temp),
            'MAX_TEMP': str(max_temp),
            'RETAIN_VALUE': str(retain_value),
            'KEEP_ALIVE_MS': str(keep_alive_ms)
            #'KAFKA_BOOTSTRAP_SERVERS': kafka_bootstrap_servers
        }
        container = self.client.containers.run(image, detach=True, environment=environment)
        return container.id



class MQTTPublisher:
    def __init__(self, host, port):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
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
#host = 'your_mqtt_broker_host'
#port = 1883

# Create MQTTPublisher instance
#mqtt_publisher = MQTTPublisher(host, port)

# Example usage
#mqtt_publisher.publish('mytopic', 'Hello, MQTT!')

# Don't forget to disconnect from the MQTT broker
#mqtt_publisher.disconnect()
    
class Loader:
    
    @staticmethod
    def read_config(file_yaml):
        with open("configuration.yaml", "r") as file:
            configurazione = yaml.safe_load(file)
        return configurazione

    @staticmethod
    def genera_uuid():
        return str(uuid.uuid4())
    


class Simulation:
    
    def __init__(self, configurazione):
        #self.configurazione = configurazione
        # Define host and port
        #mqtt_host = 'localhost'  # Inserisci l'indirizzo IP del server Mosquitto
        #mqtt_port = 1883  # Inserisci la porta su cui Mosquitto è in ascolto
        self.configurazione = configurazione
        # Leggi host e porta dalla configurazione
        mqtt_host = self.configurazione['outputs'][0]['host']
        mqtt_port = self.configurazione['outputs'][0]['port']
        self.mqtt_publisher = MQTTPublisher(mqtt_host, mqtt_port)
        

    def genera_temperatura_casuale(self, min_val, max_val):
        return round(random.uniform(min_val, max_val), 2)

    def esegui_simulazione(self):
        id_simulazione = Loader.genera_uuid()
        self.configurazione['id'] = id_simulazione

        for produttore in self.configurazione['producers']:
            if produttore['id'] == 'sens_temp_1':
                min_temp = 10.1
                max_temp = 25.5
                temperatura_casuale = self.genera_temperatura_casuale(min_temp, max_temp)
                produttore['data']['temperature'] = temperatura_casuale

            elif produttore['id'] == 'sens_temp_2':
                image = produttore['data']['temperature']['image']
                min_temp = produttore['data']['temperature']['input']['min-temp']
                max_temp = produttore['data']['temperature']['input']['max-temp']
                retain_value = produttore['data']['temperature']['input']['retain-value']
                keep_alive_ms = produttore['data']['temperature']['config']['keep-alive-ms']
                
                container_id = DockerManager().run_container(image, min_temp, max_temp, retain_value, keep_alive_ms)
                print("Container Docker avviato con ID:", container_id)        

    def stampa_configurazione(self):
        print("ID della simulazione:", self.configurazione['id'])
        print("Descrizione:", self.configurazione['description'])

        for output in self.configurazione['outputs']:
            print("Tipo di output:", output['type'])
            print("Host:", output['host'])
            print("Porta:", output['port'])
            print("Topic:", output['topic'])

        #for produttore in self.configurazione['producers']:
         #   print("ID del produttore:", produttore['id'])
          #  print("Dati del produttore:", produttore['data'])
        for produttore in self.configurazione['producers']:
            topic = f"mytopic/{produttore['id']}"
            message = f"ID della simulazione: {self.configurazione['id']}\n"
            message += f"Descrizione: {self.configurazione['description']}\n"
            message += f"ID del produttore: {produttore['id']}\n"
            message += f"Dati del produttore: {produttore['data']}\n"
            #self.mqtt_publisher.publish(topic, message)

    def avvia_container_docker(self):
        for produttore in self.configurazione['producers']:
            if produttore['id'] == 'sens_temp_2':
                image = produttore['data']['temperature']['image']
                min_temp = produttore['data']['temperature']['input']['min-temp']
                max_temp = produttore['data']['temperature']['input']['max-temp']
                retain_value = produttore['data']['temperature']['input']['retain-value']
                keep_alive_ms = produttore['data']['temperature']['config']['keep-alive-ms']
                container_id = DockerManager().run_container(image, min_temp, max_temp, retain_value, keep_alive_ms)
                print("Container Docker avviato con ID:", container_id)
        

    def publish_output_to_mqtt(self):
        for produttore in self.configurazione['producers']:
            if produttore['id'] == 'sens_temp_1' or produttore['id'] == 'sens_temp_2':
                #topic = self.configurazione['outputs'][0]['topic']  # Assume che ci sia solo un output MQTT definito nella configurazione
                topic = f"mytopic/{produttore['id']}"
                message = f"ID della simulazione: {self.configurazione['id']}\n"
                message += f"Descrizione: {self.configurazione['description']}\n"
                message += f"ID del produttore: {produttore['id']}\n"
                message += f"Dati del produttore: {produttore['data']}\n"
                self.mqtt_publisher.publish(topic, message)


    def clean_up(self):
        self.mqtt_publisher.disconnect()        


# Utilizzo delle classi e delle funzioni
configurazione = Loader.read_config('configuration.yaml')
simulatore = Simulation(configurazione)
simulatore.esegui_simulazione()
simulatore.stampa_configurazione()     
simulatore.publish_output_to_mqtt()