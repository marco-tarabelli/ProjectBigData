from kafka import KafkaConsumer, KafkaProducer
import json
import random
import os

def genera_temperatura(min_temp, max_temp):
    return random.uniform(min_temp, max_temp)

def main():
    # Ottieni il nome del topic Kafka di input e di output dall'ambiente
    kafka_input_topic = os.environ.get('KAFKA_INPUT_TOPIC', 'default_input_topic')
    kafka_output_topic = os.environ.get('KAFKA_OUTPUT_TOPIC', 'default_output_topic')

    # Configura il consumatore per leggere i dati di input
    consumer = KafkaConsumer(kafka_input_topic,
                             bootstrap_servers=['localhost:9092'],
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    # Configura il produttore per inviare l'output
    #chatgpt suggeriva kafka:9092 e non localhost
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    for message in consumer:
        data = message.value
        min_temp = data.get('min_temp')
        max_temp = data.get('max_temp')

        # Genera la temperatura
        temperatura = genera_temperatura(min_temp, max_temp)
        print("Temperatura generata:", temperatura)

        # Invia l'output al topic di output
        producer.send(kafka_output_topic, value={"temperatura": temperatura})

if __name__ == "__main__":
    main()
