import random
import numpy as np
from confluent_kafka import Producer
import time
import json


def generateTemperatura():
    # Distribucion uniforme gaussiana con media de 50
    mean = 50
    std_dev = 15

    temperatura = np.random.normal(mean, std_dev)
    return round(np.clip(temperatura, 0, 100), 2)

def generateHumedadRelativa():
    mean = 50
    std_dev = 15

    humedad = np.random.normal(mean, std_dev)
    return int(np.clip(humedad, 0, 100))

def generateDireccionDelViento():
    direcciones = ["N", "NW", "W", "SW", "S", "SE", "E", "NE"]
    return direcciones[random.randint(0, 7)]

def generarData():
    return {
        "temperatura": generateTemperatura(),
        "humedad": generateHumedadRelativa(),
        "direccion": generateDireccionDelViento()
    }

def encode(temperature, humidity, wind_direction):
    # Asumiendo que la temperatura se redondea a la décima más cercana
    temperature = int(temperature * 100)
    wind_directions = {'N': 0, 'NW': 1, 'W': 2, 'SW': 3, 'S': 4, 'SE': 5, 'E': 6, 'NE': 7}
    wind_value = wind_directions[wind_direction]

    # Combinar los valores en un solo número entero de 24 bits
    combined = (temperature << 10) | (humidity << 3) | wind_value
    
    # Convertir ese número entero a 3 bytes
    return combined.to_bytes(3, byteorder='big')

# Configuración del productor de Kafka
config = {
    'bootstrap.servers': 'lab9.alumchat.xyz:9092',
}

# Crear el productor de Kafka
producer = Producer(config)


# Función para enviar mensajes
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Mensaje codificado enviado {} [{}]'.format(msg.topic(), msg.value()))

topic = '20090'

try:
    while True:
        # Generar datos
        data = generarData()
        # # Convertir datos a string JSON
        # data_str = json.dumps(data)


        encoded_data = encode(data['temperatura'], data['humedad'], data['direccion'])
        print("Enviando datos: {}".format(data))

        # Enviar datos
        producer.produce(topic = topic, key = "sensor1", value=encoded_data, callback=delivery_report)
        # Esperar entre 15 y 30 segundos
        time.sleep(np.random.randint(15, 31))
        # Procesar los mensajes en cola
        producer.poll(0)

except KeyboardInterrupt:
    print("Interrupción por el usuario, terminando la producción de datos...")

finally:
    producer.flush()