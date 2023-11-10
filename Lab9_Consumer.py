from confluent_kafka import Consumer, KafkaError
import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# Configuración del consumidor de Kafka
config = {
    'bootstrap.servers': 'lab9.alumchat.xyz:9092',
    'group.id': 'grupo_consumidor',
    'auto.offset.reset': 'earliest'
}

# Crear el consumidor de Kafka
consumer = Consumer(config)
consumer.subscribe(['20090'])  # Reemplaza '12345' con el nombre real del topic

# Variables para almacenar los datos
all_temp = []
all_hume = []
all_wind = []

wind_directions = {'N': 0, 'NW': 1, 'W': 2, 'SW': 3, 'S': 4, 'SE': 5, 'E': 6, 'NE': 7}

def decode(encoded_message):
    # Convertir los 3 bytes a un solo número entero de 24 bits
    combined = int.from_bytes(encoded_message, byteorder='big')

    # Extraer la temperatura, humedad y dirección del viento
    temperature = (combined >> 10) / 100.0
    humidity = (combined & 0b1111111000) >> 3
    wind_value = combined & 0b111
    wind_directions = {0: 'N', 1: 'NW', 2: 'W', 3: 'SW', 4: 'S', 5: 'SE', 6: 'E', 7: 'NE'}
    wind_direction = wind_directions[wind_value]

    # Construir el JSON
    return {'temperatura': temperature, 'humedad': humidity, 'direccion': wind_direction}

# def procesarMensaje(mensaje):
#     # Convertir el mensaje de bytes a diccionario
#     return json.loads(mensaje.decode('utf-8'))

def plotAllData(all_temp, all_hume, all_wind):
    fig, axs = plt.subplots(3, 1, figsize=(10, 8)) 

    # Graficar la temperatura en la primera subtrama
    axs[0].plot(all_temp, label='Temperatura', color='red')
    axs[0].set_title('Temperatura')
    axs[0].set_ylabel('Grados Celsius')
    axs[0].legend()
    axs[0].grid(True)

    # Graficar la humedad en la segunda subtrama
    axs[1].plot(all_hume, label='Humedad', color='blue')
    axs[1].set_title('Humedad')
    axs[1].set_ylabel('Porcentaje (%)')
    axs[1].legend()
    axs[1].grid(True)

    wind_directions = {'N': 0, 'NW': 1, 'W': 2, 'SW': 3, 'S': 4, 'SE': 5, 'E': 6, 'NE': 7}
    all_wind_numeric = [wind_directions[direction] for direction in all_wind]

    axs[2].plot(all_wind_numeric, label='Dirección del Viento', color='green')
    axs[2].set_title('Dirección del Viento')
    axs[2].set_ylabel('Dirección')
    axs[2].set_yticks(range(len(wind_directions)))
    axs[2].set_yticklabels(list(wind_directions.keys()))
    axs[2].legend()
    axs[2].grid(True)

    # Configurar el eje x compartido
    for ax in axs:
        ax.set_xlabel('Tiempo (s)')

    plt.tight_layout()
    plt.show()

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Fin de la partición alcanzado, pero puede ignorarse
                continue
            else:
                print(msg.error())
                break

        # # Procesar mensaje
        # payload = procesarMensaje(msg.value())

        print("Mensaje recibido: {}".format(msg.value()))

        payload = decode(msg.value())

        print("Mensaje decodificado: {}".format(payload))
        
        all_temp.append(payload['temperatura'])
        all_hume.append(payload['humedad'])
        all_wind.append(payload['direccion'])

        # Graficar los datos
        plotAllData(all_temp, all_hume, all_wind)
        

except KeyboardInterrupt:
    print('Interrupción por el usuario')

finally:
    # Cerrar el consumidor
    consumer.close()