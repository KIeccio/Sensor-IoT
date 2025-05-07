from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def gerar_dados_sensor():
    return {
        'sensor_id': fake.uuid4(), # UUUID Unico (Identificador Único Universal)
        'temperatura': round(random.uniform(15, 40), 2), # valor float aleatório (15–40 °C)
        'umidade': round(random.uniform(30, 90), 2), # valor float aleatório (30–90%)
        'timestamp': fake.iso8601() #  data e hora ISO 8601S
    }

# Loop infinito (while True) gera e envia dados a cada 2 segundos.
# Dados são serializados com JSON (json.dumps) e enviados ao tópico topico_sensores.

while True:
    dado = gerar_dados_sensor()
    print(f"Enviando: {dado}")
    producer.send('topico_sensores', value=dado)
    time.sleep(2) # Enviando a cada 2 segundos