from kafka import KafkaConsumer
import json
import sqlite3

# Conectando ao bando SQLite

conn = sqlite3.connect('sensores.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sensores (
        sensor_id TEXT,
        temperatura REAL,
        umidade REAL,
        timestamp TEXT
    )
''')
conn.commit()

# Consumidor Kafka
consumer = KafkaConsumer(
    'topico_sensores',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Conecta ao Kafka e consome mensagens do topico_sensores.

# Cada mensagem é desserializada de JSON para dicionário Python.

# Os dados são inseridos na tabela com INSERT INTO sensores (...).

for msg in consumer:
    dado = msg.value
    print("Recebido:", dado)
    cursor.execute('''
        INSERT INTO sensores (sensor_id, temperatura, umidade, timestamp)
        VALUES (?, ?, ?, ?)
    ''', (dado['sensor_id'], dado['temperatura'], dado['umidade'], dado['timestamp']))
    conn.commit()