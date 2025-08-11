"""
Consumer c чтением json
"""
import json
from confluent_kafka import Consumer, KafkaException, KafkaError


conf = {
    'bootstrap.servers': 'localhost:9092',  # Список серверов Kafka
    'group.id': 'mygroup',                  # Идентификатор группы потребителей
    'auto.offset.reset': 'earliest'         # Начальная точка чтения ('earliest' или 'latest')
}

consumer = Consumer(conf)

topic = 'json_topic'
consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f'Конец раздела {msg.topic()} [{msg.partition()}]')
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            data = json.loads(msg.value().decode('utf-8'))
            print(f'Получено сообщение: {data}')
finally:
    consumer.close()