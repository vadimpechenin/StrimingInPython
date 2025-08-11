"""
Producer Отвечает за подачу сообщений
Простой Producer на Python с отправкой и чтением json.
"""

import json
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Ошибка доставки сообщения: {err}')
    else:
        print(f'Сообщение доставлено в {msg.topic()} [{msg.partition()}]')

topic = 'json_topic'

data = {'name': 'Alice', 'age': 30}

producer.produce(topic, key='user1', value=json.dumps(data), callback=delivery_report)
producer.poll(0)
producer.flush()