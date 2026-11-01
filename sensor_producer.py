"""Крок 2. Імітація одного датчика: періодично відправляє дані в building_sensors.

Один запуск = один датчик. Щоб імітувати кілька датчиків, запустіть скрипт
у кількох терміналах одночасно. Зупинка — Ctrl+C.
"""
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

from configs import TOPIC_SENSORS, connection_params

SEND_INTERVAL_SEC = 2

# ID датчика — випадкове, але постійне в межах одного запуску
sensor_id = random.randint(1000, 9999)

producer = KafkaProducer(
    **connection_params(),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: str(v).encode("utf-8"),
)

print(f"Sensor {sensor_id} started. Sending data to '{TOPIC_SENSORS}'...")

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": round(random.uniform(25, 45), 2),
            "humidity": round(random.uniform(15, 85), 2),
        }
        producer.send(TOPIC_SENSORS, key=sensor_id, value=data)
        producer.flush()
        print(f"Sent: {data}")
        time.sleep(SEND_INTERVAL_SEC)
except KeyboardInterrupt:
    print(f"\nSensor {sensor_id} stopped.")
finally:
    producer.close()
