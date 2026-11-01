"""Читає вихідний топік з алертами та виводить їх на екран. Зупинка — Ctrl+C."""
import json

from kafka import KafkaConsumer

from configs import MY_NAME, TOPIC_AVG_ALERTS, connection_params

consumer = KafkaConsumer(
    TOPIC_AVG_ALERTS,
    **connection_params(),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_avg_alerts_reader",
)

print(f"Listening to '{TOPIC_AVG_ALERTS}'...")
try:
    for message in consumer:
        print(json.dumps(message.value, ensure_ascii=False))
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()
