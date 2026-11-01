# Налаштування підключення до Kafka.
kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Ідентифікатор, який додається до імен топіків, щоб вони не дублювалися
MY_NAME = "polina_krashchenko"

TOPIC_SENSORS = f"{MY_NAME}_building_sensors"
TOPIC_TEMPERATURE_ALERTS = f"{MY_NAME}_temperature_alerts"
TOPIC_HUMIDITY_ALERTS = f"{MY_NAME}_humidity_alerts"
TOPIC_AVG_ALERTS = f"{MY_NAME}_avg_alerts"


def connection_params():
    """Параметри підключення для KafkaProducer / KafkaConsumer / KafkaAdminClient."""
    return {
        "bootstrap_servers": kafka_config["bootstrap_servers"],
        "security_protocol": kafka_config["security_protocol"],
        "sasl_mechanism": kafka_config["sasl_mechanism"],
        "sasl_plain_username": kafka_config["username"],
        "sasl_plain_password": kafka_config["password"],
    }
