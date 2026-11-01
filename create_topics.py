"""Створення топіків для ДЗ6: вхідний building_sensors (з ДЗ5) і вихідний avg_alerts."""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import MY_NAME, TOPIC_AVG_ALERTS, TOPIC_SENSORS, connection_params

admin_client = KafkaAdminClient(**connection_params())

for topic in [NewTopic(name=TOPIC_SENSORS, num_partitions=2, replication_factor=1),
              NewTopic(name=TOPIC_AVG_ALERTS, num_partitions=1, replication_factor=1)]:
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic.name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic.name}' already exists.")
    except Exception as e:
        print(f"An error occurred while creating '{topic.name}': {e}")

print("\nMy topics:")
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()
