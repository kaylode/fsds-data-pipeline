from confluent_kafka import Producer
import json

from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# Define the Kafka producer with JSON serialization
# confluent_kafka uses a config dict and produce() instead of KafkaProducer + send()
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


# Send a test message to the Kafka topic
# The value option converts Python objects to bytes (the format Kafka messages are in).
msg = {"id": 1, "name": "Alice"}
producer.produce(
    topic=KAFKA_TOPIC,
    value=json.dumps(msg).encode("utf-8"),
    on_delivery=delivery_report,
)
# flush() blocks until all messages are delivered
producer.flush()
print(f"Sent: {msg}")
