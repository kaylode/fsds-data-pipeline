from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SCHEMA_REGISTRY_URL

# Avro schema for the message value
USER_SCHEMA_STR = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id",   "type": "int"},
    {"name": "name", "type": "string"}
  ]
}
"""

# Schema Registry client registers/fetches schemas and assigns schema IDs.
# The AvroSerializer embeds the schema ID (5 bytes magic header) in every
# message so consumers can deserialize each message.
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_serializer = AvroSerializer(schema_registry_client, USER_SCHEMA_STR)

# Define the Kafka producer with Avro serialization
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")


msg = {"id": 1, "name": "Alice"}

producer.produce(
    topic=KAFKA_TOPIC,
    value=avro_serializer(msg, SerializationContext(KAFKA_TOPIC, MessageField.VALUE)),
    on_delivery=delivery_report,
)
producer.flush()
