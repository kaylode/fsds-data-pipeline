from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SCHEMA_REGISTRY_URL

# Avro schema must match what the producer registered
# AvroDeserializer reads the 5-byte magic header to fetch the schema ID from
# Schema Registry, then deserializes the Avro bytes back into a Python dict.
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_deserializer = AvroDeserializer(schema_registry_client)

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "my-group",
    "auto.offset.reset": "earliest",
})

consumer.subscribe([KAFKA_TOPIC])

# Continuously listen for new messages and print them as they arrive
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    value = avro_deserializer(msg.value(), SerializationContext(KAFKA_TOPIC, MessageField.VALUE))
    print(f"Received: {value}")
    # consumer.commit()
