from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SCHEMA_REGISTRY_URL

# Schema Evolution: the producer originally wrote messages with this schema (v1):
#   {"id": int, "name": string}
#
# The consumer now uses a newer reader schema (v2) that adds an "email" field
# with a default value. Avro's reader/writer schema resolution means:
#   - Old messages (no "email") → email defaults to "unknown@example.com"
#   - New messages (with "email") → email is read as-is
#
# The key rule for backwards-compatible evolution: new fields must have defaults.
READER_SCHEMA_STR = """
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id",    "type": "int"},
    {"name": "name",  "type": "string"},
    {"name": "email", "type": "string", "default": "unknown@example.com"}
  ]
}
"""

# Passing the reader schema tells AvroDeserializer to project each message
# into this shape, regardless of the writer schema version stored in Schema Registry.
schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_deserializer = AvroDeserializer(schema_registry_client, READER_SCHEMA_STR)

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
    # Old messages will show email="unknown@example.com" (default applied)
    # New messages will show the actual email value
    print(f"Received: {value}")
    # consumer.commit()

# Note that we also have other options for schema evolution, 
# such as using a null default for new fields,
# or using a separate compatibility strategy in Schema Registry. 
# The key is to ensure that the reader schema can handle all versions of the writer schema that may be encountered.
# Compatibility strategies include:
# - Backwards compatibility: new schemas (v2) can read old data (v1), but not vice versa.
# - Forwards compatibility: old schemas (v1) can read new data (v2), but not vice versa.
# - Full compatibility: new and old schemas can read each other's data.