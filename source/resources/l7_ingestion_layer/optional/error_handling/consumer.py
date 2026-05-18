from confluent_kafka import Consumer, Producer
import json
import sys

from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

DLQ_TOPIC = f"{KAFKA_TOPIC}.dlq"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "my-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

consumer.subscribe([KAFKA_TOPIC])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue

        try:
            value = json.loads(msg.value().decode("utf-8"))
            print(f"Received: {value}")
        except Exception as e:
            print(f"Failed at offset {msg.offset()}: {e} — sending to DLQ", file=sys.stderr)
            dlq_producer.produce(DLQ_TOPIC, value=msg.value())
            dlq_producer.flush()

        consumer.commit(message=msg)

except KeyboardInterrupt:
    print("Shutting down...")
finally:
    consumer.close()
    dlq_producer.flush()
