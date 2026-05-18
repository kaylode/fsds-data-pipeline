from confluent_kafka import Consumer
import json

from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# confluent_kafka uses a config dict; auto.offset.reset and group.id are dot-separated keys
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "my-group",           # Consumers with the same group.id share work
    "auto.offset.reset": "earliest",  # Start from earliest if no offset is committed yet, you can also set it to "latest" to only consume new messages
})

consumer.subscribe([KAFKA_TOPIC])

# Continuously listen for new messages and print them as they arrive
while True:
    # Timeout is in seconds; adjust as needed. If no message arrives within the timeout, poll() returns None.
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue
    value = json.loads(msg.value().decode("utf-8"))
    print(f"Received: {value}")
    # Manually commit the offset after processing the message (optional, depends on your use case)
    # Note: confluent_kafka can auto-commit via enable.auto.commit=True (default is True), but due to it is time-based, not after your processing, so we show manual commit here for better control. You can also set auto.commit.interval.ms to adjust the frequency of auto-commits.
    # If you want manual control, set enable.auto.commit=False and call consumer.commit()
    # consumer.commit()