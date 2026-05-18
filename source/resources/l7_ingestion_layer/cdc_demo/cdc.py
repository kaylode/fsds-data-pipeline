# CDC consumer: reads Debezium change events from Kafka
# Debezium monitors PostgreSQL and publishes every INSERT/UPDATE/DELETE
# to a Kafka topic named:  <server>.<schema>.<table>
#
# Architecture:
#   PostgreSQL  -->  Debezium (Kafka Connect)  -->  Kafka topic  -->  this script

import json
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC                   = "dbserver1.public.users"  # Debezium default: <server>.<schema>.<table>

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "cdc-consumer-group",
    "auto.offset.reset": "earliest",
})

consumer.subscribe([TOPIC])

print(f"Listening for changes on topic: {TOPIC}\n")

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    event = json.loads(msg.value().decode("utf-8"))

    # Debezium wraps the change inside a "payload" key
    payload = event.get("payload", event)   # handle both wrapped and unwrapped

    op     = payload.get("op")              # c=create, u=update, d=delete, r=read(snapshot)
    before = payload.get("before")          # row state BEFORE the change (None for inserts)
    after  = payload.get("after")           # row state AFTER  the change (None for deletes)

    if op == "c":
        print(f"[INSERT]   {after}")
    elif op == "u":
        print(f"[UPDATE]   before={before}  after={after}")
    elif op == "d":
        print(f"[DELETE]   {before}")
    elif op == "r":
        print(f"[SNAPSHOT] {after}")
    else:
        print(f"[UNKNOWN op={op}] {payload}")
