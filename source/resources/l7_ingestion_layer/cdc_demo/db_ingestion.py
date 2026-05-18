# This job is responsible for ingesting data from the PostgreSQL database and producing it to Kafka via Debezium.

import time
import random
import psycopg2

# --- Connection ---
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="testdb",
    user="postgres",
    password="postgres"
)
cursor = conn.cursor()

# --- Create table (run once) ---
cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id         SERIAL PRIMARY KEY,
        name       VARCHAR(100),
        email      VARCHAR(100),
        created_at TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()

# --- Sample data to pick from ---
names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"]

# --- Continuously insert rows ---
print("Starting continuous insert. Press Ctrl+C to stop.")
counter = 1

while True:
    name  = random.choice(names)
    email = f"{name.lower()}{counter}@example.com"

    cursor.execute(
        "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id",
        (name, email)
    )
    row_id = cursor.fetchone()[0]
    conn.commit()

    print(f"[{counter}] Inserted user id={row_id}  name={name}  email={email}")
    counter += 1
    time.sleep(1)   # wait 1 second between inserts
