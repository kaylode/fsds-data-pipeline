import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="metastore",
    user="hive",
    password="hive",
)
# Remember autocommit to True for DDL statements like CREATE TABLE? 
# It is used to persist the changes immediately without needing to call conn.commit() explicitly.
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS public.customers")
cursor.execute("""
    CREATE TABLE public.customers (
        customer_id INTEGER PRIMARY KEY,
        customer    VARCHAR(100),
        email       VARCHAR(200),
        tier        VARCHAR(20)
    )
""")

customers = [
    (1001, "Alice",   "alice@example.com",   "gold"),
    (1002, "Bob",     "bob@example.com",     "silver"),
    (1003, "Charlie", "charlie@example.com", "bronze"),
    (1004, "Diana",   "diana@example.com",   "gold"),
]

cursor.executemany("INSERT INTO public.customers VALUES (%s, %s, %s, %s)", customers)
print(f"Inserted {len(customers)} rows.")

cursor.execute("SELECT * FROM public.customers ORDER BY customer")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
