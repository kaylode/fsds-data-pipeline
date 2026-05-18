import trino

conn = trino.dbapi.connect(
    host="localhost",
    port=8080,
    user="trino",
)

cursor = conn.cursor()

# Join Delta Lake orders with PostgreSQL customers via Trino
cursor.execute("""
    SELECT
        o.order_id,
        c.email,
        c.tier,
        o.amount,
        o.order_date
    FROM delta.delta_zone.orders AS o
    JOIN postgresql.public.customers AS c
      ON o.customer_id = c.customer_id
    ORDER BY o.order_id
""")

rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

print(" | ".join(columns))
print("-" * 80)
if rows:
    for row in rows:
        print(" | ".join(str(v) for v in row))
else:
    print("No matching rows.")

cursor.close()
conn.close()
