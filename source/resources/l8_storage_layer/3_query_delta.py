import trino

conn = trino.dbapi.connect(
    host="localhost",
    port=8080,
    user="trino",
)

cursor = conn.cursor()
# # Step 1: Create Delta Lake table in Trino (if not exists) pointing to the same MinIO location as before.
# cursor.execute("""
#     CREATE SCHEMA IF NOT EXISTS delta.delta_zone 
#     WITH (location = 's3://lakehouse/')
# """)
# # Step 2: Register the Delta Lake table with Trino's Delta connector (if not already registered).
# cursor.execute("""
#     CALL delta.system.register_table(
#         schema_name => 'delta_zone',
#         table_name => 'orders',
#         table_location => 's3://lakehouse/topics/orders/'
#     )
# """)

# Step 3: Query the Delta Lake table via Trino to verify it works.
cursor.execute("SELECT * FROM delta.delta_zone.orders")
rows = cursor.fetchall()

# Step 4: Try to insert some data into the Delta table
cursor.execute("""
    INSERT INTO delta.delta_zone.orders (order_id, customer_id, product, amount, order_date)
    VALUES (1, 1001, 'Widget', 19.99, TIMESTAMP '2024-01-01 10:00:00')
""")

if rows:
    for row in rows:
        print(row)
else:
    print("Table is empty.")

cursor.close()
conn.close()