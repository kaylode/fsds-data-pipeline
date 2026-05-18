import trino

conn = trino.dbapi.connect(
    host="localhost",
    port=8080,
    user="trino"
)

cursor = conn.cursor()
cursor.autocommit = True

# Create a schema parquet_zon if not exists
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS hive.parquet_zone
    WITH (location = 's3://datalake/')
""")
# Remember the columns must match the Parquet schema exactly (case-sensitive) for Trino to read it correctly.
cursor.execute("""
    CREATE TABLE IF NOT EXISTS hive.parquet_zone.orders (
        order_id    INTEGER,
        customer_id INTEGER,
        product     VARCHAR,
        amount      DOUBLE,
        order_date  TIMESTAMP
    )
    WITH (
        format = 'PARQUET',
        external_location = 's3://datalake/topics/orders/'
    )
""")

# Verify the table was created and can read data from MinIO. 
cursor.execute("SELECT * FROM hive.parquet_zone.orders")
rows = cursor.fetchall()

# Try to insert some data into the Parquet table.
cursor.execute("""
    INSERT INTO hive.parquet_zone.orders (order_id, customer_id, product, amount, order_date)
    VALUES (1, 1001, 'Widget', 19.99, TIMESTAMP '2024-01-01 10:00:00')
""")

if rows:
    for row in rows:
        print(row)
else:
    print("Table is empty.")

cursor.close()
conn.close()
