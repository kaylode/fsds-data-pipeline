### How-to Run 
1. Step 1: Register Debezium connector and start ingest sample data into DB
    ```bash
    bash register.sh
    python db_ingestion.py
    ```

2. Step 2: CDC
    ```bash
    python cdc.py
    ```