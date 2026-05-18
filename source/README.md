# 🚀 Full-Stack Data Streaming (FSDS) Cluster

A state-of-the-art, zero-privilege **Change Data Capture (CDC)** and **Kafka Data Ingestion** pipeline designed for 100% rootless container execution. Powered by **KRaft-mode Kafka**, **Debezium**, **PostgreSQL**, and managed via a sleek developer **Redpanda Console UI**.

---

## 🏗️ System Architecture

This cluster orchestrates a production-grade **real-time CDC patient event streaming and analytical consumption pipeline**:

```mermaid
graph TD
    subgraph Ingestion Layer
        Sim[EHR Replay Simulator] -- SQL Insertions --> DB[(PostgreSQL)]
        PGWeb[pgweb Database Explorer] -- Port 8085 --> DB
    end

    subgraph Change Data Capture (CDC)
        DB -- WAL Logical Replication --> Debezium[Debezium Connect]
    end

    subgraph Event Broker
        Debezium -- Port 8083 --> Kafka((Kafka Broker))
        Kafka -- Schema Validation --> SR[Schema Registry]
    end

    subgraph Real-Time Analytics
        Kafka -- Event Subscription --> Consumer[EHR Live CDC Consumer]
    end

    subgraph Developer Management
        RP[Redpanda Console UI] -- Port 8080 --> Kafka
        RP -- Connect REST API --> Debezium
        RP -- Schema Registry API --> SR
    end

    style Sim fill:#ffeb3b,stroke:#333,stroke-width:2px,color:#333
    style Consumer fill:#ff7043,stroke:#333,stroke-width:2px,color:#fff
    style PGWeb fill:#ab47bc,stroke:#333,stroke-width:2px,color:#fff
    style RP fill:#ff6f61,stroke:#333,stroke-width:2px,color:#fff
    style Kafka fill:#4facfe,stroke:#333,stroke-width:2px,color:#fff
    style DB fill:#66bb6a,stroke:#333,stroke-width:2px,color:#fff
```

---

## 🖥️ Graphical Developer Tooling & UI Consoles

After launching the cluster with `make up`, you can access two high-fidelity graphical user interfaces:

### 🎯 Redpanda Console (Kafka & Connect UI)
* **URL:** [http://localhost:8080](http://localhost:8080)
* **Features:**
  * **Interactive Message Inspection:** Browse topics, partition offsets, and inspect actual serialized payloads in real time.
  * **Connect Management:** Monitor active Debezium connectors, trace connector tasks, and configure database logical streams.
  * **Schema Registry Integration:** Inspect registered Apache Avro/JSON schemas, subject revisions, and compatibility levels.
  * **Consumer Group Health:** Instantly monitor group state, consumer lags, and broker distribution.

### 🗃️ pgweb (PostgreSQL Visual Database Explorer)
* **URL:** [http://localhost:8085](http://localhost:8085)
* **Features:**
  * **Zero-Config Automatic Connection:** Instantly pre-authenticated using your workspace environment credentials.
  * **Visual Schema Explorer:** Browse, sort, and query tables (`subjects`, `patient_events`) without writing custom SQL.
  * **Interactive Query Console:** Write, execute, and inspect the outcomes of SQL queries instantly.
  * **Data Export:** Export queried patient rows straight to JSON, CSV, or XML with a single click.

---

## 🔌 Port & Service Directory

All services run under **Host Network Mode (`network_mode: host`)** for seamless, low-latency communication on localhost:

| Service | Port | Description | Protocol / Endpoint |
| :--- | :---: | :--- | :--- |
| **Redpanda Console** | `8080` | **Kafka & Connector Web UI Console** | HTTP |
| **pgweb** | `8085` | **Visual PostgreSQL Database Explorer** | HTTP |
| **PostgreSQL** | `5432` | Primary Database with logical replication enabled | SQL |
| **Kafka Broker** | `9092` | Event Broker (KRaft Mode - no ZooKeeper) | TCP / PLAINTEXT |
| **Debezium Connect** | `8083` | Kafka Connect Engine with pre-packaged connectors | HTTP REST API |
| **Schema Registry** | `8081` | Confluent Schema Registry | HTTP REST API |
| **Kafka Controller** | `9093` | KRaft Controller quorum listener | TCP |

---

## 🚀 Quick Start

Ensure your ports are free, then bring up the cluster using the registered Make targets:

```bash
# 1. (Optional) Check if ports are occupied before boot
make check-ports

# 2. Boot the entire cluster in the background
make up

# 3. View the active container status
make ps

# 4. Follow log streams
make logs

# 5. Tear down the cluster
make down
```

---

## 🔬 Running the CDC Demo Walkthrough

We have bundled a complete, production-grade baseline verification script in the [resources/l7_ingestion_layer/cdc_demo/](resources/l7_ingestion_layer/cdc_demo/) folder.

### Step 1: Initialize Database & Connector
Boot up your database and Debezium connect targets, then register the PostgreSQL connector (run this from the demo directory):
```bash
cd resources/l7_ingestion_layer/cdc_demo
bash register.sh
```

### Step 2: Start DB Ingestion Stream
Start inserting continuous mock rows into PostgreSQL (updates once per second):
```bash
# Executed via uv run to comply with python requirements
uv run python db_ingestion.py
```

### Step 3: Run the Real-Time CDC Consumer
In a new terminal window, run the consumer script to watch database updates get captured by Debezium, pushed to Kafka, and consumed in real time:
```bash
uv run python cdc.py
```

---

## 🏥 Real-Time EHR Patient Event CDC Pipeline (Production Walkthrough)

We have engineered an advanced, end-to-end clinical simulation and Change Data Capture pipeline using processed **MEDS** patient records. 

The simulator features a high-fidelity **Hybrid Gap-Skipping Algorithm** which dynamically compresses long periods of inactive patient history (such as years between birth and admission) into 0.5-second pauses while preserving microsecond chronological offsets for active clinical logs (vitals, labs).

Follow this step-by-step production flow:

### Step 1: Initialize Database Tables & Kafka Topics
To prevent bootstrap synchronization issues, ensure the target database schema exists and the Kafka broker has registered the topics:
```bash
# 1. Boot up the FSDS Podman stack
make up

# 2. Programmatically synchronize PostgreSQL DDL and pre-register Debezium connector
uv run python -c "from fsds.core.database import get_db_connection, init_db_schema; init_db_schema(get_db_connection())"
curl -X POST -H "Content-Type: application/json" -d @config/ehr-connector-config.json http://localhost:8083/connectors
```

### Step 2: Start the Live CDC Event Consumer
In your first terminal window, start the real-time event decoder. It will subscribe to the Kafka topics and wait for messages:
```bash
uv run scripts/data/ehr_cdc_consumer.py
```

### Step 3: Run the Patient Stream Simulation
In your second terminal window, trigger the replay stream (replaying 1 day of clinical history per second):
```bash
# Executed via pre-configured Make target
make simulate
```
*Press **ENTER** in the simulator terminal when prompted to begin the live event stream.*

### Step 4: Monitor the Live Flow
* **Visual Database Explorer:** Monitor table inserts and browse patient tables on **pgweb** at **[http://localhost:8085](http://localhost:8085)**.
* **Message Broker Console:** Inspect serialized Kafka event payloads in real-time on the **Redpanda Console** at **[http://localhost:8080](http://localhost:8080)**.
* **CDC Consumer Terminal:** Watch formatted clinical vitals, labs, and timeline admissions decoded and logged in real-time.

---

## ⚙️ Host & Storage Configuration

To maintain absolute isolation and simple file cleanup, all Podman and application cache directories, along with temporary database volumes, are locked inside the local project workspace under the hidden **`.tmp`** folder.

* **Podman Cache Directories:** Managed within [`.tmp/`](.tmp/) (`XDG_DATA_HOME`, `XDG_CONFIG_HOME`, `XDG_CACHE_HOME`).
* **Clean storage command:** Run `make clean` to completely reset local container states and wipe out all local cache blocks instantly.
