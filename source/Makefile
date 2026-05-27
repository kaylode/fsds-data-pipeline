.PHONY: up down logs ps clean check-ports free-ports install-podman generate ingest stream query help datahub-ingest

# ─── Infrastructure ───────────────────────────────────────────────────────────

# Start all core containers (Postgres, Kafka, MinIO, Hive Metastore, Trino)
up:
	@bash scripts/podman/run_podman.sh core up -d
# 	@echo "⌛ Waiting for Postgres to accept connections..."
# 	@until podman exec postgres pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done
# 	@echo "🔑 Ensuring all databases and roles exist..."
# 	@bash scripts/misc/ensure_dbs.sh
# 	@echo "🚀 Infrastructure is up and configured."

# Stop all containers
down:
	@bash scripts/podman/run_podman.sh core down

# Follow container logs
logs:
	@bash scripts/podman/run_podman.sh core logs

# Show container status
ps:
	@bash scripts/podman/run_podman.sh core ps

# Check if configured ports are in use
check-ports:
	@~/.local/bin/uv run scripts/misc/check_ports.py

# Kill any processes occupying the configured ports
free-ports:
	@~/.local/bin/uv run scripts/misc/free_ports.py

# Reset Podman storage and wipe the .tmp directory
clean:
	@bash scripts/misc/clean_tmp.sh

# Download and install the full native Podman server package statically without root
install-podman:
	@bash scripts/podman/install_podman_static.sh

# ─── EHR Data Pipeline ───────────────────────────────────────────────────────

# Generate both offline (Parquet) and stream (JSON) synthetic EHR data
#   Offline: patients, wards, event_metadata, visits, events  → data/synthetic/
#   Stream : clinical_event_stream.json                       → data/synthetic/
generate:
	@echo "🏭  Generating offline EHR Parquet tables..."
	@uv run scripts/course-work/1a_generate_offline_ehr.py --mode offline
	@echo "📡  Generating streaming EHR JSON events..."
	@uv run scripts/course-work/1a_generate_offline_ehr.py --mode stream
	@echo "✅  Generation complete."

# Ingest offline historical data into the Bronze Lakehouse (MinIO / Delta / Trino)
#   Topics: raw_patients, raw_wards, raw_events
ingest:
	@echo "📥  Ingesting historical EHR data into Bronze layer..."
	@uv run scripts/course-work/2a_ingest_to_bronze.py
	@echo "✅  Ingestion complete."

# Simulate live event streaming: publish JSON events to a Kafka topic
stream:
	@echo "🚀  Publishing clinical events to Kafka topic..."
	@uv run scripts/course-work/1b_produce_stream.py
	@echo "✅  Streaming complete."

# Pull from Kafka topic → append to Delta Lake, then query the Bronze layer for an overview
consume:
	@echo "🔄  Consuming Kafka topic → Bronze Delta Lake..."
	@uv run scripts/course-work/2b_consume_stream.py

# Submit PySpark job from host to Spark Cluster for Silver transformations
transform:
	@echo "✨  Running PySpark Silver layer transformations..."
	@uv run scripts/course-work/3_transform_to_silver.py
	@echo "✅  Silver transformations complete."

golden:
	@echo "✨  Running PySpark Gold layer transformations..."
	@uv run scripts/course-work/4_transform_to_gold.py
	@echo "✅  Gold transformations complete."

# Query and validate Lakehouse (Bronze & Silver) tables in Trino
query-lakehouse:
	@echo "🔍  Querying and validating Lakehouse tables..."
	@uv run scripts/course-work/query_lakehouse.py

# Ingest all data sources to DataHub GMS
datahub-ingest:
	@bash scripts/misc/datahub_ingest.sh

# ─── Help ────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  FSDS EHR Pipeline — available targets"
	@echo "  ────────────────────────────────────────────────────────────────"
	@echo "  Infrastructure"
	@echo "    make up              Start all core containers"
	@echo "    make down            Stop all containers"
	@echo "    make logs            Follow container logs"
	@echo "    make ps              List container status"
	@echo "    make check-ports     Check if configured ports are in use"
	@echo "    make free-ports      Kill all processes occupying configured ports"
	@echo "    make clean           Reset Podman storage and wipe .tmp"
	@echo "    make install-podman  Install native statically-linked Podman"
	@echo ""
	@echo "  EHR Data Pipeline"
	@echo "    make generate        Generate offline Parquet + streaming JSON data"
	@echo "    make ingest          Ingest historical data into Bronze Lakehouse"
	@echo "    make stream          Publish stream events to Kafka topic"
	@echo "    make query-lakehouse Query and validate Lakehouse (Bronze + Silver) tables"
	@echo "    make transform       Run PySpark Silver layer transformations"
	@echo "    make datahub-ingest  Ingest all data sources to DataHub GMS"
	@echo ""
