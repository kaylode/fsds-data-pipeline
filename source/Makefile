.PHONY: up down logs ps clean check-ports free-ports install-podman build-spark generate ingest stream transform golden compute-features query-lakehouse datahub-up datahub-down datahub-logs datahub-ingest datahub-enrich datahub-register feast-setup flink-stream feast-stream merge-features query-featurestore export-ml-dataset help airflow-up airflow-down airflow-logs airflow-ps build-airflow

export PATH := $(HOME)/bin:$(HOME)/.local/bin:$(PATH)
export CONTAINERS_CONF := $(shell pwd)/../.tmp/config/containers/containers.conf
export CONTAINERS_STORAGE_CONF := $(shell pwd)/../.tmp/config/containers/storage.conf
export XDG_RUNTIME_DIR := /tmp/fsds-run-$(USER)

# ─── Infrastructure ───────────────────────────────────────────────────────────

# Start all core containers (Postgres, Kafka, MinIO, Hive Metastore, Trino)
up:
	@echo "📥 Ensuring Flink connector JARs are downloaded..."
	@bash scripts/installation/download_jars.sh
	@bash scripts/podman/run_podman.sh core up -d
	@echo "⌛ Waiting for Postgres to accept connections..."
	@until python3 -c "import socket; s = socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', $${POSTGRES_PORT:-5432}))" >/dev/null 2>&1; do sleep 1; done
	@echo "🔑 Ensuring all databases and roles exist..."
	@bash scripts/misc/ensure_dbs.sh
	@echo "🚀 Infrastructure is up and configured."


# Stop all containers
down:
	@bash scripts/podman/run_podman.sh core down

# Follow container logs
logs:
	@bash scripts/podman/run_podman.sh core logs

# Show container status
ps:
	@bash scripts/podman/run_podman.sh core ps

# Build custom Airflow image using host-exec-export-import approach to avoid permission errors
build-airflow:
	@echo "🔨 Building Airflow custom image (export/import method)..."
	@bash scripts/podman/build_airflow_image.sh

# Start Airflow stack (image must already be built via: make build-airflow)
airflow-up:
	@echo "🚀 Starting Airflow..."
	@bash scripts/podman/run_podman.sh airflow up -d
	@echo "⌛ Waiting for Airflow Webserver to be healthy (timeout 300s)..."
	@i=0; until curl -s --fail http://127.0.0.1:$${AIRFLOW_WEBSERVER_PORT:-8082}/health >/dev/null 2>&1; do \
		sleep 2; echo -n "."; i=$$((i+2)); \
		if [ $$i -ge 300 ]; then echo ""; echo "❌ Timed out. Check logs: make airflow-logs"; exit 1; fi; \
	done
	@echo ""
	@echo "✅ Airflow is up at http://127.0.0.1:$${AIRFLOW_WEBSERVER_PORT:-8082} (user: airflow / pass: airflow)"

# Stop Airflow stack
airflow-down:
	@bash scripts/podman/run_podman.sh airflow down

# Follow Airflow container logs
airflow-logs:
	@bash scripts/podman/run_podman.sh airflow logs

# Show Airflow container status
airflow-ps:
	@bash scripts/podman/run_podman.sh airflow ps


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
	@bash scripts/installation/install_podman_static.sh

# Build the custom Spark image with Python 3.12 (needed to match host Python version).
# Uses podman run+exec+commit instead of podman build to avoid lgetxattr errors
# in rootless Podman when snapshotting layers that have security.capability xattrs.
# Run this ONCE before 'make up'.
build-spark:
	@echo "🔨 Building Spark+Python3.12 image (run/exec/commit method)..."
	@bash scripts/podman/build_spark_image.sh

# ─── EHR Data Pipeline ───────────────────────────────────────────────────────

# Generate both offline (Parquet) and stream (JSON) synthetic EHR data
#   Offline: patients, wards, event_metadata, visits, events  → data/synthetic/
#   Stream : clinical_event_stream.json                       → data/synthetic/
generate:
	@echo "🏭  Generating offline EHR Parquet tables..."
	@uv run scripts/main/1a_generate_offline_ehr.py
	@echo "✅  Generation complete."

# Ingest offline historical data into the Bronze Lakehouse (MinIO / Delta / Trino)
#   Topics: raw_patients, raw_wards, raw_events
ingest:
	@echo "📥  Ingesting historical EHR data into Bronze layer..."
	@uv run scripts/main/2a_ingest_to_bronze.py
	@echo "✅  Ingestion complete."

# Simulate live event streaming: publish JSON events to a Kafka topic
stream:
	@echo "🚀  Publishing clinical events to Kafka topic..."
	@uv run scripts/main/1b_produce_stream.py
	@echo "✅  Streaming complete."

# Submit PySpark job from host to Spark Cluster for Silver transformations
transform:
	@echo "✨  Running PySpark Silver layer transformations..."
	@uv run scripts/main/3_transform_to_silver.py
	@echo "✅  Silver transformations complete."

golden:
	@echo "✨  Running PySpark Gold layer transformations..."
	@uv run scripts/main/4_transform_to_gold.py
	@echo "✅  Gold transformations complete."

# Compute pre-aggregated feature tables (feat_*) and training labels (gold_visit_labels)
features:
	@echo "🧮  Computing feature tables and training labels..."
	@uv run scripts/main/5_compute_features.py
	@echo "✅  Feature engineering complete."

# Query and validate all Lakehouse layers (Bronze / Silver / Gold) in Trino
query-lakehouse:
	@echo "🔍  Querying and validating Lakehouse tables..."
	@uv run scripts/main/query_lakehouse.py

# Start DataHub metadata platform (opensearch + gms + actions + frontend)
# Requires core stack to be running first: make up
datahub-up:
	@echo "📊 Starting DataHub metadata platform..."
	@bash scripts/misc/ensure_dbs.sh
	@bash scripts/podman/run_podman.sh datahub up -d
	@echo "⌛ Waiting for DataHub GMS to be healthy..."
	@until curl -s --fail http://127.0.0.1:$${DATAHUB_GMS_PORT:-8088}/health >/dev/null 2>&1; do sleep 3; echo -n "."; done
	@echo ""
	@echo "✅ DataHub is up — UI: http://127.0.0.1:$${DATAHUB_FRONTEND_PORT:-9002}  GMS: http://127.0.0.1:$${DATAHUB_GMS_PORT:-8088}"
	@echo "📥 Running source ingestion (postgres, kafka, trino, feast)..."
	@bash scripts/misc/datahub_ingest.sh || echo "⚠️  datahub-ingest had failures — check output above"
	@echo "🏷️  Enriching dataset metadata (docs, owners, tags, domain)..."
	@~/.local/bin/uv run scripts/misc/datahub_enrich_metadata.py || echo "⚠️  Metadata enrichment had failures — re-run: make datahub-enrich"

# Stop DataHub stack only (leaves core infrastructure running)
datahub-down:
	@bash scripts/podman/run_podman.sh datahub down

# Follow DataHub container logs
datahub-logs:
	@bash scripts/podman/run_podman.sh datahub logs

# Ingest all data sources to DataHub GMS (one-shot)
datahub-ingest:
	@bash scripts/misc/datahub_ingest.sh

# Enrich DataHub dataset metadata (docs, owners, tags, domain, terms) — one-shot
datahub-enrich:
	@echo "🏷️  Enriching DataHub dataset metadata..."
	@~/.local/bin/uv run scripts/misc/datahub_enrich_metadata.py

# Start unified Flink job: archives raw stream to Bronze + computes 24h rolling features
flink-stream:
	@echo "⚡  Starting unified Flink stream processor..."
	@uv run scripts/main/6_flink_stream_processor.py

# Unified feature pipeline: apply + materialize + stream push + 60s merge loop
feature-pipeline:
	@echo "🚀  Starting unified feature pipeline (Ctrl-C to stop)..."
	@uv run scripts/main/7_feature_pipeline.py

# Test online feature retrieval from the Feast online store
query-featurestore:
	@echo "🔍  Testing online feature serving..."
	@uv run scripts/main/query_featurestore.py --online

# ─── Help ────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  FSDS EHR Pipeline — available targets"
	@echo "  ────────────────────────────────────────────────────────────────"
	@echo "  Infrastructure"
	@echo "    make up              Start core stack (Kafka, Spark, Flink, Trino, MinIO, Postgres, Redis)"
	@echo "    make down            Stop core stack"
	@echo "    make logs            Follow core container logs"
	@echo "    make ps              List container status"
	@echo "    make datahub-up      Start DataHub platform (run after make up)"
	@echo "    make datahub-down    Stop DataHub platform"
	@echo "    make datahub-logs    Follow DataHub container logs"
	@echo "    make check-ports     Check if configured ports are in use"
	@echo "    make free-ports      Kill all processes occupying configured ports"
	@echo "    make clean           Reset Podman storage and wipe .tmp"
	@echo "    make install-podman  Install native statically-linked Podman"
	@echo ""
	@echo "  Batch Pipeline (run in order)"
	@echo "    make generate          Generate offline Parquet + streaming JSON data"
	@echo "    make ingest            Ingest historical data into Bronze Delta tables"
	@echo "    make transform         PySpark: Bronze → Silver (clean, deduplicate)"
	@echo "    make golden            PySpark: Silver → Gold (dims, facts, OBT, typed dims)"
	@echo "    make compute-features  PySpark: Gold → feat_* tables + training labels"
	@echo "    make feast-setup       Register and materialize Feast features"
	@echo ""
	@echo "  Streaming Pipeline (run continuously, in separate terminals)"
	@echo "    make stream            Publish clinical events to Kafka (patient-events topic)"
	@echo "    make flink-stream      Flink: archive raw stream to Bronze + 24h rolling features"
	@echo "    make feast-stream      Bridge Flink output to Feast online store"
	@echo ""
	@echo "  Serving"
	@echo "    make merge-features    Every 15 min: merge offline + online → prediction_features"
	@echo ""
	@echo "  Validation & ML Export"
	@echo "    make query-lakehouse   Validate all Bronze / Silver / Gold tables in Trino"
	@echo "    make query-featurestore  Test online feature retrieval from Feast"
	@echo "    make export-ml-dataset   Build train.parquet + test.parquet for ML training"
	@echo "    make datahub-ingest    One-shot metadata ingestion to DataHub"
	@echo "    make datahub-register  Register all sources with DataHub managed ingestion (runs on schedule)"
	@echo ""
