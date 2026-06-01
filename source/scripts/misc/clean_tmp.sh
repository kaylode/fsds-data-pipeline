#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
# .tmp is one level up from source/ — matches ../../.tmp/ in docker-compose volume paths
TMP_DIR="$( cd "$PROJECT_ROOT/.." && pwd )/.tmp"

export PATH="$HOME/bin:$HOME/.local/bin:$PATH"
# Mirror the rootless Podman env from run_podman.sh so podman commands work
export PODMAN="${PODMAN:-$HOME/bin/podman}"
export CONTAINERS_CONF="$TMP_DIR/config/containers/containers.conf"
export CONTAINERS_STORAGE_CONF="$TMP_DIR/config/containers/storage.conf"
export XDG_RUNTIME_DIR="$TMP_DIR/run"

FORCE=false
if [ "$1" == "-f" ]; then
    FORCE=true
fi

if [ "$FORCE" = false ]; then
    echo "⚠️  Warning: This will:"
    echo "     • Stop all running containers"
    echo "     • Wipe MinIO (Delta Lake), PostgreSQL, Redis, and Kafka data
     • Delete generated synthetic EHR data and ML datasets
     • Delete the Feast feature store registry (registry.db)"
    echo "     • Reset Podman storage"
    echo "     • flink-lib/ (connector JARs) will be preserved"
    read -p "Are you sure you want to proceed? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Cleanup aborted."
        exit 0
    fi
fi

# 1. Stop and remove each known container by name (more reliable than compose down)
CONTAINERS=(
    flink-taskmanager flink-jobmanager
    spark-worker spark-master
    trino hive-metastore
    minio redis postgres kafka
)
echo "🛑 Stopping containers..."
for c in "${CONTAINERS[@]}"; do
    podman stop "$c" 2>/dev/null && echo "   stopped $c" || true
    podman rm   "$c" 2>/dev/null || true
done

# 2. Kill any stray podman processes
pkill -u "$USER" -f podman 2>/dev/null || true
sleep 1
# Do not run podman system reset -f to preserve pulled images in the local registry


# 3. Wipe data volumes (preserve flink-lib — user-downloaded JARs)
DATA_DIRS=(
    minio-data
    postgres-data
    metastore-db-data
    redis-data
    kafka-data
    spark-logs
    flink-logs
)
echo "🗑️  Wiping data in $TMP_DIR ..."
for dir in "${DATA_DIRS[@]}"; do
    target="$TMP_DIR/$dir"
    if [ -d "$target" ]; then
        rm -rf "$target"
        echo "   removed $dir/"
    fi
done

# 4. Wipe generated pipeline data (synthetic EHR, ML datasets, Feast registry)
echo "🗑️  Wiping generated data and feature store registry..."
rm -rf "$PROJECT_ROOT/data/synthetic"
rm -rf "$PROJECT_ROOT/data/ml"
rm -f  "$PROJECT_ROOT/config/feature_store/data/registry.db"

echo "✅ Clean complete. Run 'make up' then re-run the pipeline."
