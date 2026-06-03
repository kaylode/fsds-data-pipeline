#!/bin/bash
set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PROJECT_ROOT="$(cd "$script_dir/../.." && pwd)"
WORKSPACE_DIR="$(cd "$PROJECT_ROOT/.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

export PODMAN="${HOME}/bin/podman"
export CONTAINERS_CONF="$WORKSPACE_DIR/.tmp/config/containers/containers.conf"
export CONTAINERS_STORAGE_CONF="$WORKSPACE_DIR/.tmp/config/containers/storage.conf"
export XDG_RUNTIME_DIR="$WORKSPACE_DIR/.tmp/run"

INGEST_TIMEOUT="${DATAHUB_INGEST_TIMEOUT:-120}"
RECIPES="/datahub-recipes"

SUCCEEDED=()
FAILED=()
SKIPPED=()

ingest() {
    local name="$1"
    local recipe="$2"
    echo "  → $name"
    if timeout "$INGEST_TIMEOUT" \
        "$PODMAN" exec datahub-actions \
        datahub ingest -c "$recipe" 2>&1; then
        SUCCEEDED+=("$name")
    else
        echo "  ✗ $name (exit $?)" >&2
        FAILED+=("$name")
    fi
}

print_summary() {
    echo ""
    echo "─────────────────────────────────────────────"
    echo "  Ingestion summary"
    echo "─────────────────────────────────────────────"
    for s in "${SUCCEEDED[@]}"; do echo "  ✓  $s"; done
    for f in "${FAILED[@]}";    do echo "  ✗  $f  ← failed"; done
    for k in "${SKIPPED[@]}";   do echo "  –  $k  ← skipped"; done
    echo "─────────────────────────────────────────────"
}

echo "📥  Ingesting data sources to DataHub..."

# Verify GMS is reachable from inside datahub-actions before doing anything.
echo "  ⚙  Verifying GMS connectivity..."
_GMS_PORT="${DATAHUB_GMS_PORT:-8088}"
GMS_STATUS=$("$PODMAN" exec datahub-actions \
    sh -c "curl -s -o /dev/null -w \"%{http_code}\" http://127.0.0.1:${_GMS_PORT}/health 2>/dev/null" 2>&1 | tail -1)
if [ "$GMS_STATUS" != "200" ]; then
    echo "  ✗  Cannot reach DataHub GMS (http://127.0.0.1:${_GMS_PORT}/health → $GMS_STATUS)"
    echo "     Make sure the stack is up and GMS has finished starting."
    echo "     Check: podman logs datahub-gms --tail 20"
    exit 1
fi
echo "  ✓  GMS reachable (HTTP $GMS_STATUS)"

# Install extra deps into datahub-actions that are not bundled in the image.
# These are fast (<5 s) and idempotent.
echo "  ⚙  Installing missing container deps (redis, feast-trino)..."
"$PODMAN" exec datahub-actions pip install --quiet redis feast-trino 2>&1 \
    | grep -vE "^$|already satisfied|Requirement already" || true

ingest "postgres" "$RECIPES/postgres.yaml"
ingest "kafka"    "$RECIPES/kafka.yaml"

# hive-metastore plugin is not bundled in datahub-actions; hive tables
# are already visible via trino's hive catalog.
SKIPPED+=("hive-metastore  (plugin not in datahub-actions; covered by trino)")

ingest "trino"    "$RECIPES/trino.yaml"
ingest "feast"    "$RECIPES/feast.yaml"

# minio s3 source requires pyspark (~300 MB) which is not in datahub-actions.
# The trino delta catalog already exposes all lakehouse tables.
SKIPPED+=("minio            (s3 source needs pyspark; covered by trino delta catalog)")

print_summary
