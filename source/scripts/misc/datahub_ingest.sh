#!/bin/bash
set -e

# Resolve directories
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PROJECT_ROOT="$(cd "$script_dir/../.." && pwd)"

# Load environment variables if .env exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Detect uv binary
UV_CMD="$HOME/.local/bin/uv"

echo "📥  Ingesting data sources to DataHub..."

# $UV_CMD run datahub ingest -c "$PROJECT_ROOT/config/datahub-recipes/postgres.yaml"
# $UV_CMD run datahub ingest -c "$PROJECT_ROOT/config/datahub-recipes/kafka.yaml"
# $UV_CMD run datahub ingest -c "$PROJECT_ROOT/config/datahub-recipes/hive.yaml"
$UV_CMD run datahub ingest -c "$PROJECT_ROOT/config/datahub-recipes/trino.yaml"
$UV_CMD run datahub ingest -c "$PROJECT_ROOT/config/datahub-recipes/minio.yaml"

echo "✅  Ingestion complete."
