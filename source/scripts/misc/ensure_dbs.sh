#!/bin/bash
# Load env
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PROJECT_ROOT="$(cd "$script_dir/../.." && pwd)"
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Ensure user bin directories are in PATH for rootless podman
export PATH="$HOME/bin:$HOME/.local/bin:$PATH"

# Define defaults
POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_DB=${POSTGRES_DB:-postgres}

# Set XDG_RUNTIME_DIR to the workspace-specific run directory
export XDG_RUNTIME_DIR="$PROJECT_ROOT/../.tmp/run"

# Function to run psql inside the postgres container
run_sql() {
    podman exec -i postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1
}

# 1. Create roles if they do not exist
run_sql <<EOSQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${AIRFLOW_DB_USER:-airflow}') THEN
        CREATE ROLE ${AIRFLOW_DB_USER:-airflow} WITH LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD:-airflow}';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${DATAHUB_DB_USER:-datahub}') THEN
        CREATE ROLE ${DATAHUB_DB_USER:-datahub} WITH LOGIN PASSWORD '${DATAHUB_DB_PASSWORD:-datahub_password}';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${METASTORE_DB_USER:-hive}') THEN
        CREATE ROLE ${METASTORE_DB_USER:-hive} WITH LOGIN PASSWORD '${METASTORE_DB_PASSWORD:-hive}';
    END IF;
END
\$\$;
EOSQL

# 2. Create databases if they do not exist
create_db_if_not_exists() {
    local db_name=$1
    local db_owner=$2
    local exists=$(podman exec -i postgres psql -U "$POSTGRES_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='$db_name'")
    if [ "$exists" != "1" ]; then
        echo "Creating database '$db_name' owned by '$db_owner'..."
        podman exec -i postgres psql -U "$POSTGRES_USER" -c "CREATE DATABASE $db_name OWNER $db_owner;"
        podman exec -i postgres psql -U "$POSTGRES_USER" -c "GRANT ALL PRIVILEGES ON DATABASE $db_name TO $db_owner;"
    fi
}

create_db_if_not_exists "${AIRFLOW_DB_NAME:-airflow}" "${AIRFLOW_DB_USER:-airflow}"
create_db_if_not_exists "${DATAHUB_DB_NAME:-datahub}" "${DATAHUB_DB_USER:-datahub}"
create_db_if_not_exists "${METASTORE_DB_NAME:-metastore}" "${METASTORE_DB_USER:-hive}"
