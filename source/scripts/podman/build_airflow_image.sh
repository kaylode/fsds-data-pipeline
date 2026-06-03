#!/usr/bin/env bash
# scripts/podman/build_airflow_image.sh
#
# Builds localhost/airflow-custom:latest by running the base Airflow container,
# installing dependencies via pip install inside it via exec, then committing to a new image.
#
# WHY NOT `podman build`:
#   Rootless Podman with fuse-overlayfs calls lgetxattr() on every file in
#   the layer when committing a RUN step. The Apache Airflow base image has
#   /var/cache/apt/archives/partial with a security.capability xattr that
#   cannot be read without CAP_SYS_ADMIN, causing "permission denied".
#   Using podman exec + podman export/import avoids this layer-snapshotting entirely.

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
WORKSPACE_DIR="$( cd "$PROJECT_ROOT/.." && pwd )"

export CONTAINERS_CONF="${WORKSPACE_DIR}/.tmp/config/containers/containers.conf"
export CONTAINERS_STORAGE_CONF="${WORKSPACE_DIR}/.tmp/config/containers/storage.conf"
export XDG_RUNTIME_DIR="${WORKSPACE_DIR}/.tmp/run"
export PATH="${HOME}/bin:${HOME}/.local/bin:${PATH}"

# Locate podman binary
PODMAN=""
for p in "${HOME}/.local/bin/podman" "${HOME}/bin/podman"; do
    if [ -x "$p" ]; then PODMAN="$p"; break; fi
done
if [ -z "$PODMAN" ]; then
    echo "❌ podman not found in ~/.local/bin or ~/bin"
    exit 1
fi

BASE_IMAGE="docker.io/apache/airflow:2.10.2"
TARGET_IMAGE="localhost/airflow-custom:latest"
CONTAINER_NAME="airflow-custom-build"

echo "🧹 Removing any leftover build container..."
$PODMAN rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "🚀 Starting temporary container from base Airflow image..."
$PODMAN run -d \
    --name "$CONTAINER_NAME" \
    --user root \
    --entrypoint /bin/bash \
    "$BASE_IMAGE" \
    -c "sleep 3600"

echo "📦 Installing custom Python packages inside container..."
$PODMAN exec -u root "$CONTAINER_NAME" bash -c '
    set -e
    /home/airflow/.local/bin/pip install --no-cache-dir \
        "great_expectations==0.18.19" \
        "acryl-datahub[airflow]" \
        "acryl-datahub-airflow-plugin" \
        "pandas" \
        "sqlglot" \
        "python-dotenv" \
        "feast[trino]" || \
    /usr/local/bin/python -m pip install --no-cache-dir \
        "great_expectations==0.18.19" \
        "acryl-datahub[airflow]" \
        "acryl-datahub-airflow-plugin" \
        "pandas" \
        "sqlglot" \
        "python-dotenv" \
        "feast[trino]"



    echo "  → scrubbing apt cache (security.capability xattrs block rootless podman commit)..."
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* || true
    rm -rf /var/cache/apt/archives || true
    
    echo "  ✅ Custom dependencies installed successfully."
'

echo "💾 Exporting container filesystem and importing as image $TARGET_IMAGE..."
$PODMAN export "$CONTAINER_NAME" | $PODMAN import \
    --change 'USER 50000' \
    --change 'ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]' \
    --change 'ENV PATH=/root/bin:/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' \
    --change 'ENV LANG=C.UTF-8' \
    --change 'ENV GPG_KEY=7169605F62C751356D054A26A821E680E5FA6305' \
    --change 'ENV PYTHON_VERSION=3.12.6' \
    --change 'ENV PYTHON_BASE_IMAGE=python:3.12-slim-bookworm' \
    --change 'ENV DEBIAN_FRONTEND=noninteractive' \
    --change 'ENV LANGUAGE=C.UTF-8' \
    --change 'ENV LC_ALL=C.UTF-8' \
    --change 'ENV LC_CTYPE=C.UTF-8' \
    --change 'ENV LC_MESSAGES=C.UTF-8' \
    --change 'ENV LD_LIBRARY_PATH=/usr/local/lib' \
    --change 'ENV RUNTIME_APT_DEPS=' \
    --change 'ENV ADDITIONAL_RUNTIME_APT_DEPS=' \
    --change 'ENV RUNTIME_APT_COMMAND=echo' \
    --change 'ENV ADDITIONAL_RUNTIME_APT_COMMAND=' \
    --change 'ENV INSTALL_MYSQL_CLIENT=true' \
    --change 'ENV INSTALL_MYSQL_CLIENT_TYPE=mariadb' \
    --change 'ENV INSTALL_MSSQL_CLIENT=true' \
    --change 'ENV INSTALL_POSTGRES_CLIENT=true' \
    --change 'ENV GUNICORN_CMD_ARGS=--worker-tmp-dir /dev/shm' \
    --change 'ENV AIRFLOW_INSTALLATION_METHOD=' \
    --change 'ENV VIRTUAL_ENV=/home/airflow/.local' \
    --change 'ENV AIRFLOW_UID=50000' \
    --change 'ENV AIRFLOW_USER_HOME_DIR=/home/airflow' \
    --change 'ENV AIRFLOW_HOME=/opt/airflow' \
    --change 'ENV DUMB_INIT_SETSID=1' \
    --change 'ENV PS1=(airflow)' \
    --change 'ENV AIRFLOW_VERSION=2.10.2' \
    --change 'ENV AIRFLOW__CORE__LOAD_EXAMPLES=false' \
    --change 'ENV AIRFLOW_PIP_VERSION=24.2' \
    --change 'ENV AIRFLOW_UV_VERSION=0.4.1' \
    --change 'ENV AIRFLOW_USE_UV=false' \
    --change 'ENV BUILD_ID=' \
    --change 'ENV COMMIT_SHA=35087d7d10714130cc3e9e9730e34b07fc56938d' \
    - \
    "$TARGET_IMAGE"

echo "🧹 Removing build container..."
$PODMAN rm -f "$CONTAINER_NAME"

echo ""
echo "✅ Done! Custom Airflow image built: $TARGET_IMAGE"
$PODMAN image inspect "$TARGET_IMAGE" --format "   Size: {{.Size}} bytes"
