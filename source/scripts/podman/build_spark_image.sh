#!/usr/bin/env bash
# scripts/podman/build_spark_image.sh
#
# Builds localhost/spark-3.12:latest by running the base Spark container,
# installing Python 3.12 inside it via exec, then committing to a new image.
#
# WHY NOT `podman build`:
#   Rootless Podman with fuse-overlayfs calls lgetxattr() on every file in
#   the layer when committing a RUN step. The Apache Spark base image has
#   /var/cache/apt/archives/partial with a security.capability xattr that
#   cannot be read without CAP_SYS_ADMIN, causing "permission denied".
#   Using podman exec + podman commit avoids this layer-snapshotting entirely.

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

BASE_IMAGE="docker.io/apache/spark:3.5.6-scala2.12-java11-python3-ubuntu"
TARGET_IMAGE="localhost/spark-3.12:latest"
CONTAINER_NAME="spark-py312-build"

echo "🧹 Removing any leftover build container..."
$PODMAN rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "🚀 Starting temporary container from base Spark image..."
$PODMAN run -d \
    --name "$CONTAINER_NAME" \
    --user root \
    --entrypoint /bin/bash \
    "$BASE_IMAGE" \
    -c "sleep 3600"

echo "📦 Installing Python 3.12 inside container..."
$PODMAN exec -u root "$CONTAINER_NAME" bash -c '
    set -e

    # Use Gregory Szorc'"'"'s python-build-standalone prebuilt binaries.
    # No apt-get, no PPA, no dpkg post-install scripts (avoids PostgreSQL / ssl-cert failures).
    # These are fully self-contained glibc builds that work on any modern Linux.
    PY_VER="3.12.10"
    PY_BUILD="20250409"
    PY_URL="https://github.com/indygreg/python-build-standalone/releases/download/${PY_BUILD}/cpython-${PY_VER}+${PY_BUILD}-x86_64-unknown-linux-gnu-install_only.tar.gz"

    echo "  → downloading Python ${PY_VER} standalone build..."
    cd /tmp
    if command -v wget > /dev/null 2>&1; then
        wget -q "$PY_URL" -O python312.tar.gz
    elif command -v curl > /dev/null 2>&1; then
        curl -fsSL "$PY_URL" -o python312.tar.gz
    else
        echo "❌ Neither wget nor curl found in container"; exit 1
    fi

    echo "  → extracting to /usr/local/..."
    # The tarball extracts to a "python/" directory — strip that prefix
    tar -xzf python312.tar.gz -C /usr/local/ --strip-components=1
    rm python312.tar.gz

    echo "  → verifying install..."
    /usr/local/bin/python3.12 --version

    echo "  → upgrading pip..."
    /usr/local/bin/python3.12 -m pip install --upgrade pip setuptools --no-cache-dir

    echo "  → creating spark home directory..."
    mkdir -p /home/spark
    # NOTE: In rootless Podman, chown to any UID other than 0 (root) fails with
    # "Invalid argument" because the target UID is outside the user namespace
    # mapping. We use chmod instead — the running identity is set by
    # --change 'USER spark' at commit time, so the dir just needs to be accessible.
    chmod 755 /home/spark

    echo "  → scrubbing apt cache (security.capability xattrs block rootless podman commit)..."
    # /var/cache/apt/archives/partial has a security.capability xattr that requires
    # CAP_SYS_ADMIN to read via lgetxattr(). Rootless Podman lacks this capability,
    # so 'podman commit' fails when snapshotting any layer containing that path.
    # Deleting it inside the container removes it from the committed layer entirely.
    rm -rf /var/cache/apt/archives /var/lib/apt/lists/* /tmp/* /var/tmp/*

    echo "  ✅ Python 3.12 installed: $(/usr/local/bin/python3.12 --version)"
'

echo "💾 Exporting container filesystem and importing as image $TARGET_IMAGE..."
# WHY export|import INSTEAD OF commit:
#   'podman commit' copies all base image layers verbatim, calling lgetxattr()
#   on every file in every layer. The base Spark image has a file in
#   /var/cache/apt/archives/partial with a security.capability xattr that
#   requires CAP_SYS_ADMIN — which rootless Podman lacks → permission denied.
#
#   'podman export' reads the *merged* overlayfs view (the final flattened
#   filesystem) and streams it as a plain tar. No per-layer lgetxattr calls.
#   'podman import' then builds a single-layer image from that tar.
#   We pass the same metadata via --change flags to preserve runtime behaviour.
$PODMAN export "$CONTAINER_NAME" | $PODMAN import \
    --change 'USER spark' \
    --change 'ENV HOME=/home/spark' \
    --change 'ENV PATH=/home/spark/.local/bin:/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' \
    --change 'ENV PYSPARK_PYTHON=/usr/local/bin/python3.12' \
    --change 'ENTRYPOINT ["/opt/entrypoint.sh"]' \
    - \
    "$TARGET_IMAGE"

echo "🧹 Removing build container..."
$PODMAN rm -f "$CONTAINER_NAME"

echo ""
echo "✅ Done! Image built: $TARGET_IMAGE"
$PODMAN image inspect "$TARGET_IMAGE" --format "   Size: {{.Size}} bytes"
