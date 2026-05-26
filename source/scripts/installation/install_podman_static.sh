#!/usr/bin/env bash

# Exit immediately on error
set -e

echo "=========================================================="
echo "📦 Rootless Native Podman Static Installer"
echo "=========================================================="

BIN_DIR="$HOME/bin"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TMP_DIR="$WORKSPACE_DIR/.tmp/installer"

# Ensure directories exist
mkdir -p "$BIN_DIR"
mkdir -p "$TMP_DIR"
mkdir -p "$WORKSPACE_DIR/.tmp/data" "$WORKSPACE_DIR/.tmp/runroot" "$WORKSPACE_DIR/.tmp/config/containers"

cd "$TMP_DIR"

# Download the latest mgoltzsche/podman-static release
echo "📥 Downloading latest statically-linked Podman bundle..."
curl -fsSL -o podman-linux-amd64.tar.gz https://github.com/mgoltzsche/podman-static/releases/latest/download/podman-linux-amd64.tar.gz

echo "📦 Extracting files..."
tar -xzf podman-linux-amd64.tar.gz

echo "🚀 Installing native Podman binaries to $BIN_DIR..."
# Backup existing podman just in case
if [ -f "$BIN_DIR/podman" ]; then
    echo "💾 Backing up existing podman client binary to podman.bak"
    mv "$BIN_DIR/podman" "$BIN_DIR/podman.bak"
fi

cp podman-linux-amd64/usr/local/bin/* "$BIN_DIR/"
cp podman-linux-amd64/usr/local/lib/podman/* "$BIN_DIR/"
chmod +x "$BIN_DIR"/podman "$BIN_DIR"/crun "$BIN_DIR"/conmon "$BIN_DIR"/fuse-overlayfs "$BIN_DIR"/pasta "$BIN_DIR"/fusermount3 "$BIN_DIR"/rootlessport "$BIN_DIR"/netavark

echo "⚙️ Writing custom rootless configuration files..."

# Generate custom storage.conf
# Store runtime and overlay layers in the local workspace directory
cat <<EOF > "$WORKSPACE_DIR/.tmp/config/containers/storage.conf"
[storage]
driver = "overlay"
runroot = "$WORKSPACE_DIR/.tmp/runroot"
graphroot = "$WORKSPACE_DIR/.tmp/storage"

[storage.options]
additionalimagestores = []

[storage.options.overlay]
mount_program = "$BIN_DIR/fuse-overlayfs"
force_mask = "700"
ignore_chown_errors = "true"
EOF

# Generate custom containers.conf
cat <<EOF > "$WORKSPACE_DIR/.tmp/config/containers/containers.conf"
[containers]
image_volume_mode = "ignore"

[engine]
runtime = "crun"
cgroup_manager = "cgroupfs"
events_logger = "none"
conmon_path = [
    "$BIN_DIR/conmon"
]
helper_binaries_dir = [
    "$BIN_DIR"
]

[engine.runtimes]
crun = [
    "$BIN_DIR/crun"
]
EOF

# Also sync them to user's home configuration directory for CLI convenience
mkdir -p "$HOME/.config/containers"
cp "$WORKSPACE_DIR/.tmp/config/containers/storage.conf" "$HOME/.config/containers/storage.conf"
cp "$WORKSPACE_DIR/.tmp/config/containers/containers.conf" "$HOME/.config/containers/containers.conf"

# Clean up installer directory
rm -rf "$TMP_DIR"

echo "=========================================================="
echo "🎉 Native Podman Static Installation Successful!"
echo "   Binaries installed: podman, crun, conmon, and helpers"
echo "   Stored in: $BIN_DIR"
echo "=========================================================="
echo "Verify the installation by running:"
echo "   podman info"
