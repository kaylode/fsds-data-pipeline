#!/usr/bin/env bash
# scripts/podman/fix_deadlock.sh
# Resolves Podman deadlocks by forcefully killing daemons and wiping corrupted local storage.
# WARNING: This will destroy all local rootless containers and images!

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
WORKSPACE_DIR="$( cd "$SCRIPT_DIR/../.." && pwd )"

echo "=========================================================="
echo "⚠️  Podman Deadlock Resolver & Storage Reset"
echo "=========================================================="
echo "This will forcefully kill all Podman processes and wipe the"
echo "local rootless storage directory to clear deadlocks."
echo "Any pulled images will need to be re-downloaded."
echo "Press Ctrl+C within 5 seconds to cancel..."
sleep 5

echo "🔪 Killing stuck podman, conmon, and crun processes..."
pkill -9 podman || true
pkill -9 conmon || true
pkill -9 crun || true
pkill -9 podman-compose || true

echo "🧹 Attempting to renumber locks (if possible)..."
if command -v podman &> /dev/null; then
    podman system renumber || true
elif [ -f "$HOME/bin/podman" ]; then
    "$HOME/bin/podman" system renumber || true
fi

echo "🗑️  Wiping corrupted rootless storage ($WORKSPACE_DIR/.tmp/)..."
rm -rf "$WORKSPACE_DIR/.tmp/storage" "$WORKSPACE_DIR/.tmp/runroot"

echo "⚙️  Initializing fresh Podman storage..."
if command -v podman &> /dev/null; then
    podman info > /dev/null || true
elif [ -f "$HOME/bin/podman" ]; then
    "$HOME/bin/podman" info > /dev/null || true
fi

echo "✅ Deadlock resolved. Podman storage has been reset."
echo "You can now safely restart your stacks (e.g., make up)."
