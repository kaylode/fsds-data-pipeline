#!/usr/bin/env bash

# Get project directories
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Go to project root
cd "$PROJECT_ROOT"
export XDG_RUNTIME_DIR="$PROJECT_ROOT/../.tmp/run"

FORCE=false
if [ "$1" == "-f" ]; then
    FORCE=true
fi

if [ "$FORCE" = false ]; then
    echo "⚠️  Warning: This will delete the entire .tmp/ directory containing Podman's data, caches, and configuration."
    read -p "Are you sure you want to proceed? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Cleanup aborted."
        exit 0
    fi
fi

# Ensure user bin directories are in PATH
export PATH="$HOME/bin:$HOME/.local/bin:$PATH"

# Reset Podman storage if configured to avoid file lock issues
if command -v podman &> /dev/null && [ -f .env ]; then
    echo "🧹 Resetting Podman system storage for local redirects..."
    pgrep -u "$USER" -f podman
    pkill -9 -u "$USER" -f podman
    set -a && source .env && set +a
    podman system reset -f &>/dev/null
fi

# echo "🧹 Deleting $PROJECT_ROOT/.tmp..."
# rm -rf "$PROJECT_ROOT/.tmp"

# Storage is now fully contained in the workspace .tmp folder

echo "✅ Cleanup completed successfully!"
