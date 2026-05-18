#!/usr/bin/env bash

# Get project directories
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

# Go to project root
cd "$PROJECT_ROOT"

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
    set -a && source .env && set +a
    podman system reset -f &>/dev/null
fi

echo "🧹 Deleting $PROJECT_ROOT/.tmp..."
rm -rf "$PROJECT_ROOT/.tmp"

echo "🧹 Deleting local tmp directories..."
rm -rf "/tmp/podman-$USER"

echo "✅ Cleanup completed successfully!"
