#!/usr/bin/env bash

# Get project directories
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"

# Go to project root
cd "$PROJECT_ROOT"

# Ensure .tmp directories exist
mkdir -p ../.tmp/data ../.tmp/config/containers ../.tmp/cache

# Restore rootless config files if missing (e.g. after a clean)
if [ ! -f "$PROJECT_ROOT/../.tmp/config/containers/containers.conf" ] && [ -f "$HOME/.config/containers/containers.conf" ]; then
    echo "⚙️ Restoring custom rootless Podman configurations from $HOME/.config/containers/..."
    mkdir -p "$PROJECT_ROOT/../.tmp/config/containers"
    cp "$HOME/.config/containers/containers.conf" "$PROJECT_ROOT/../.tmp/config/containers/containers.conf"
    cp "$HOME/.config/containers/storage.conf" "$PROJECT_ROOT/../.tmp/config/containers/storage.conf"
fi

# Verify .env exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found at $PROJECT_ROOT/.env"
    exit 1
fi

# Ensure user bin directories are in PATH
export PATH="$HOME/bin:$HOME/.local/bin:$PATH"

# Export all environment variables in .env cleanly preserving quoted spaces
set -a
source .env
set +a


# Default to "up" command if none is provided
COMMAND=${1:-up}

# Define the location of compose file
COMPOSE_FILE="config/docker-compose.yaml"

if [ ! -f "$COMPOSE_FILE" ]; then
    echo "❌ Error: Compose file not found at $PROJECT_ROOT/$COMPOSE_FILE"
    exit 1
fi

# Add --image-volume=ignore only for run/up commands to prevent named volume errors
EXTRA_ARGS=()
if [ "$COMMAND" = "up" ] || [ "$COMMAND" = "run" ]; then
    EXTRA_ARGS+=( "--podman-run-args=--image-volume=ignore" )
fi

# Detect which command to use
if [ -f "$PROJECT_ROOT/.venv/bin/podman-compose" ]; then
    echo "🚀 Using virtual environment podman-compose via uv..."
    uv run python -m podman_compose "${EXTRA_ARGS[@]}" -f "$COMPOSE_FILE" "$COMMAND" "${@:2}"
elif command -v podman-compose &> /dev/null; then
    echo "🚀 Using system podman-compose to run '$COMMAND'..."
    podman-compose "${EXTRA_ARGS[@]}" -f "$COMPOSE_FILE" "$COMMAND" "${@:2}"
elif podman compose version &> /dev/null; then
    echo "🚀 Using podman compose to run '$COMMAND'..."
    podman compose -f "$COMPOSE_FILE" "$COMMAND" "${@:2}"
else
    echo "❌ Error: Neither 'podman-compose' nor the 'podman compose' command could be found."
    echo "Please ensure podman-compose is installed in your environment."
    exit 1
fi
