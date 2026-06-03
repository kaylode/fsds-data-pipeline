#!/usr/bin/env bash
# scripts/podman/run_podman.sh
# Centralized control script for FSDS container stacks under rootless Podman.
# Usage:
#   run_podman.sh [core|sglang] [command] [args...]
# Default stack is 'core', default command is 'up'.

# Get project directories
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
WORKSPACE_DIR="$( cd "$PROJECT_ROOT/.." && pwd )"

# Go to project root
cd "$PROJECT_ROOT"

# Parse Stack and Command
STACK="core"
COMMAND=""
SHIFT_COUNT=0

if [ "$1" = "core" ] || [ "$1" = "sglang" ] || [ "$1" = "storage" ] || [ "$1" = "airflow" ] || [ "$1" = "datahub" ]; then
    STACK="$1"
    COMMAND="${2:-up}"
    SHIFT_COUNT=2
else
    # Default to core stack, first argument is the command
    COMMAND="${1:-up}"
    SHIFT_COUNT=1
fi

# Safely shift arguments for additional parameters to compose
if [ $# -ge $SHIFT_COUNT ]; then
    shift $SHIFT_COUNT
else
    shift $#
fi

# Ensure .tmp directories exist
mkdir -p "$WORKSPACE_DIR/.tmp/data" "$WORKSPACE_DIR/.tmp/config/containers" "$WORKSPACE_DIR/.tmp/cache"

# Restore rootless config files if missing (e.g. after a clean)
if [ ! -f "$WORKSPACE_DIR/.tmp/config/containers/containers.conf" ] && [ -f "$HOME/.config/containers/containers.conf" ]; then
    echo "⚙️ Restoring custom rootless Podman configurations from $HOME/.config/containers/..."
    mkdir -p "$WORKSPACE_DIR/.tmp/config/containers"
    cp "$HOME/.config/containers/containers.conf" "$WORKSPACE_DIR/.tmp/config/containers/containers.conf"
    cp "$HOME/.config/containers/storage.conf" "$WORKSPACE_DIR/.tmp/config/containers/storage.conf"
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

# Ensure both podman-compose and podman use the exact same base podman binary and rootless configurations
export PODMAN="$HOME/bin/podman"
export CONTAINERS_CONF="$WORKSPACE_DIR/.tmp/config/containers/containers.conf"
export CONTAINERS_STORAGE_CONF="$WORKSPACE_DIR/.tmp/config/containers/storage.conf"
export XDG_RUNTIME_DIR="$WORKSPACE_DIR/.tmp/run"
mkdir -p -m 700 "$XDG_RUNTIME_DIR"

# Configure stack-specific variables
if [ "$STACK" = "sglang" ]; then
    COMPOSE_FILE="config/docker-compose-sglang.yaml"
    SERVICE_NAME="SGLang"
elif [ "$STACK" = "airflow" ]; then
    COMPOSE_FILE="config/orchestration/docker-compose-airflow.yaml"
    SERVICE_NAME="Airflow"
elif [ "$STACK" = "datahub" ]; then
    COMPOSE_FILE="config/docker-compose-datahub.yaml"
    SERVICE_NAME="DataHub"
elif [ "$STACK" = "storage" ]; then
    echo "⚠️ Stack 'storage' has been merged into 'core'. Running 'core' stack instead."
    COMPOSE_FILE="config/docker-compose.yaml"
    SERVICE_NAME="Core FSDS"
else
    COMPOSE_FILE="config/docker-compose.yaml"
    SERVICE_NAME="Core FSDS"
fi

if [ ! -f "$COMPOSE_FILE" ]; then
    echo "❌ Error: Compose file not found at $PROJECT_ROOT/$COMPOSE_FILE"
    exit 1
fi

# Pretty-print available models if starting SGLang server
if [ "$STACK" = "sglang" ] && [ "$COMMAND" = "up" ]; then
    BOLD='\033[1m'
    GREEN='\033[0;32m'
    CYAN='\033[0;36m'
    NC='\033[0m' # No Color

    echo -e "${BOLD}${CYAN}=================================================================${NC}"
    echo -e "${BOLD}📂 AVAILABLE CLUSTER MODELS (/home/support/llm):${NC}"
    echo -e "${BOLD}${CYAN}=================================================================${NC}"
    if [ -d "/home/support/llm" ]; then
        for dir in /home/support/llm/*; do
            if [ -d "$dir" ] && [ "$(basename "$dir")" != "lfs" ] && [ "$(basename "$dir")" != "lost+found" ] && [ "$(basename "$dir")" != "README" ]; then
                echo -e "  ${GREEN}•${NC} ${BOLD}$(basename "$dir")${NC}"
            fi
        done
    else
        echo -e "  ${BOLD}(No cluster models found at /home/support/llm)${NC}"
    fi
    echo -e "${BOLD}${CYAN}=================================================================${NC}"
    echo ""
fi

# Add --image-volume=ignore only for run/up commands to prevent named volume errors
EXTRA_ARGS=()
if [ "$COMMAND" = "up" ] || [ "$COMMAND" = "run" ]; then
    EXTRA_ARGS+=( "--podman-run-args=--image-volume=ignore" )
fi

# Detect compose runner and execute command
if [ -f "$PROJECT_ROOT/.venv/bin/podman-compose" ]; then
    echo "🚀 [$SERVICE_NAME] Using virtual environment podman-compose via uv..."
    uv run python -m podman_compose "${EXTRA_ARGS[@]}" -f "$COMPOSE_FILE" "$COMMAND" "$@"
elif command -v podman-compose &> /dev/null; then
    echo "🚀 [$SERVICE_NAME] Using system podman-compose to run '$COMMAND'..."
    podman-compose "${EXTRA_ARGS[@]}" -f "$COMPOSE_FILE" "$COMMAND" "$@"
elif podman compose version &> /dev/null; then
    echo "🚀 [$SERVICE_NAME] Using podman compose to run '$COMMAND'..."
    podman compose -f "$COMPOSE_FILE" "$COMMAND" "$@"
else
    echo "❌ Error: Neither 'podman-compose' nor the 'podman compose' command could be found."
    echo "Please ensure podman-compose is installed in your environment."
    exit 1
fi
