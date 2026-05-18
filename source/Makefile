.PHONY: up down logs ps clean check-ports install-podman simulate sglang-up sglang-down sglang-logs sglang-test help
 
# Run all containers under rootless Podman with local cache redirection
up:
	@bash scripts/podman/run_podman.sh up -d

# Stop all containers
down:
	@bash scripts/podman/run_podman.sh down

# View container logs
logs:
	@bash scripts/podman/run_podman.sh logs -f

# View container status
ps:
	@bash scripts/podman/run_podman.sh ps

# Run the Python check_ports script
check-ports:
	@~/.local/bin/uv run scripts/misc/check_ports.py

# Clean up all containers, images, and wipe the local .tmp directory
clean:
	@bash scripts/misc/clean_tmp.sh

# Drop the core EHR database tables to reset the simulation environment
clean-db:
	@~/.local/bin/uv run python -c "from core.database import get_db_connection, drop_db_schema; drop_db_schema(get_db_connection())"


# Download and install the full native Podman server package statically without root
install-podman:
	@bash scripts/podman/install_podman_static.sh

simulate:
	~/.local/bin/uv run scripts/data/ehr_simulator.py --num_subjects 50 --speedup 86400

# Isolated SGLang Inference Container Targets
sglang-up:
	@bash scripts/podman/run_vllm.sh up -d

sglang-down:
	@bash scripts/podman/run_vllm.sh down

sglang-logs:
	@bash scripts/podman/run_vllm.sh logs -f

sglang-test:
	@uv run scripts/misc/test_sglang.py

# Help command to show all options
help:
	@echo "Available commands in this Makefile:"
	@echo "  make up             - Start all core FSDS containers in the background"
	@echo "  make down           - Stop and remove all core FSDS containers"
	@echo "  make logs           - Follow logs of all core FSDS containers"
	@echo "  make ps             - List status of all core FSDS containers"
	@echo "  make check-ports    - Check if configured ports are occupied"
	@echo "  make clean          - Reset Podman storage and wipe the .tmp directory"
	@echo "  make install-podman - Install native statically-linked Podman server"
	@echo "  make simulate       - Run EHR simulator script"
	@echo "  make sglang-up      - Start isolated SGLang GPU inference container"
	@echo "  make sglang-down    - Stop isolated SGLang GPU inference container"
	@echo "  make sglang-logs    - Follow logs of the SGLang inference container"
	@echo "  make sglang-test    - Send a test prompt query to the SGLang server"
