# 📖 CLAUDE.md — Agent & Developer Handbook

This document outlines the core architecture commands, operational rules, and code style requirements of the Full-Stack Data Streaming (FSDS) cluster project.

---

## 🛠️ Development & Operational Commands

### 🐳 Container Orchestration (Podman Compose)
* **Check occupied ports**: `make check-ports`
* **Boot cluster**: `make up`
* **Check status**: `make ps`
* **Follow logs**: `make logs`
* **Stop cluster**: `make down`
* **Wipe all caches/volumes**: `make clean`
* **Boot isolated vLLM Inference**: `make vllm-up`
* **Follow isolated vLLM logs**: `make vllm-logs`
* **Stop isolated vLLM container**: `make vllm-down`
* **Query/Test vLLM API server**: `make vllm-test`

### 🐍 Python Execution & Scripting (Always via `uv`)
* **Run meds loader demo**: `uv run python core/meds_loader.py`
* **Run ehr simulator (Python 3.12 managed runtime)**:
  `uv run --python 3.12 python scripts/data/ehr_simulator.py --num_subjects 100 --speedup 86400`
* **Run baseline CDC consumer**: `uv run python resources/l7_ingestion_layer/cdc_demo/cdc.py`
* **Run database mock ingester**: `uv run python resources/l7_ingestion_layer/cdc_demo/db_ingestion.py`

---

## 📋 Coding Guidelines & Rules

Any agent or developer modifying the codebase must strictly follow these constraints:

### 1. Package Installation and Run Execution
* ALWAYS use **`uv`** for dependency management.
  * *Add dependency*: `uv add <package>`
  * *Run script*: `uv run python <script.py>`
* Never run pip install directly or invoke raw `python`/`python3`.

### 2. Logging & System Tracing (Loguru)
* ALWAYS use **`loguru`** for all Python logging.
* Import standard log object: `from loguru import logger`
* Use appropriate log levels (`logger.debug`, `logger.info`, `logger.warning`, `logger.error`, `logger.exception`) instead of raw `print()` statements.

### 3. Environment Config Parameterization
* Parameterize **only necessary host-specific and sensitive environment configurations** in `.env` (e.g. ports, local directories, db passwords).
* Keep `.env` simple and well-documented. Do not clutter it with application constants.
* Standard local runtime directories must be kept under `.tmp/` inside the local repository.
