# Rootless Podman Cheatsheet & Guide

This guide is designed for developers running containers in a **rootless** (non-root) environment using **Podman** as a modern, secure, daemonless alternative to Docker.

---

## 🚀 Why Podman? (Rootless & Daemonless)

Unlike Docker, Podman does not rely on a background daemon running as `root` (the Docker daemon). 
* **Security:** Containers run entirely within your unprivileged user space. Even if a container is compromised, the attacker has no root access to the host system.
* **No Daemon:** There is no single point of failure. Each container runs as a direct child process of your shell session.
* **Compatibility:** Podman commands are 100% syntactically identical to Docker commands. You can even set up a shell alias:
  ```bash
  alias docker=podman
  ```

---

## 📂 Rootless Storage Locations & Customizing to `.tmp`

By default, rootless Podman stores all assets inside your user home directory:
* **Container Images & Volumes:** `~/.local/share/containers/storage/`
* **Configuration Files:** `~/.config/containers/`
* **Temporary Files & Runtime States:** `/run/user/<your-uid>/`

### Redirecting Storage & Cache to `.tmp`

If you want to keep all Podman cache and generated files inside your workspace's `.tmp` folder (making them easy to find, manage, or delete at once), you can redirect Podman using the **XDG Base Directory Environment Variables** defined in your `.env` file:

```env
# Podman Storage & Cache Redirects
XDG_DATA_HOME='/path/to/your/workspace/fsds/.tmp/data'
XDG_CONFIG_HOME='/path/to/your/workspace/fsds/.tmp/config'
XDG_CACHE_HOME='/path/to/your/workspace/fsds/.tmp/cache'
```

#### How to Apply the Redirection:

Before running your Podman or compose commands, load and export these variables in your active shell terminal:

```bash
# 1. Load and export the variables from .env
export $(grep -v '^#' .env | xargs)

# 2. Run Podman (it will now write entirely into .tmp/)
podman-compose up -d
```

To verify that the redirection has taken effect, run:
```bash
podman info --format '{{.Store.GraphRoot}}'
```
It will output `/path/to/your/workspace/fsds/.tmp/data/containers/storage` instead of your home directory.

---

## 🛠️ Essential Podman Commands

### 1. Basic Operations
| Command | Description |
| :--- | :--- |
| `podman run -d --name <name> -p <host>:<container> <image>` | Run a container in the background |
| `podman ps` | List all running containers |
| `podman ps -a` | List all containers (running and stopped) |
| `podman logs -f <container>` | Follow container logs in real-time |
| `podman stop <container>` | Stop a running container |
| `podman start <container>` | Start a stopped container |
| `podman rm <container>` | Remove a stopped container (`-f` to force remove running) |

### 2. Image Management
| Command | Description |
| :--- | :--- |
| `podman pull <image>` | Pull an image from registry |
| `podman images` | List locally cached images |
| `podman rmi <image>` | Remove a local image |
| `podman search <term>` | Search registries for an image |

### 3. Inspection & Debugging
| Command | Description |
| :--- | :--- |
| `podman exec -it <container> /bin/bash` | Execute an interactive shell inside a running container |
| `podman inspect <container>` | View low-level details (network, mounts, environment) in JSON |
| `podman top <container>` | Display running processes of a container |
| `podman stats` | Display live resource usage stats (CPU, memory, net, I/O) |

---

## 👥 Rootless Quirks & Best Practices (Important!)

Running rootless containers introduces a few unique behaviors regarding **networking** and **file permissions**.

### 1. Port Bindings (< 1024)
By default on Linux, unprivileged users cannot bind to ports below `1024` (privileged ports).
* **Do not use** `-p 80:80` or `-p 443:443`.
* **Instead, use high-range ports** like `-p 8080:8080`, `-p 9092:9092`, `-p 5432:5432` (which matches our `.env` configuration perfectly).

### 2. Host Directory Volume Mounts (Permission Denied Fixes)
When you bind mount a host directory into a container (`-v /path/to/host:/path/in/container`), the container's internal users may not map directly to your host user.

To fix permission issues, use one of the following strategies:
* **The Keep-ID Flag (Highly Recommended):**
  Tells Podman to map your current host user UID inside the container. This prevents permission issues on files generated/read by the container.
  ```bash
  podman run -d --name my-app -v ./data:/data:userns=keep-id my-image
  ```
* **SELinux Labeling (`:z` or `:Z`):**
  If your host system runs SELinux (RHEL/CentOS/Fedora), add `:z` (shared volume) or `:Z` (private volume) to the volume flag so Podman can update labels automatically.
  ```bash
  podman run -d -v ./data:/data:z my-image
  ```

---

## 🐙 Running Docker Compose with Podman

If your project utilizes a `docker-compose.yaml` file, you can run it using **Podman Compose** or standard **Docker Compose** routed to Podman's UNIX socket.

### Option A: Using `podman-compose` (Python Package)
`podman-compose` is a drop-in Python alternative. To run it:
```bash
# Start all services defined in docker-compose.yaml
podman-compose up -d

# Stop all services
podman-compose down

# View logs for all services
podman-compose logs -f
```

### Option B: Using `docker-compose` with Podman Socket
If standard `docker-compose` is installed, you can configure it to talk directly to Podman's user-level systemd socket:
```bash
# 1. Enable the Podman socket for your user
systemctl --user enable --now podman.socket

# 2. Point Docker Compose to the Podman user socket
export DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock"

# 3. Use standard compose commands normally
docker-compose up -d
```

---

## 🛠️ Makefile Quick-Start (Recommended)

To simplify operations, a `Makefile` is registered in the project root. This automates checking ports, loading environment variables, running compose operations, and cleaning up:

| Command | Action |
| :--- | :--- |
| `make up` | Starts all services in the background under rootless Podman (using local `.tmp/` cache redirects). |
| `make down` | Stops and removes all active containers. |
| `make logs` | Follows logs for all active containers in real-time. |
| `make ps` | Lists status of all running containers. |
| `make check-ports` | Runs the Python environment port availability scanner. |
| `make clean` | Safely resets Podman local storage and wipes the `.tmp/` folder. |

---

## 🧹 System Cleanup Commands
Rootless Podman can accumulate unused layers, stopped containers, and cache. Run this periodically to free up disk space in your home directory:
```bash
# Clean up stopped containers, unused networks, and dangling images
podman system prune -f

# Clean up EVERYTHING, including unused images and volumes
podman system prune -a --volumes -f
```
