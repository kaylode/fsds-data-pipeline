#!/usr/bin/env python3
"""
free_ports.py — Kill any processes occupying the ports declared in .env.

Uses `ss -tlnp` (no root required) to find PIDs, then sends SIGTERM
followed by SIGKILL if the process is still alive after a grace period.
"""
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

# ── Colours ──────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

GRACE_SECONDS = 3   # wait between SIGTERM and SIGKILL


def parse_ports_from_env(env_path: Path) -> dict[str, int]:
    """Return {VAR_NAME: port} for every *_PORT variable in .env."""
    ports: dict[str, int] = {}
    with open(env_path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.strip().split("#", 1)[0].strip()
            if not line or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip().strip("'\"")
            if "PORT" in key:
                try:
                    ports[key] = int(val)
                except ValueError:
                    print(f"{YELLOW}⚠️  Skipping {key}: non-integer value '{val}'{RESET}")
    return ports


def pids_on_port(port: int) -> list[int]:
    """
    Return a list of PIDs listening on *port* using `ss -tlnp`.
    Falls back to `lsof` if ss is unavailable.
    """
    pids: list[int] = []

    # Try ss first (iproute2, always available on modern Linux)
    try:
        out = subprocess.check_output(
            ["ss", "-tlnp", f"sport = :{port}"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        # ss output line looks like:
        #   LISTEN 0  128  *:8086  *:*  users:(("console",pid=12345,fd=3))
        for match in re.finditer(r'pid=(\d+)', out):
            pids.append(int(match.group(1)))
        return pids
    except FileNotFoundError:
        pass  # ss not found, try lsof

    # Fallback: lsof
    try:
        out = subprocess.check_output(
            ["lsof", "-ti", f"tcp:{port}"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        for line in out.splitlines():
            line = line.strip()
            if line.isdigit():
                pids.append(int(line))
    except (FileNotFoundError, subprocess.CalledProcessError):
        pass

    return pids


def process_name(pid: int) -> str:
    """Best-effort process name for display."""
    try:
        return Path(f"/proc/{pid}/comm").read_text().strip()
    except OSError:
        return "?"


def kill_pid(pid: int) -> bool:
    """
    Send SIGTERM; if still alive after GRACE_SECONDS, send SIGKILL.
    Returns True if the process is gone.
    """
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True   # already gone
    except PermissionError:
        return False  # can't kill it

    deadline = time.monotonic() + GRACE_SECONDS
    while time.monotonic() < deadline:
        time.sleep(0.2)
        try:
            os.kill(pid, 0)   # probe: raises if gone
        except ProcessLookupError:
            return True

    # Still alive → SIGKILL
    try:
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.5)
        os.kill(pid, 0)
        return False  # survived SIGKILL (zombie?) — unusual
    except ProcessLookupError:
        return True
    except PermissionError:
        return False


def main() -> None:
    print(f"{BOLD}{CYAN}=== Port Liberator ==={RESET}\n")

    script_dir = Path(__file__).resolve().parent
    env_path   = script_dir.parent.parent / ".env"

    if not env_path.exists():
        print(f"{RED}❌  .env not found at {env_path}{RESET}")
        sys.exit(1)

    print(f"Reading configuration from: {env_path}\n")
    ports = parse_ports_from_env(env_path)

    if not ports:
        print(f"{YELLOW}⚠️  No port definitions found in .env.{RESET}")
        sys.exit(0)

    freed       = 0
    already_free = 0
    failed      = 0
    max_len     = max(len(k) for k in ports)

    for key, port in sorted(ports.items()):
        key_padded = key.ljust(max_len)
        pids = pids_on_port(port)

        if not pids:
            print(f"  ✅  {BOLD}{key_padded}{RESET} ({port:5}): already free")
            already_free += 1
            continue

        names = ", ".join(f"{process_name(p)}[{p}]" for p in pids)
        print(f"  🔪  {BOLD}{key_padded}{RESET} ({port:5}): killing {names}", end=" … ", flush=True)

        all_dead = all(kill_pid(p) for p in pids)
        if all_dead:
            print(f"{GREEN}{BOLD}freed{RESET}")
            freed += 1
        else:
            print(f"{RED}{BOLD}FAILED (permission denied?){RESET}")
            failed += 1

    print("\n" + "=" * 42)
    parts = []
    if freed:
        parts.append(f"{GREEN}{BOLD}{freed} freed{RESET}")
    if already_free:
        parts.append(f"{GREEN}{already_free} already free{RESET}")
    if failed:
        parts.append(f"{RED}{BOLD}{failed} failed{RESET}")
    print("  " + "  |  ".join(parts))

    if failed:
        print(f"\n{RED}Some ports could not be freed (try with sudo).{RESET}")
        sys.exit(1)
    else:
        print(f"\n{GREEN}{BOLD}✅  All ports are now free.{RESET}")
        sys.exit(0)


if __name__ == "__main__":
    main()
