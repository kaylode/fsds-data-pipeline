#!/usr/bin/env python3
import socket
import sys
from pathlib import Path

# Color and emoji definitions
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

def check_port(port: int) -> bool:
    """Returns True if the port is in use/occupied, False otherwise."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
            return False
        except OSError:
            return True

def main():
    print(f"{BOLD}{CYAN}=== Environment Port Occupancy Checker ==={RESET}\n")

    # Locate the .env file at the project root (two levels up from scripts/misc/)
    script_dir = Path(__file__).resolve().parent
    env_path = script_dir.parent.parent / ".env"

    if not env_path.exists():
        print(f"{RED}❌ Error: .env file not found at {env_path}{RESET}")
        sys.exit(1)

    print(f"Reading configuration from: {env_path}")
    
    # Parse port configurations
    ports_to_check = {}
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                # Remove inline comments
                line = line.split("#", 1)[0].strip()
                if not line:
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip()
                    if key.endswith("_PORT") or "PORT" in key:
                        # Strip single or double quotes
                        val = val.strip("'\"")
                        try:
                            port_val = int(val)
                            ports_to_check[key] = port_val
                        except ValueError:
                            print(f"{YELLOW}⚠️  Warning: Line {line_num} has non-integer port value '{val}' for {key}. Skipping.{RESET}")
    except Exception as e:
        print(f"{RED}❌ Error reading .env file: {e}{RESET}")
        sys.exit(1)

    if not ports_to_check:
        print(f"{YELLOW}⚠️  No port definitions found in the .env file!{RESET}")
        sys.exit(0)

    print(f"Found {len(ports_to_check)} ports to check...\n")

    occupied_count = 0
    max_len = max(len(k) for k in ports_to_check.keys())

    for key, port in sorted(ports_to_check.items()):
        is_occupied = check_port(port)
        key_padded = key.ljust(max_len)
        if is_occupied:
            status = f"{RED}{BOLD}OCCUPIED{RESET}"
            icon = "❌"
            occupied_count += 1
        else:
            status = f"{GREEN}{BOLD}FREE    {RESET}"
            icon = "✅"
        
        print(f" {icon}  {BOLD}{key_padded}{RESET} (Port {port:5}): {status}")

    print("\n" + "=" * 42)
    if occupied_count > 0:
        print(f"\n{RED}{BOLD}❌ Status: FAILED{RESET}")
        print(f"{RED}{occupied_count} port(s) are currently occupied. Please free them or update .env before starting services.{RESET}")
        sys.exit(1)
    else:
        print(f"\n{GREEN}{BOLD}✅ Status: SUCCESS{RESET}")
        print(f"{GREEN}All configured ports are free and ready for use!{RESET}")
        sys.exit(0)

if __name__ == "__main__":
    main()
