#!/usr/bin/env python3
"""Return classic-LXC inventory as JSON for mqtt-monitor."""

from __future__ import annotations

import json
import os
import subprocess
import sys


LXC_PATH = "/var/lib/lxc"


def read_autostart(config_path: str) -> bool:
    """Return whether an LXC config enables autostart."""
    try:
        with open(config_path, "r", encoding="utf-8") as file_handle:
            for raw_line in file_handle:
                line = raw_line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                key, value = [part.strip() for part in line.split("=", 1)]
                if key == "lxc.start.auto":
                    return value.lower() in {"1", "true", "yes", "on"}
    except OSError:
        return False
    return False


def list_container_names(base_path: str) -> list[str]:
    """Return sorted classic-LXC container names from the configured path."""
    if not os.path.isdir(base_path):
        return []
    names = []
    for name in sorted(os.listdir(base_path)):
        config_path = os.path.join(base_path, name, "config")
        if os.path.isfile(config_path):
            names.append(name)
    return names


def read_container_status(name: str, base_path: str) -> str:
    """Return the runtime state for a classic-LXC container."""
    try:
        result = subprocess.run(
            ["lxc-info", "-n", name, "-P", base_path, "-sH"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.SubprocessError, subprocess.TimeoutExpired):
        return "UNKNOWN"

    status = result.stdout.strip().upper()
    if result.returncode != 0 or not status:
        return "UNKNOWN"
    return status


def collect_inventory(base_path: str = LXC_PATH) -> list[dict[str, object]]:
    """Collect container inventory suitable for JSON output."""
    containers = []
    for name in list_container_names(base_path):
        config_path = os.path.join(base_path, name, "config")
        containers.append({
            "name": name,
            "status": read_container_status(name, base_path),
            "autostart": read_autostart(config_path),
        })
    return containers


def main() -> int:
    """Print the current LXC inventory as JSON."""
    json.dump({"containers": collect_inventory()}, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
