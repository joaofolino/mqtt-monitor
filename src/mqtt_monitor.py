#!/usr/bin/env python3
"""Compatibility entrypoint for mqtt-monitor."""

from __future__ import annotations

import os
import sys


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SHARE_DIR = "/usr/share/mqtt-monitor"
for candidate_path in [CURRENT_DIR, SHARE_DIR]:
    if candidate_path not in sys.path and os.path.isdir(candidate_path):
        sys.path.insert(0, candidate_path)

from mqtt_monitor_v2.config import load_config  # noqa: E402
from mqtt_monitor_v2.engine import main  # noqa: E402
from mqtt_monitor_v2.home_assistant import notify_issue_transitions, publish_data, publish_snapshot, setup_sensors  # noqa: E402
from mqtt_monitor_v2.issues import apply_issue_transitions, evaluate_snapshot_issues, load_issue_state, save_issue_state  # noqa: E402
from mqtt_monitor_v2.models import Config, MonitorConfig, MonitorState  # noqa: E402
from mqtt_monitor_v2.system import (  # noqa: E402
    check_package_version,
    collect_snapshot,
    connect_mqtt,
    disk_cache,
    empty_io_stats,
    get_capacity_gib,
    get_disk_info,
    get_disk_io,
    get_raid_status,
    get_usage,
    log,
    on_connect,
    parse_mdstat_devices,
    parse_smart_output,
    parse_smartctl_exit_code,
    run_cmd,
    send_alert,
    setup_logging,
)


if __name__ == "__main__":
    main()
