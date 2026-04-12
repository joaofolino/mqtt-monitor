"""Main runtime loop for mqtt-monitor v2."""

from __future__ import annotations

import os
import time

import paho.mqtt.client as mqtt

from .config import load_config
from .home_assistant import notify_issue_transitions, publish_snapshot, setup_sensors, write_dashboard_file
from .issues import apply_issue_transitions, evaluate_snapshot_issues, load_issue_state, save_issue_state
from .models import Issue, MonitorState
from .system import check_package_version, collect_snapshot, connect_mqtt, log, setup_logging


def known_issues_from_state(persisted_issue_state: dict[str, dict]) -> list[Issue]:
    """Convert persisted issue dictionaries back into Issue objects."""
    issues = []
    for issue_data in persisted_issue_state.values():
        issues.append(
            Issue(
                key=issue_data["key"],
                title=issue_data["title"],
                description=issue_data["description"],
                severity=issue_data["severity"],
                resource_type=issue_data["resource_type"],
                resource_name=issue_data["resource_name"],
                active=issue_data.get("active", False),
                started_at=issue_data.get("started_at"),
                resolved_at=issue_data.get("resolved_at"),
            )
        )
    return issues


def learn_expected_containers(config, snapshot, state: MonitorState):
    """Learn expected LXC containers from autostarted or previously running containers."""
    if config.expected_lxc_containers:
        return
    learned = list(state.discovered_expected_lxc_containers)
    for container in snapshot.containers:
        if container.autostart or container.running:
            learned.append(container.name)
    state.discovered_expected_lxc_containers = sorted(set(learned))


def main():
    """Run the main monitoring loop."""
    config = load_config(os.getenv("CONFIG_PATH", "/etc/mqtt-monitor"))
    logger = setup_logging(config)
    state = MonitorState(persisted_issue_state=load_issue_state(config))
    client = mqtt.Client()
    connect_mqtt(config, logger, client)
    if check_package_version(config, logger):
        return

    while True:
        try:
            snapshot = collect_snapshot(config, logger, state)
            learn_expected_containers(config, snapshot, state)
            current_issues = evaluate_snapshot_issues(config, snapshot, state)
            issues = apply_issue_transitions(snapshot, current_issues, state)
            setup_sensors(config, client, snapshot=snapshot, issues=known_issues_from_state(state.persisted_issue_state))
            publish_snapshot(config, logger, client, snapshot, issues)
            write_dashboard_file(config, snapshot, state)
            notify_issue_transitions(config, logger, issues)
            save_issue_state(config, state.persisted_issue_state)
            time.sleep(config.sleep_interval)
        except Exception as error:  # pylint: disable=broad-except
            log(config, logger, f"Unhandled error in monitoring loop: {error}", "ERROR")
            time.sleep(config.error_sleep)
