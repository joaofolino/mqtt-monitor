"""Issue evaluation and persistence for mqtt-monitor v2."""

from __future__ import annotations

import json
import os
from dataclasses import asdict

from .models import Config, Issue, MonitorSnapshot, MonitorState


SEVERITY_RANK = {
    "info": 0,
    "warning": 1,
    "critical": 2,
}


def issue_state_path(config: Config) -> str:
    """Return the persisted issue state path."""
    return os.path.join(config.state_dir, "issue-state.json")


def ensure_state_dir(config: Config):
    """Ensure the runtime state directory exists."""
    os.makedirs(config.state_dir, exist_ok=True)


def load_issue_state(config: Config) -> dict[str, dict]:
    """Load persisted issue state from disk."""
    ensure_state_dir(config)
    state_path = issue_state_path(config)
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as file_handle:
            return json.load(file_handle)
    except (OSError, json.JSONDecodeError):
        return {}


def save_issue_state(config: Config, persisted_issue_state: dict[str, dict]):
    """Save issue state to disk."""
    ensure_state_dir(config)
    with open(issue_state_path(config), "w", encoding="utf-8") as file_handle:
        json.dump(persisted_issue_state, file_handle, indent=2, sort_keys=True)


def severity_value(severity: str) -> int:
    """Return a sortable numeric severity."""
    return SEVERITY_RANK.get(severity, -1)


def build_issue(
    key: str,
    title: str,
    description: str,
    severity: str,
    resource_type: str,
    resource_name: str,
) -> Issue:
    """Create an active issue candidate."""
    return Issue(
        key=key,
        title=title,
        description=description,
        severity=severity,
        resource_type=resource_type,
        resource_name=resource_name,
        active=True,
    )


def add_issue_if(candidates: list[Issue], condition: bool, **issue_kwargs):
    """Append an issue when the condition is true."""
    if condition:
        candidates.append(build_issue(**issue_kwargs))


def evaluate_snapshot_issues(config: Config, snapshot: MonitorSnapshot, state: MonitorState) -> list[Issue]:
    """Evaluate active issues from the current snapshot."""
    candidates: list[Issue] = []

    usage_warning_threshold = max(int(config.md1_usage_threshold - 10), 80)
    for filesystem in snapshot.filesystems:
        add_issue_if(
            candidates,
            filesystem.usage_percentage >= config.md1_usage_threshold,
            key=f"filesystem/{filesystem.slug}/critical_usage",
            title=f"{filesystem.mount_point} usage critical",
            description=(
                f"{filesystem.mount_point} is at {filesystem.usage_percentage}% used "
                f"with {filesystem.free_space} free"
            ),
            severity="critical",
            resource_type="filesystem",
            resource_name=filesystem.mount_point,
        )
        add_issue_if(
            candidates,
            usage_warning_threshold <= filesystem.usage_percentage < config.md1_usage_threshold,
            key=f"filesystem/{filesystem.slug}/high_usage",
            title=f"{filesystem.mount_point} usage high",
            description=(
                f"{filesystem.mount_point} is at {filesystem.usage_percentage}% used "
                f"with {filesystem.free_space} free"
            ),
            severity="warning",
            resource_type="filesystem",
            resource_name=filesystem.mount_point,
        )

    for raid in snapshot.raids:
        add_issue_if(
            candidates,
            raid.status != "active",
            key=f"raid/{raid.name}/status",
            title=f"RAID {raid.name} degraded",
            description=f"RAID {raid.name} status is {raid.status}. Failed devices: {', '.join(raid.failed_devices) or 'unknown'}",
            severity="critical",
            resource_type="raid_array",
            resource_name=raid.name,
        )

    for disk in snapshot.disks:
        add_issue_if(
            candidates,
            disk.availability != "online",
            key=f"disk/{disk.name}/availability",
            title=f"Disk {disk.name} unavailable",
            description=f"Disk {disk.name} is reporting {disk.availability}",
            severity="critical",
            resource_type="disk",
            resource_name=disk.name,
        )
        add_issue_if(
            candidates,
            disk.smart_health_status == "FAILED" or disk.disk_failing,
            key=f"disk/{disk.name}/smart_failure",
            title=f"Disk {disk.name} SMART failure",
            description=f"Disk {disk.name} reported SMART status {disk.smart_health_status or 'unknown'}",
            severity="critical",
            resource_type="disk",
            resource_name=disk.name,
        )

        health_flags = []
        if disk.prefail_attributes_threshold:
            health_flags.append("prefail threshold exceeded")
        if disk.past_attributes_threshold:
            health_flags.append("past threshold attribute")
        if disk.device_error_log_errors:
            health_flags.append("device error log errors")
        if disk.self_test_log_errors:
            health_flags.append("self test errors")
        if disk.smart_command_failed:
            health_flags.append("SMART command failed")
        add_issue_if(
            candidates,
            bool(health_flags),
            key=f"disk/{disk.name}/smart_warning",
            title=f"Disk {disk.name} SMART warning",
            description=f"Disk {disk.name} has SMART warnings: {', '.join(health_flags)}",
            severity="warning",
            resource_type="disk",
            resource_name=disk.name,
        )

        add_issue_if(
            candidates,
            disk.max_temperature is not None and disk.max_temperature >= 70,
            key=f"disk/{disk.name}/temperature_critical",
            title=f"Disk {disk.name} temperature critical",
            description=f"Disk {disk.name} reached {disk.max_temperature} C",
            severity="critical",
            resource_type="disk",
            resource_name=disk.name,
        )
        add_issue_if(
            candidates,
            disk.max_temperature is not None and 60 <= disk.max_temperature < 70,
            key=f"disk/{disk.name}/temperature_warning",
            title=f"Disk {disk.name} temperature high",
            description=f"Disk {disk.name} reached {disk.max_temperature} C",
            severity="warning",
            resource_type="disk",
            resource_name=disk.name,
        )
        add_issue_if(
            candidates,
            disk.name == "md1" and disk.write_kilobytes_per_second > config.md1_write_spike_threshold,
            key=f"disk/{disk.name}/write_spike",
            title=f"Disk {disk.name} write spike",
            description=(
                f"Disk {disk.name} write rate is {disk.write_kilobytes_per_second:.2f} KB/s "
                f"over the {config.md1_write_spike_threshold:.2f} KB/s threshold"
            ),
            severity="warning",
            resource_type="disk",
            resource_name=disk.name,
        )

    if snapshot.host.cpu_temperature_celsius is not None:
        add_issue_if(
            candidates,
            snapshot.host.cpu_temperature_celsius >= 90,
            key="host/cpu_temperature/critical",
            title="CPU temperature critical",
            description=f"CPU temperature is {snapshot.host.cpu_temperature_celsius:.1f} C",
            severity="critical",
            resource_type="host",
            resource_name="cpu",
        )
        add_issue_if(
            candidates,
            80 <= snapshot.host.cpu_temperature_celsius < 90,
            key="host/cpu_temperature/warning",
            title="CPU temperature high",
            description=f"CPU temperature is {snapshot.host.cpu_temperature_celsius:.1f} C",
            severity="warning",
            resource_type="host",
            resource_name="cpu",
        )

    expected_container_names = set(config.expected_lxc_containers or state.discovered_expected_lxc_containers)
    for container in snapshot.containers:
        should_be_running = container.name in expected_container_names or container.autostart
        add_issue_if(
            candidates,
            should_be_running and not container.running,
            key=f"container/{container.name}/stopped",
            title=f"Container {container.name} stopped",
            description=f"Container {container.name} is {container.status.lower()} but is expected to be running",
            severity="warning",
            resource_type="container",
            resource_name=container.name,
        )

    deduped: dict[str, Issue] = {}
    for issue in candidates:
        current = deduped.get(issue.key)
        if current is None or severity_value(issue.severity) > severity_value(current.severity):
            deduped[issue.key] = issue
    return sorted(deduped.values(), key=lambda issue: (-severity_value(issue.severity), issue.title))


def apply_issue_transitions(snapshot: MonitorSnapshot, active_issues: list[Issue], state: MonitorState) -> list[Issue]:
    """Apply issue transitions using the persisted state."""
    previous_state = state.persisted_issue_state
    next_state: dict[str, dict] = {}
    merged_issues: list[Issue] = []

    for issue in active_issues:
        previous = previous_state.get(issue.key, {})
        issue.started_at = previous.get("started_at") if previous.get("active") else snapshot.generated_at
        issue.newly_active = not previous.get("active", False)
        issue.resolved_at = None
        next_state[issue.key] = asdict(issue)
        merged_issues.append(issue)

    for key, previous in previous_state.items():
        if previous.get("active") and key not in next_state:
            resolved_issue = Issue(
                key=key,
                title=previous["title"],
                description=previous["description"],
                severity=previous["severity"],
                resource_type=previous["resource_type"],
                resource_name=previous["resource_name"],
                active=False,
                started_at=previous.get("started_at"),
                resolved_at=snapshot.generated_at,
                newly_resolved=True,
            )
            next_state[key] = asdict(resolved_issue)
            merged_issues.append(resolved_issue)
        elif key not in next_state:
            next_state[key] = previous

    state.persisted_issue_state = next_state
    return sorted(
        merged_issues,
        key=lambda issue: (not issue.active, -severity_value(issue.severity), issue.title),
    )


def active_issues(issues: list[Issue]) -> list[Issue]:
    """Return currently active issues."""
    return [issue for issue in issues if issue.active]


def highest_active_severity(issues: list[Issue]) -> str:
    """Return the highest severity among active issues."""
    active = active_issues(issues)
    if not active:
        return "none"
    return max(active, key=lambda issue: severity_value(issue.severity)).severity
