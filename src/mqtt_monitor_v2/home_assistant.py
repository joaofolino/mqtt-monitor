"""Home Assistant output support for mqtt-monitor v2."""

from __future__ import annotations

import hashlib
import json
import os
import re

import requests
import yaml

from .issues import active_issues, highest_active_severity, severity_value
from .models import safe_slug


def entity_prefix(config) -> str:
    """Return an entity-safe prefix derived from self_name."""
    prefix = re.sub(r"[^a-z0-9_]+", "_", config.self_name.lower())
    prefix = re.sub(r"_+", "_", prefix).strip("_")
    return prefix or "mqtt_monitor"


def disk_entity_slug(disk_name: str) -> str:
    """Return a Home Assistant-safe disk object identifier suffix."""
    return safe_slug(disk_name)


def compact_path_label(path: str) -> str:
    """Return a shorter label for filesystem paths."""
    if path == "/":
        return "Root"
    parts = [part for part in path.strip("/").split("/") if part]
    if len(parts) <= 2:
        return "/".join(parts)
    return "/".join(parts[-2:])


def gauge_card(entity: str, name: str, icon: str, *, unit: str = "%", maximum: float = 100, yellow: float = 70, red: float = 90) -> dict:
    """Return a standard gauge card definition."""
    return {
        "type": "gauge",
        "entity": entity,
        "name": name,
        "unit": unit,
        "min": 0,
        "max": maximum,
        "severity": {
            "green": 0,
            "yellow": yellow,
            "red": red,
        },
        "needle": True,
        "icon": icon,
    }


def tile_card(entity: str, name: str, icon: str) -> dict:
    """Return a compact tile card definition."""
    return {
        "type": "tile",
        "entity": entity,
        "name": name,
        "icon": icon,
    }


def entities_card(title: str, entities: list[str]) -> dict:
    """Return a Home Assistant entities card."""
    return {
        "type": "entities",
        "title": title,
        "show_header_toggle": False,
        "entities": entities,
    }


def chunked(values: list, size: int) -> list[list]:
    """Return values split into fixed-size chunks."""
    return [values[index:index + size] for index in range(0, len(values), size)]


def is_logical_disk(disk) -> bool:
    """Return whether a disk represents a logical array or mapper-backed volume."""
    return disk.path.startswith("/dev/md") or disk.path.startswith("/dev/mapper/")


def disk_entities(disk, include_io: bool) -> list[str]:
    """Return the relevant entity ids for a disk card."""
    entities = [f"sensor.disk_{disk.slug}_capacity_gib"]
    if disk.smart_health_status is not None:
        entities.append(f"sensor.disk_{disk.slug}_smart_health_status")
    if disk.max_temperature is not None:
        entities.append(f"sensor.disk_{disk.slug}_max_temperature")
    if disk.power_on_hours is not None:
        entities.append(f"sensor.disk_{disk.slug}_power_on_hours")
    if include_io:
        entities.extend([
            f"sensor.disk_{disk.slug}_read_kilobytes_per_second",
            f"sensor.disk_{disk.slug}_write_kilobytes_per_second",
            f"sensor.disk_{disk.slug}_busy_time_percentage",
        ])
    return entities


def build_dashboard_view(config, snapshot) -> dict:
    """Build the main Home Assistant dashboard view."""
    prefix = entity_prefix(config)

    overview_cards = [
        {"type": "heading", "heading": "Overview", "icon": "mdi:server-network"},
        {
            "type": "grid",
            "columns": 2,
            "square": False,
            "cards": [
                tile_card(f"sensor.{prefix}_active_issue_count", "Active Issues", "mdi:alert"),
                tile_card(f"sensor.{prefix}_highest_issue_severity", "Highest Severity", "mdi:alert-circle-outline"),
                gauge_card(
                    f"sensor.{prefix}_cpu_load_percentage",
                    "CPU Load",
                    "mdi:cpu-64-bit",
                    maximum=100,
                    yellow=50,
                    red=80,
                ),
                gauge_card(
                    f"sensor.{prefix}_cpu_temperature_celsius",
                    "CPU Temp",
                    "mdi:thermometer",
                    unit="C",
                    maximum=100,
                    yellow=60,
                    red=80,
                ),
                tile_card(f"sensor.{prefix}_memory_used_megabytes", "Memory Used", "mdi:memory"),
            ],
        },
        {
            "type": "markdown",
            "title": "Active Issues",
            "content": "\n".join([
                f"{{% set generated = state_attr('sensor.{prefix}_active_issue_count', 'generated_at') %}}",
                f"{{% set issues = state_attr('sensor.{prefix}_active_issue_count', 'active_issues') or [] %}}",
                "{% if issues | count == 0 %}",
                "All monitored resources are reporting normally.",
                "{% else %}",
                "{% for issue in issues %}",
                "- **{{ issue.title }}** ({{ issue.severity }})",
                "  {{ issue.description }}",
                "{% endfor %}",
                "{% endif %}",
                "{% if generated %}",
                "",
                "_Last update: {{ generated }}_",
                "{% endif %}",
            ]),
        },
    ]

    storage_cards = [{"type": "heading", "heading": "Storage", "icon": "mdi:harddisk"}]
    if snapshot.filesystems:
        storage_cards.append(
            {
                "type": "grid",
                "columns": 2,
                "square": False,
                "cards": [
                    gauge_card(
                        f"sensor.{prefix}_{filesystem.slug}_disk_usage_percentage",
                        compact_path_label(filesystem.mount_point),
                        "mdi:harddisk",
                        maximum=100,
                        yellow=75,
                        red=90,
                    )
                    for filesystem in snapshot.filesystems
                ],
            }
        )
    if snapshot.raids:
        storage_cards.append(
            {
                "type": "grid",
                "columns": 2,
                "square": False,
                "cards": [
                    tile_card(
                        f"sensor.raid_{raid.slug}_status",
                        f"RAID {raid.name}",
                        "mdi:harddisk-plus",
                    )
                    for raid in snapshot.raids
                ],
            }
        )
    container_cards = [{"type": "heading", "heading": "Containers", "icon": "mdi:package-variant-closed"}]
    if snapshot.containers:
        container_cards.append(
            entities_card(
                "LXC Runtime",
                [f"sensor.{prefix}_container_{container.slug}_status" for container in snapshot.containers],
            )
        )
    else:
        container_cards.append({"type": "markdown", "content": "No LXC containers discovered."})

    sections = [
        {"cards": overview_cards},
        {"cards": storage_cards},
        {"cards": container_cards},
    ]

    logical_disks = [disk for disk in snapshot.disks if is_logical_disk(disk)]
    physical_disks = [disk for disk in snapshot.disks if not is_logical_disk(disk)]
    for index, group in enumerate(chunked(physical_disks, 2)):
        sections.append(
            {
                "cards": [
                    {
                        "type": "heading",
                        "heading": "Physical Disks" if index == 0 else "Physical Disks (cont.)",
                        "icon": "mdi:harddisk",
                    },
                    *[
                        entities_card(f"Disk {disk.name}", disk_entities(disk, config.enable_io_mqtt))
                        for disk in group
                    ],
                ]
            }
        )

    for index, group in enumerate(chunked(logical_disks, 2)):
        sections.append(
            {
                "cards": [
                    {
                        "type": "heading",
                        "heading": "Logical Volumes" if index == 0 else "Logical Volumes (cont.)",
                        "icon": "mdi:chart-sankey",
                    },
                    *[
                        entities_card(f"Disk {disk.name}", disk_entities(disk, config.enable_io_mqtt))
                        for disk in group
                    ],
                ]
            }
        )

    return {
        "title": "Server Monitor",
        "path": "server-monitor",
        "icon": "mdi:server-network",
        "type": "sections",
        "max_columns": 3,
        "sections": sections,
    }


def build_dashboard_config(config, snapshot) -> dict:
    """Build the full Home Assistant dashboard config."""
    return {
        "title": "Server Monitor",
        "views": [build_dashboard_view(config, snapshot)],
    }


def monitor_device(config) -> dict:
    """Return the shared Home Assistant device payload."""
    prefix = entity_prefix(config)
    return {
        "identifiers": [f"{prefix}_monitor"],
        "name": f"{config.self_name.title()} Monitor",
        "manufacturer": "mqtt-monitor",
        "model": "mqtt-monitor v2",
    }


def publish_data(config, logger, client, topic, value, retain=False):
    """Publish data to an MQTT topic."""
    try:
        if value is None:
            return
        if isinstance(value, bool):
            value = str(value).lower()
        elif isinstance(value, (int, float)):
            value = str(value)
        client.publish(topic, value, retain=retain)
    except ConnectionError as error:
        logger.error("MQTT publish error %s: %s", topic, error)


def publish_json(client, topic: str, payload: dict | list, retain: bool = True):
    """Publish a JSON payload to MQTT."""
    client.publish(topic, json.dumps(payload), retain=retain)


def publish_discovery(client, component: str, object_id: str, payload: dict):
    """Publish a Home Assistant MQTT discovery payload."""
    client.publish(f"homeassistant/{component}/{object_id}/config", json.dumps(payload), retain=True)


def setup_sensors(config, client, snapshot=None, issues=None):
    """Publish Home Assistant MQTT discovery payloads."""
    device = monitor_device(config)
    prefix = entity_prefix(config)
    host_sensors = [
        ("cpu_load_percentage", "%", "CPU Load Percentage"),
        ("memory_used_megabytes", "MB", "Memory Used Megabytes"),
        ("cpu_temperature_celsius", "C", "CPU Temperature Celsius"),
    ]
    for suffix, unit, title in host_sensors:
        object_id = f"{prefix}_{suffix}"
        publish_discovery(
            client,
            "sensor",
            object_id,
            {
                "name": f"{config.self_name.title()} {title}",
                "object_id": object_id,
                "unique_id": f"{prefix}_monitor_{suffix}",
                "device": device,
                "state_topic": f"{config.topic_prefix}/host/{suffix}",
                "unit_of_measurement": unit,
            },
        )

    mounts = snapshot.filesystems if snapshot else []
    if not mounts:
        mounts = [
            type(
                "FilesystemProxy",
                (),
                {
                    "slug": safe_slug(mount.strip("/").replace("/", "_") or "root"),
                    "mount_point": mount,
                },
            )
            for mount in config.disk_usage_mounts
        ]
    for mount in mounts:
        object_id = f"{prefix}_{mount.slug}_disk_usage_percentage"
        publish_discovery(
            client,
            "sensor",
            object_id,
            {
                "name": f"{config.self_name.title()} {compact_path_label(getattr(mount, 'mount_point', mount.slug)).title()} Disk Usage Percentage",
                "object_id": object_id,
                "unique_id": f"{prefix}_monitor_{mount.slug}_disk_usage_percentage",
                "device": device,
                "state_topic": f"{config.topic_prefix}/host/{mount.slug}_disk_usage_percentage",
                "unit_of_measurement": "%",
                "json_attributes_topic": f"{config.topic_prefix}/host/{mount.slug}_disk_usage_attributes",
            },
        )

    publish_discovery(
        client,
        "sensor",
        f"{prefix}_active_issue_count",
        {
            "name": f"{config.self_name.title()} Active Issue Count",
            "object_id": f"{prefix}_active_issue_count",
            "unique_id": f"{prefix}_monitor_active_issue_count",
            "device": device,
            "state_topic": f"{config.topic_prefix}/issues/active_issue_count",
            "json_attributes_topic": f"{config.topic_prefix}/issues/summary",
            "icon": "mdi:alert",
        },
    )
    publish_discovery(
        client,
        "sensor",
        f"{prefix}_highest_issue_severity",
        {
            "name": f"{config.self_name.title()} Highest Issue Severity",
            "object_id": f"{prefix}_highest_issue_severity",
            "unique_id": f"{prefix}_monitor_highest_issue_severity",
            "device": device,
            "state_topic": f"{config.topic_prefix}/issues/highest_severity",
            "icon": "mdi:alert-circle-outline",
        },
    )

    disk_names = [disk.name for disk in snapshot.disks] if snapshot else [device_path.split("/")[-1] for device_path in config.devices]
    for disk_name in disk_names:
        disk_slug = disk_entity_slug(disk_name)
        metric_specs = [
            ("max_temperature", "C"),
            ("power_on_hours", "h"),
            ("reallocated_sectors", None),
            ("spin_up_count", None),
            ("smart_health_status", None),
            ("capacity_gib", "GiB"),
        ]
        if config.enable_io_mqtt:
            metric_specs.extend([
                ("read_kilobytes_per_second", "KB/s"),
                ("write_kilobytes_per_second", "KB/s"),
                ("read_count_per_second", "ops/s"),
                ("write_count_per_second", "ops/s"),
                ("busy_time_percentage", "%"),
            ])
        for metric_name, unit in metric_specs:
            payload = {
                "name": f"Disk {disk_name} {metric_name.replace('_', ' ').title()}",
                "object_id": f"disk_{disk_slug}_{metric_name}",
                "unique_id": f"{prefix}_monitor_disk_{disk_slug}_{metric_name}",
                "device": device,
                "state_topic": f"{config.topic_prefix}/disk/{disk_name}/{metric_name}",
                "availability_topic": f"{config.topic_prefix}/disk/{disk_name}/availability",
                "payload_available": "online",
                "payload_not_available": "offline",
            }
            if unit is not None:
                payload["unit_of_measurement"] = unit
            if metric_name == "smart_health_status":
                payload["json_attributes_topic"] = f"{config.topic_prefix}/disk/{disk_name}/attributes"
            publish_discovery(client, "sensor", f"disk_{disk_slug}_{metric_name}", payload)

        for flag in [
            "command_line_parse_error",
            "device_open_failed",
            "smart_command_failed",
            "disk_failing",
            "prefail_attributes_threshold",
            "past_attributes_threshold",
            "device_error_log_errors",
            "self_test_log_errors",
        ]:
            publish_discovery(
                client,
                "binary_sensor",
                f"disk_{disk_slug}_{flag}",
                {
                    "name": f"Disk {disk_name} {flag.replace('_', ' ').title()}",
                    "object_id": f"disk_{disk_slug}_{flag}",
                    "unique_id": f"{prefix}_monitor_disk_{disk_slug}_{flag}",
                    "device": device,
                    "state_topic": f"{config.topic_prefix}/disk/{disk_name}/{flag}",
                    "payload_on": "true",
                    "payload_off": "false",
                    "availability_topic": f"{config.topic_prefix}/disk/{disk_name}/availability",
                    "payload_available": "online",
                    "payload_not_available": "offline",
                },
            )

    raid_items = snapshot.raids if snapshot else [type("RaidProxy", (), {"name": raid_name, "slug": safe_slug(raid_name)}) for raid_name in config.raid_arrays]
    for raid in raid_items:
        publish_discovery(
            client,
            "sensor",
            f"raid_{raid.slug}_status",
            {
                "name": f"RAID {raid.name} Status",
                "object_id": f"raid_{raid.slug}_status",
                "unique_id": f"{prefix}_monitor_raid_{raid.slug}_status",
                "device": device,
                "state_topic": f"{config.topic_prefix}/raid/{raid.name}/status",
                "json_attributes_topic": f"{config.topic_prefix}/raid/{raid.name}/attributes",
            },
        )

    containers = snapshot.containers if snapshot else []
    for container in containers:
        publish_discovery(
            client,
            "binary_sensor",
            f"{prefix}_container_{container.slug}_running",
            {
                "name": f"Container {container.name} Running",
                "object_id": f"{prefix}_container_{container.slug}_running",
                "unique_id": f"{prefix}_monitor_container_{container.slug}_running",
                "device": device,
                "state_topic": f"{config.topic_prefix}/container/{container.name}/running",
                "payload_on": "true",
                "payload_off": "false",
                "json_attributes_topic": f"{config.topic_prefix}/container/{container.name}/attributes",
            },
        )
        publish_discovery(
            client,
            "sensor",
            f"{prefix}_container_{container.slug}_status",
            {
                "name": f"Container {container.name} Status",
                "object_id": f"{prefix}_container_{container.slug}_status",
                "unique_id": f"{prefix}_monitor_container_{container.slug}_status",
                "device": device,
                "state_topic": f"{config.topic_prefix}/container/{container.name}/status",
            },
        )

    for issue in issues or []:
        publish_discovery(
            client,
            "binary_sensor",
            f"{prefix}_issue_{issue.slug}_active",
            {
                "name": f"{issue.title} Active",
                "object_id": f"{prefix}_issue_{issue.slug}_active",
                "unique_id": f"{prefix}_monitor_issue_{issue.slug}_active",
                "device": device,
                "state_topic": f"{config.topic_prefix}/issues/{issue.slug}/active",
                "payload_on": "true",
                "payload_off": "false",
                "json_attributes_topic": f"{config.topic_prefix}/issues/{issue.slug}/attributes",
            },
        )


def publish_snapshot(config, logger, client, snapshot, issues):
    """Publish a snapshot and the current issue state to MQTT."""
    publish_data(config, logger, client, f"{config.topic_prefix}/host/cpu_load_percentage", snapshot.host.cpu_load_percentage)
    publish_data(config, logger, client, f"{config.topic_prefix}/host/memory_used_megabytes", snapshot.host.memory_used_megabytes)
    publish_data(
        config,
        logger,
        client,
        f"{config.topic_prefix}/host/cpu_temperature_celsius",
        snapshot.host.cpu_temperature_celsius,
    )

    for filesystem in snapshot.filesystems:
        publish_data(
            config,
            logger,
            client,
            f"{config.topic_prefix}/host/{filesystem.slug}_disk_usage_percentage",
            filesystem.usage_percentage,
            retain=True,
        )
        publish_json(
            client,
            f"{config.topic_prefix}/host/{filesystem.slug}_disk_usage_attributes",
            {
                "mount_point": filesystem.mount_point,
                "free_space": filesystem.free_space,
                "generated_at": snapshot.generated_at,
            },
            retain=True,
        )

    for disk in snapshot.disks:
        publish_data(config, logger, client, f"{config.topic_prefix}/disk/{disk.name}/availability", disk.availability, retain=True)
        publish_data(
            config,
            logger,
            client,
            f"{config.topic_prefix}/disk/{disk.name}/capacity_gib",
            disk.capacity_gib,
            retain=True,
        )
        publish_json(
            client,
            f"{config.topic_prefix}/disk/{disk.name}/attributes",
            {
                "capacity_gib": disk.capacity_gib,
                "capacity_bytes": disk.capacity_bytes,
                "power_on_hours": disk.power_on_hours,
                "generated_at": snapshot.generated_at,
                "path": disk.path,
            },
            retain=True,
        )
        for metric_name in [
            "max_temperature",
            "power_on_hours",
            "reallocated_sectors",
            "spin_up_count",
            "smart_health_status",
            "read_kilobytes_per_second",
            "write_kilobytes_per_second",
            "read_count_per_second",
            "write_count_per_second",
            "busy_time_percentage",
        ]:
            publish_data(
                config,
                logger,
                client,
                f"{config.topic_prefix}/disk/{disk.name}/{metric_name}",
                getattr(disk, metric_name),
                retain=True,
            )
        for flag in [
            "command_line_parse_error",
            "device_open_failed",
            "smart_command_failed",
            "disk_failing",
            "prefail_attributes_threshold",
            "past_attributes_threshold",
            "device_error_log_errors",
            "self_test_log_errors",
        ]:
            publish_data(
                config,
                logger,
                client,
                f"{config.topic_prefix}/disk/{disk.name}/{flag}",
                getattr(disk, flag),
                retain=True,
            )

    for raid in snapshot.raids:
        publish_data(config, logger, client, f"{config.topic_prefix}/raid/{raid.name}/status", raid.status, retain=True)
        publish_json(
            client,
            f"{config.topic_prefix}/raid/{raid.name}/attributes",
            {
                "failed_devices": raid.failed_devices,
                "raw": raid.raw,
                "generated_at": snapshot.generated_at,
            },
            retain=True,
        )

    for container in snapshot.containers:
        publish_data(
            config,
            logger,
            client,
            f"{config.topic_prefix}/container/{container.name}/running",
            container.running,
            retain=True,
        )
        publish_data(
            config,
            logger,
            client,
            f"{config.topic_prefix}/container/{container.name}/status",
            container.status,
            retain=True,
        )
        publish_json(
            client,
            f"{config.topic_prefix}/container/{container.name}/attributes",
            {
                "autostart": container.autostart,
                "generated_at": snapshot.generated_at,
            },
            retain=True,
        )

    current_active_issues = active_issues(issues)
    publish_data(
        config,
        logger,
        client,
        f"{config.topic_prefix}/issues/active_issue_count",
        len(current_active_issues),
        retain=True,
    )
    publish_data(
        config,
        logger,
        client,
        f"{config.topic_prefix}/issues/highest_severity",
        highest_active_severity(issues),
        retain=True,
    )
    publish_json(
        client,
        f"{config.topic_prefix}/issues/summary",
        {
            "generated_at": snapshot.generated_at,
            "active_issues": [
                {
                    "key": issue.key,
                    "title": issue.title,
                    "description": issue.description,
                    "severity": issue.severity,
                    "resource_type": issue.resource_type,
                    "resource_name": issue.resource_name,
                    "started_at": issue.started_at,
                }
                for issue in current_active_issues
            ],
        },
        retain=True,
    )
    for issue in issues:
        publish_data(
            config,
            logger,
            client,
            f"{config.topic_prefix}/issues/{issue.slug}/active",
            issue.active,
            retain=True,
        )
        publish_json(
            client,
            f"{config.topic_prefix}/issues/{issue.slug}/attributes",
            {
                "key": issue.key,
                "title": issue.title,
                "description": issue.description,
                "severity": issue.severity,
                "resource_type": issue.resource_type,
                "resource_name": issue.resource_name,
                "started_at": issue.started_at,
                "resolved_at": issue.resolved_at,
                "active": issue.active,
            },
            retain=True,
        )


def build_dashboard_yaml(config, snapshot) -> str:
    """Build a Home Assistant dashboard YAML document."""
    return yaml.safe_dump(build_dashboard_config(config, snapshot), sort_keys=False, allow_unicode=True)


def write_dashboard_file(config, snapshot, state):
    """Write the generated dashboard YAML file if it changed."""
    if not config.generate_dashboard:
        return
    dashboard_yaml = build_dashboard_yaml(config, snapshot)
    signature = hashlib.sha256(dashboard_yaml.encode("utf-8")).hexdigest()
    if signature == state.last_dashboard_signature:
        return
    state.last_dashboard_signature = signature
    os.makedirs(os.path.dirname(config.dashboard_output_path), exist_ok=True)
    with open(config.dashboard_output_path, "w", encoding="utf-8") as file_handle:
        file_handle.write(dashboard_yaml)


def home_assistant_headers(config) -> dict[str, str]:
    """Return Home Assistant API headers."""
    return {
        "Authorization": f"Bearer {config.home_assistant_token}",
        "Content-Type": "application/json",
    }


def call_home_assistant_service(config, service_name: str, payload: dict):
    """Call a Home Assistant service by its service name."""
    domain, service = service_name.split(".", 1)
    url = f"{config.home_assistant_url.rstrip('/')}/api/services/{domain}/{service}"
    return requests.post(url, headers=home_assistant_headers(config), json=payload, timeout=20)


def notify_issue_transitions(config, logger, issues):
    """Send Home Assistant notifications for issue transitions."""
    if not config.home_assistant_url or not config.home_assistant_token:
        return

    for issue in issues:
        if severity_value(issue.severity) < severity_value(config.notification_minimum_severity):
            continue

        notification_id = f"{config.self_name}_monitor_{issue.slug}"
        title = f"{config.self_name.title()} monitor: {issue.title}"
        if issue.newly_active and config.notify_on_new_issue:
            if config.home_assistant_persistent_notifications:
                call_home_assistant_service(
                    config,
                    "persistent_notification.create",
                    {
                        "title": title,
                        "message": issue.description,
                        "notification_id": notification_id,
                    },
                )
            if config.home_assistant_notify_service:
                call_home_assistant_service(
                    config,
                    config.home_assistant_notify_service,
                    {
                        "title": title,
                        "message": issue.description,
                    },
                )
        if issue.newly_resolved and config.notify_on_resolve:
            if config.home_assistant_persistent_notifications:
                call_home_assistant_service(
                    config,
                    "persistent_notification.dismiss",
                    {"notification_id": notification_id},
                )
            if config.home_assistant_notify_service:
                call_home_assistant_service(
                    config,
                    config.home_assistant_notify_service,
                    {
                        "title": f"{config.self_name.title()} monitor: resolved",
                        "message": f"{issue.title} resolved",
                    },
                )
