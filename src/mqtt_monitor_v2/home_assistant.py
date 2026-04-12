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


def monitor_display_name(config) -> str:
    """Return a human-friendly monitor/device name without duplicate suffixes."""
    if config.home_assistant_display_name:
        name = config.home_assistant_display_name.strip()
    else:
        name = inferred_monitor_display_name(config)
    name = re.sub(r"[_-]+", " ", name).strip()
    return re.sub(r"\s+", " ", name).title() or "Monitor"


def inferred_monitor_display_name(config) -> str:
    """Infer a host-like display name when the self name is generic."""
    generic_names = {"mqtt-monitor", "mqtt_monitor", "monitor"}
    if config.self_name.lower() in generic_names:
        parts = [part for part in config.topic_prefix.split("/") if part]
        if len(parts) >= 3 and parts[0] == "home":
            return parts[1]
    return config.self_name


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


def tile_card(
    entity: str,
    name: str,
    icon: str,
    *,
    color: str | None = None,
    hide_state: bool | None = None,
    state_content: str | list[str] | None = None,
) -> dict:
    """Return a compact tile card definition."""
    card = {
        "type": "tile",
        "entity": entity,
        "name": name,
        "icon": icon,
    }
    if color is not None:
        card["color"] = color
    if hide_state is not None:
        card["hide_state"] = hide_state
    if state_content is not None:
        card["state_content"] = state_content
    return card


def conditional_card(conditions: list[dict], card: dict) -> dict:
    """Return a conditional Lovelace card."""
    return {
        "type": "conditional",
        "conditions": conditions,
        "card": card,
    }


def entities_card(title: str, entities: list[str] | list[dict], *, state_color: bool = False) -> dict:
    """Return a Home Assistant entities card."""
    card = {
        "type": "entities",
        "title": title,
        "show_header_toggle": False,
        "entities": entities,
    }
    if state_color:
        card["state_color"] = True
    return card


def chunked(values: list, size: int) -> list[list]:
    """Return values split into fixed-size chunks."""
    return [values[index:index + size] for index in range(0, len(values), size)]


def named_entity(entity: str, name: str) -> dict:
    """Return an entity row with a shorter dashboard label."""
    return {
        "entity": entity,
        "name": name,
    }


def is_logical_disk(disk) -> bool:
    """Return whether a disk represents a logical array or mapper-backed volume."""
    return disk.path.startswith("/dev/md") or disk.path.startswith("/dev/mapper/")


def disk_entities(disk, include_io: bool) -> list[dict]:
    """Return the relevant entity ids for a disk card."""
    entities = [named_entity(f"sensor.disk_{disk.slug}_capacity_gib", "Capacity (GiB)")]
    if disk.smart_health_status is not None:
        entities.append(named_entity(f"sensor.disk_{disk.slug}_smart_health_status", "SMART Health"))
    if disk.max_temperature is not None:
        entities.append(named_entity(f"sensor.disk_{disk.slug}_max_temperature", "Max Temp"))
    if disk.power_on_hours is not None:
        entities.append(named_entity(f"sensor.disk_{disk.slug}_power_on_hours", "Power-On Hours"))
    if include_io:
        entities.extend(
            [
                named_entity(f"sensor.disk_{disk.slug}_read_kilobytes_per_second", "I/O Read"),
                named_entity(f"sensor.disk_{disk.slug}_write_kilobytes_per_second", "I/O Write"),
                named_entity(f"sensor.disk_{disk.slug}_busy_time_percentage", "I/O Busy"),
            ]
        )
    return entities


def mount_is_maintenance(mount_point: str) -> bool:
    """Return whether a mount point is low-priority maintenance storage."""
    return mount_point == "/boot" or mount_point.startswith("/boot/")


def sorted_filesystems(filesystems) -> list:
    """Return filesystems in operational priority order."""
    return sorted(
        filesystems,
        key=lambda filesystem: (
            mount_is_maintenance(filesystem.mount_point),
            filesystem.mount_point != "/",
            -filesystem.usage_percentage,
            filesystem.mount_point,
        ),
    )


def split_filesystems(filesystems) -> tuple[list, list]:
    """Split filesystems into primary and maintenance groups."""
    primary = []
    maintenance = []
    for filesystem in sorted_filesystems(filesystems):
        if mount_is_maintenance(filesystem.mount_point):
            maintenance.append(filesystem)
        else:
            primary.append(filesystem)
    return primary, maintenance


def hottest_disk(snapshot):
    """Return the hottest physical disk with a temperature reading."""
    candidates = [disk for disk in snapshot.disks if not is_logical_disk(disk) and disk.max_temperature is not None]
    if not candidates:
        return None
    return max(candidates, key=lambda disk: (disk.max_temperature, disk.name))


def expected_container_names(config, snapshot) -> set[str]:
    """Return containers that should normally be treated as expected services."""
    configured = set(config.expected_lxc_containers)
    autostarted = {container.name for container in snapshot.containers if container.autostart}
    return configured | autostarted


def container_sort_key(container, expected_names: set[str]) -> tuple:
    """Return an operational sort key for containers."""
    expected = container.name in expected_names
    return (
        0 if expected and not container.running else 1,
        0 if expected else 1,
        0 if container.running else 1,
        container.name,
    )


def container_tile_card(prefix: str, container) -> dict:
    """Return a color-aware expected-service tile stack."""
    running_entity = f"binary_sensor.{prefix}_container_{container.slug}_running"
    status_entity = f"sensor.{prefix}_container_{container.slug}_status"
    return {
        "type": "vertical-stack",
        "cards": [
            conditional_card(
                [{"entity": running_entity, "state": "on"}],
                tile_card(
                    running_entity,
                    container.name,
                    "mdi:package-variant-closed-check",
                    color="green",
                    state_content="state",
                ),
            ),
            conditional_card(
                [{"entity": running_entity, "state": "off"}],
                tile_card(
                    status_entity,
                    container.name,
                    "mdi:package-variant-closed-remove",
                    color="amber",
                    state_content="state",
                ),
            ),
        ],
    }


def disk_heat_card(disk) -> dict:
    """Return a disk temperature gauge with severity-aware thresholds."""
    return gauge_card(
        f"sensor.disk_{disk.slug}_max_temperature",
        disk.name,
        "mdi:thermometer",
        unit="C",
        maximum=100,
        yellow=60,
        red=70,
    )


def disk_activity_score(disk) -> float:
    """Return a sortable combined throughput score for a disk."""
    return disk.read_kilobytes_per_second + disk.write_kilobytes_per_second


def disk_io_card(disk) -> dict:
    """Return a compact disk I/O summary card."""
    return entities_card(
        f"Disk {disk.name}",
        [
            named_entity(f"sensor.disk_{disk.slug}_read_kilobytes_per_second", "I/O Read (KB/s)"),
            named_entity(f"sensor.disk_{disk.slug}_write_kilobytes_per_second", "I/O Write (KB/s)"),
            named_entity(f"sensor.disk_{disk.slug}_busy_time_percentage", "I/O Busy (%)"),
        ],
    )


def disk_io_markdown_card(disks) -> dict:
    """Return a dense markdown table for disk I/O overview."""
    lines = [
        "| Disk | Read KB/s | Write KB/s | Busy % |",
        "| --- | ---: | ---: | ---: |",
    ]
    for disk in disks:
        lines.append(
            "| {name} | {{{{ states('sensor.disk_{slug}_read_kilobytes_per_second') }}}} | "
            "{{{{ states('sensor.disk_{slug}_write_kilobytes_per_second') }}}} | "
            "{{{{ states('sensor.disk_{slug}_busy_time_percentage') }}}} |".format(
                name=disk.name,
                slug=disk.slug,
            )
        )
    return {
        "type": "markdown",
        "content": "\n".join(lines),
    }


def metric_label(metric_name: str) -> str:
    """Return a concise Home Assistant label for a metric name."""
    labels = {
        "cpu_load_percentage": "CPU Load",
        "memory_used_megabytes": "Memory Used",
        "cpu_temperature_celsius": "CPU Temp",
        "max_temperature": "Max Temp",
        "power_on_hours": "Power-On Hours",
        "reallocated_sectors": "Reallocated",
        "spin_up_count": "Spin Ups",
        "smart_health_status": "SMART Health",
        "capacity_gib": "Capacity",
        "read_kilobytes_per_second": "I/O Read",
        "write_kilobytes_per_second": "I/O Write",
        "read_count_per_second": "I/O Read Ops",
        "write_count_per_second": "I/O Write Ops",
        "busy_time_percentage": "I/O Busy",
        "command_line_parse_error": "Parse Error",
        "device_open_failed": "Open Failed",
        "smart_command_failed": "SMART Command Failed",
        "disk_failing": "Disk Failing",
        "prefail_attributes_threshold": "Prefail Threshold",
        "past_attributes_threshold": "Past Threshold",
        "device_error_log_errors": "Device Errors",
        "self_test_log_errors": "Self Test Errors",
    }
    return labels.get(metric_name, metric_name.replace("_", " ").title())


def sensor_precision(metric_name: str) -> int | None:
    """Return the desired Home Assistant display precision for a numeric metric."""
    zero_decimal_metrics = {
        "memory_used_megabytes",
        "max_temperature",
        "power_on_hours",
        "reallocated_sectors",
        "spin_up_count",
        "capacity_gib",
    }
    one_decimal_metrics = {
        "cpu_load_percentage",
        "cpu_temperature_celsius",
        "read_kilobytes_per_second",
        "write_kilobytes_per_second",
        "read_count_per_second",
        "write_count_per_second",
        "busy_time_percentage",
    }
    if metric_name in zero_decimal_metrics:
        return 0
    if metric_name in one_decimal_metrics:
        return 1
    return None


def build_dashboard_view(config, snapshot) -> dict:
    """Build the main Home Assistant dashboard view."""
    prefix = entity_prefix(config)
    root_filesystem = next((filesystem for filesystem in snapshot.filesystems if filesystem.mount_point == "/"), None)
    hottest = hottest_disk(snapshot)
    primary_filesystems, maintenance_filesystems = split_filesystems(snapshot.filesystems)
    expected_names = expected_container_names(config, snapshot)
    sorted_containers = sorted(snapshot.containers, key=lambda container: container_sort_key(container, expected_names))
    expected_containers = [container for container in sorted_containers if container.name in expected_names]
    other_containers = [container for container in sorted_containers if container.name not in expected_names]
    temperature_disks = sorted(
        [disk for disk in snapshot.disks if not is_logical_disk(disk) and disk.max_temperature is not None],
        key=lambda disk: (-disk.max_temperature, disk.name),
    )
    activity_disks = sorted(
        snapshot.disks,
        key=lambda disk: (-disk_activity_score(disk), -disk.busy_time_percentage, disk.name),
    )

    overview_cards = [
        {"type": "heading", "heading": "Health", "icon": "mdi:server-network"},
        {
            "type": "grid",
            "columns": 3,
            "square": False,
            "cards": (
                [
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
                ]
                + (
                    [
                        gauge_card(
                            f"sensor.{prefix}_cpu_temperature_celsius",
                            "CPU Temp",
                            "mdi:thermometer",
                            unit="C",
                            maximum=100,
                            yellow=80,
                            red=90,
                        )
                    ]
                    if snapshot.host.cpu_temperature_celsius is not None
                    else []
                )
                + (
                    [
                        gauge_card(
                            f"sensor.{prefix}_{root_filesystem.slug}_disk_usage_percentage",
                            "Root Usage",
                            "mdi:harddisk",
                            maximum=100,
                            yellow=75,
                            red=90,
                        )
                    ]
                    if root_filesystem is not None
                    else []
                )
                + (
                    [
                        gauge_card(
                            f"sensor.disk_{hottest.slug}_max_temperature",
                            f"Hottest Disk ({hottest.name})",
                            "mdi:thermometer-chevron-up",
                            unit="C",
                            maximum=100,
                            yellow=60,
                            red=70,
                        )
                    ]
                    if hottest is not None
                    else []
                )
                + [tile_card(f"sensor.{prefix}_memory_used_megabytes", "Memory Used", "mdi:memory")]
            ),
        },
        {
            "type": "markdown",
            "title": "Attention",
            "content": "\n".join([
                f"{{% set generated = state_attr('sensor.{prefix}_active_issue_count', 'generated_at') %}}",
                f"{{% set issues = state_attr('sensor.{prefix}_active_issue_count', 'active_issues') or [] %}}",
                "{% set critical = issues | selectattr('severity', 'equalto', 'critical') | list | count %}",
                "{% set warning = issues | selectattr('severity', 'equalto', 'warning') | list | count %}",
                "{% if issues | count == 0 %}",
                "All monitored resources are reporting normally.",
                "{% else %}",
                "**Critical:** {{ critical }}  |  **Warning:** {{ warning }}",
                "",
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

    storage_cards = [{"type": "heading", "heading": "Core Storage", "icon": "mdi:harddisk"}]
    if primary_filesystems:
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
                    for filesystem in primary_filesystems
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
                        raid.name,
                        "mdi:harddisk-plus",
                    )
                    for raid in snapshot.raids
                ],
            }
        )
    if maintenance_filesystems:
        storage_cards.append(
            entities_card(
                "Maintenance Mounts",
                [
                    named_entity(
                        f"sensor.{prefix}_{filesystem.slug}_disk_usage_percentage",
                        compact_path_label(filesystem.mount_point),
                    )
                    for filesystem in maintenance_filesystems
                ],
                state_color=True,
            )
        )

    expected_service_cards = [{"type": "heading", "heading": "Expected Services", "icon": "mdi:package-variant-closed"}]
    if expected_containers:
        expected_service_cards.append(
            {
                "type": "grid",
                "columns": 3,
                "square": False,
                "cards": [container_tile_card(prefix, container) for container in expected_containers],
            }
        )
    else:
        expected_service_cards.append({"type": "markdown", "content": "No expected services are currently marked for LXC monitoring."})

    container_cards = [{"type": "heading", "heading": "Other Containers", "icon": "mdi:package-variant"}]
    if other_containers:
        container_cards.append(
            entities_card(
                "Legacy Or Optional Containers",
                [
                    named_entity(
                        f"binary_sensor.{prefix}_container_{container.slug}_running",
                        container.name,
                    )
                    for container in other_containers
                ],
                state_color=True,
            )
        )
    else:
        container_cards.append({"type": "markdown", "content": "No additional containers discovered."})

    disk_heat_cards = [{"type": "heading", "heading": "Disk Heat", "icon": "mdi:thermometer"}]
    if temperature_disks:
        disk_heat_cards.append(
            {
                "type": "grid",
                "columns": 3,
                "square": False,
                "cards": [disk_heat_card(disk) for disk in temperature_disks],
            }
        )

    disk_activity_cards = [{"type": "heading", "heading": "Disk I/O", "icon": "mdi:pulse"}]
    if activity_disks:
        disk_activity_cards.append(disk_io_markdown_card(activity_disks))
        disk_activity_cards.extend([disk_io_card(disk) for disk in activity_disks[:3]])

    sections = [
        {"cards": overview_cards},
        {"cards": storage_cards},
        {"cards": expected_service_cards},
        {"cards": container_cards},
        {"cards": disk_heat_cards},
        {"cards": disk_activity_cards},
    ]

    logical_disks = [disk for disk in snapshot.disks if is_logical_disk(disk)]
    physical_disks = [disk for disk in snapshot.disks if not is_logical_disk(disk)]
    for index, group in enumerate(chunked(physical_disks, 2)):
        sections.append(
            {
                "cards": [
                    {
                        "type": "heading",
                        "heading": "Physical Disk Detail" if index == 0 else "Physical Disk Detail (cont.)",
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
                        "heading": "Logical Volume Detail" if index == 0 else "Logical Volume Detail (cont.)",
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
        "name": monitor_display_name(config),
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
        ("cpu_load_percentage", "%", "CPU Load"),
        ("memory_used_megabytes", "MB", "Memory Used"),
        ("cpu_temperature_celsius", "C", "CPU Temp"),
    ]
    for suffix, unit, title in host_sensors:
        object_id = f"{prefix}_{suffix}"
        payload = {
            "name": title,
            "object_id": object_id,
            "unique_id": f"{prefix}_monitor_{suffix}",
            "device": device,
            "state_topic": f"{config.topic_prefix}/host/{suffix}",
            "unit_of_measurement": unit,
        }
        precision = sensor_precision(suffix)
        if precision is not None:
            payload["suggested_display_precision"] = precision
        publish_discovery(client, "sensor", object_id, payload)

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
                "name": f"{compact_path_label(getattr(mount, 'mount_point', mount.slug))} Usage",
                "object_id": object_id,
                "unique_id": f"{prefix}_monitor_{mount.slug}_disk_usage_percentage",
                "device": device,
                "state_topic": f"{config.topic_prefix}/host/{mount.slug}_disk_usage_percentage",
                "unit_of_measurement": "%",
                "json_attributes_topic": f"{config.topic_prefix}/host/{mount.slug}_disk_usage_attributes",
                "suggested_display_precision": 0,
            },
        )

    publish_discovery(
        client,
        "sensor",
        f"{prefix}_active_issue_count",
        {
            "name": "Active Issues",
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
            "name": "Highest Severity",
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
                "name": f"{disk_name} {metric_label(metric_name)}",
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
            precision = sensor_precision(metric_name)
            if precision is not None:
                payload["suggested_display_precision"] = precision
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
                    "name": f"{disk_name} {metric_label(flag)}",
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
                "name": raid.name,
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
                    "name": f"{container.name} Running",
                    "object_id": f"{prefix}_container_{container.slug}_running",
                    "unique_id": f"{prefix}_monitor_container_{container.slug}_running",
                    "device": device,
                    "device_class": "running",
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
                "name": container.name,
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
        title = f"{monitor_display_name(config)}: {issue.title}"
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
