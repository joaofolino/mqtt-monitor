"""Core models for the mqtt-monitor v2 runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
import re


@dataclass
class Config:
    """Runtime configuration for mqtt-monitor."""

    enable_email: bool
    log_level: str
    enable_io_mqtt: bool
    alert_email: str
    smtp_server: str
    smtp_port: int
    smtp_user: str
    smtp_pass: str
    package_version: str
    broker: str
    port: int
    mqtt_user: str
    mqtt_pass: str
    self_name: str
    smart_interval: int
    topic_prefix: str
    log_dir: str
    log_file: str
    devices: list[str]
    raid_arrays: list[str]
    mdstat_path: str
    cpu_temp_path: str
    disk_usage_mounts: list[str]
    max_io_rate: float
    md1_usage_threshold: float
    md1_write_spike_threshold: float
    sleep_interval: float
    error_sleep: float
    state_dir: str = "/var/lib/mqtt-monitor"
    auto_discover_disks: bool = True
    auto_discover_raid_arrays: bool = True
    auto_discover_mounts: bool = True
    auto_discover_lxc: bool = True
    ignored_devices: list[str] = field(default_factory=lambda: ["loop", "ram", "zram", "fd", "sr", "dm-"])
    ignored_mounts: list[str] = field(default_factory=lambda: ["/boot/efi", "/snap"])
    expected_lxc_containers: list[str] = field(default_factory=list)
    generate_dashboard: bool = True
    dashboard_output_path: str = "/var/lib/mqtt-monitor/home-assistant-dashboard.yaml"
    home_assistant_url: str = ""
    home_assistant_token: str = ""
    home_assistant_notify_service: str = ""
    home_assistant_persistent_notifications: bool = True
    notify_on_new_issue: bool = True
    notify_on_resolve: bool = True
    notification_minimum_severity: str = "warning"


MonitorConfig = Config


@dataclass
class HostMetrics:
    """Host-level metrics."""

    cpu_load_percentage: float
    memory_used_megabytes: int
    cpu_temperature_celsius: float | None


@dataclass
class FilesystemMetric:
    """Filesystem usage metrics."""

    mount_point: str
    usage_percentage: int
    free_space: str

    @property
    def slug(self) -> str:
        """Return the normalized mount name used in MQTT/HA entities."""
        return self.mount_point.strip("/").replace("/", "_") or "root"


@dataclass
class DiskMetric:
    """Metrics and health state for a block device."""

    path: str
    name: str
    capacity_gib: int
    capacity_bytes: str | int | None
    availability: str
    smart_health_status: str | None
    max_temperature: int | None
    power_on_hours: int | None
    reallocated_sectors: int | None
    spin_up_count: int | None
    read_kilobytes_per_second: float
    write_kilobytes_per_second: float
    read_count_per_second: float
    write_count_per_second: float
    busy_time_percentage: float
    command_line_parse_error: bool
    device_open_failed: bool
    smart_command_failed: bool
    disk_failing: bool
    prefail_attributes_threshold: bool
    past_attributes_threshold: bool
    device_error_log_errors: bool
    self_test_log_errors: bool

    @property
    def slug(self) -> str:
        """Return a Home Assistant-safe disk identifier."""
        slug = re.sub(r"[^a-z0-9_]+", "_", self.name.lower())
        slug = re.sub(r"_+", "_", slug).strip("_")
        return slug or "disk"


@dataclass
class RaidMetric:
    """Status for a Linux software RAID array."""

    name: str
    status: str
    raw: str
    failed_devices: list[str] = field(default_factory=list)


@dataclass
class ContainerMetric:
    """State for an LXC container."""

    name: str
    status: str
    autostart: bool = False

    @property
    def running(self) -> bool:
        """Return whether the container is running."""
        return self.status.upper() == "RUNNING"


@dataclass
class Issue:
    """First-class monitor issue used for HA entities and notifications."""

    key: str
    title: str
    description: str
    severity: str
    resource_type: str
    resource_name: str
    active: bool
    started_at: str | None = None
    resolved_at: str | None = None
    newly_active: bool = False
    newly_resolved: bool = False

    @property
    def slug(self) -> str:
        """Return a stable MQTT/HA-safe slug for the issue."""
        return self.key.replace("/", "_").replace(".", "_").replace("-", "_")


@dataclass
class MonitorSnapshot:
    """Complete monitor snapshot for one loop."""

    generated_at: str
    host: HostMetrics
    filesystems: list[FilesystemMetric]
    disks: list[DiskMetric]
    raids: list[RaidMetric]
    containers: list[ContainerMetric]


@dataclass
class MonitorState:
    """Mutable runtime state kept between loops."""

    previous_io_stats: dict[str, dict] = field(default_factory=dict)
    previous_loop_time: float | None = None
    last_smart_check_time: float = 0.0
    persisted_issue_state: dict[str, dict] = field(default_factory=dict)
    last_dashboard_signature: str = ""
    discovered_expected_lxc_containers: list[str] = field(default_factory=list)
