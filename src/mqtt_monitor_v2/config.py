"""Configuration loading for mqtt-monitor v2."""

from __future__ import annotations

import configparser

from .models import Config


def split_csv(value: str) -> list[str]:
    """Split a comma-separated config value, dropping blanks."""
    return [item.strip() for item in value.split(",") if item.strip()]


def get_option(config: configparser.ConfigParser, section: str, option: str, fallback: str) -> str:
    """Return a string option with a fallback."""
    return config.get(section, option, fallback=fallback)


def get_bool(config: configparser.ConfigParser, section: str, option: str, fallback: bool) -> bool:
    """Return a boolean option with a fallback."""
    return config.getboolean(section, option, fallback=fallback)


def get_int(config: configparser.ConfigParser, section: str, option: str, fallback: int) -> int:
    """Return an integer option with a fallback."""
    return config.getint(section, option, fallback=fallback)


def get_float(config: configparser.ConfigParser, section: str, option: str, fallback: float) -> float:
    """Return a float option with a fallback."""
    return config.getfloat(section, option, fallback=fallback)


def load_config(config_path: str = "/etc/mqtt-monitor") -> Config:
    """Load runtime configuration from `mqtt-monitor.conf`."""
    parser = configparser.ConfigParser()
    config_file_path = f"{config_path}/mqtt-monitor.conf"
    print(f"Loading config from: {config_file_path}")
    if not parser.read(config_file_path):
        raise FileNotFoundError(f"Config file {config_file_path} not found or unreadable")

    return Config(
        enable_email=get_bool(parser, "monitor", "enable_email", False),
        log_level=get_option(parser, "monitor", "log_level", "INFO"),
        enable_io_mqtt=get_bool(parser, "monitor", "enable_io_mqtt", True),
        alert_email=get_option(parser, "monitor", "alert_email", "user@example.com"),
        smtp_server=get_option(parser, "monitor", "smtp_server", "smtp.gmail.com"),
        smtp_port=get_int(parser, "monitor", "smtp_port", 587),
        smtp_user=get_option(parser, "monitor", "smtp_user", "user@example.com"),
        smtp_pass=get_option(parser, "monitor", "smtp_pass", "your-app-password"),
        package_version=get_option(parser, "monitor", "package_version", "latest"),
        broker=get_option(parser, "mqtt", "broker", "localhost"),
        port=get_int(parser, "mqtt", "port", 1883),
        mqtt_user=get_option(parser, "mqtt", "mqtt_user", "mqtt-user"),
        mqtt_pass=get_option(parser, "mqtt", "mqtt_pass", "mqtt-password"),
        self_name=get_option(parser, "mqtt", "self_name", "mqtt-monitor"),
        smart_interval=get_int(parser, "internals", "smart_interval", 900),
        topic_prefix=get_option(parser, "internals", "topic_prefix", "home/server/mqtt-monitor"),
        log_dir=get_option(parser, "internals", "log_dir", "/var/log"),
        log_file=get_option(parser, "internals", "log_file", "/var/log/mqtt-monitor.log"),
        devices=split_csv(get_option(parser, "internals", "devices", "")),
        raid_arrays=split_csv(get_option(parser, "internals", "raid_arrays", "")),
        mdstat_path=get_option(parser, "internals", "mdstat_path", "/proc/mdstat"),
        cpu_temp_path=get_option(parser, "internals", "cpu_temp_path", "/sys/class/thermal/thermal_zone0/temp"),
        disk_usage_mounts=split_csv(get_option(parser, "internals", "disk_usage_mounts", "/,/var/log")),
        max_io_rate=get_float(parser, "internals", "max_io_rate", 10000.0),
        md1_usage_threshold=get_float(parser, "internals", "md1_usage_threshold", 95.0),
        md1_write_spike_threshold=get_float(parser, "internals", "md1_write_spike_threshold", 400.0),
        sleep_interval=get_float(parser, "internals", "sleep_interval", 60.0),
        error_sleep=get_float(parser, "internals", "error_sleep", 10.0),
        state_dir=get_option(parser, "internals", "state_dir", "/var/lib/mqtt-monitor"),
        auto_discover_disks=get_bool(parser, "discovery", "auto_discover_disks", True),
        auto_discover_raid_arrays=get_bool(parser, "discovery", "auto_discover_raid_arrays", True),
        auto_discover_mounts=get_bool(parser, "discovery", "auto_discover_mounts", True),
        auto_discover_lxc=get_bool(parser, "discovery", "auto_discover_lxc", True),
        ignored_devices=split_csv(get_option(parser, "discovery", "ignored_devices", "loop,ram,zram,fd,sr,dm-")),
        ignored_mounts=split_csv(get_option(parser, "discovery", "ignored_mounts", "/boot/efi,/snap")),
        expected_lxc_containers=split_csv(get_option(parser, "discovery", "expected_lxc_containers", "")),
        generate_dashboard=get_bool(parser, "home_assistant", "generate_dashboard", True),
        dashboard_output_path=get_option(
            parser,
            "home_assistant",
            "dashboard_output_path",
            "/var/lib/mqtt-monitor/home-assistant-dashboard.yaml",
        ),
        home_assistant_display_name=get_option(parser, "home_assistant", "display_name", ""),
        home_assistant_url=get_option(parser, "home_assistant", "url", ""),
        home_assistant_token=get_option(parser, "home_assistant", "token", ""),
        home_assistant_notify_service=get_option(parser, "home_assistant", "notify_service", ""),
        home_assistant_persistent_notifications=get_bool(
            parser,
            "home_assistant",
            "persistent_notifications",
            True,
        ),
        notify_on_new_issue=get_bool(parser, "home_assistant", "notify_on_new_issue", True),
        notify_on_resolve=get_bool(parser, "home_assistant", "notify_on_resolve", True),
        notification_minimum_severity=get_option(
            parser,
            "home_assistant",
            "notification_minimum_severity",
            "warning",
        ).lower(),
    )
