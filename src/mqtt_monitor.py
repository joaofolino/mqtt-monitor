#!/usr/bin/env python3
"""
MQTT Monitor: Monitors system metrics and publishes to MQTT.

This script monitors disk health, RAID status, disk I/O, CPU temperature, and disk
usage, publishing metrics to an MQTT broker and sending alerts via email for critical
events like disk failures or high usage.
"""

import configparser
import json
import logging
import os
import smtplib
import subprocess
import time
from dataclasses import dataclass
from email.mime.text import MIMEText

import paho.mqtt.client as mqtt
import psutil
import requests


@dataclass
class Config:
    """Configuration settings for MQTT Monitor."""

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
    devices: list
    raid_arrays: list
    mdstat_path: str
    cpu_temp_path: str
    disk_usage_mounts: list
    max_io_rate: float
    md1_usage_threshold: float
    md1_write_spike_threshold: float
    sleep_interval: float
    error_sleep: float


MonitorConfig = Config


def load_config(config_path="/etc/mqtt-monitor"):
    """Load configuration from a file."""
    config = configparser.ConfigParser()
    config_file_path = f"{config_path}/mqtt-monitor.conf"
    print(f"Loading config from: {config_file_path}")
    if not config.read(config_file_path):
        raise FileNotFoundError(f"Config file {config_file_path} not found or unreadable")
    try:
        return Config(
            enable_email=config.getboolean("monitor", "enable_email"),
            log_level=config["monitor"]["log_level"],
            enable_io_mqtt=config.getboolean("monitor", "enable_io_mqtt"),
            alert_email=config["monitor"]["alert_email"],
            smtp_server=config["monitor"]["smtp_server"],
            smtp_port=config.getint("monitor", "smtp_port"),
            smtp_user=config["monitor"]["smtp_user"],
            smtp_pass=config["monitor"]["smtp_pass"],
            package_version=config["monitor"]["package_version"],
            broker=config["mqtt"]["broker"],
            port=config.getint("mqtt", "port"),
            mqtt_user=config["mqtt"]["mqtt_user"],
            mqtt_pass=config["mqtt"]["mqtt_pass"],
            self_name=config["mqtt"]["self_name"],
            smart_interval=config.getint("internals", "smart_interval"),
            topic_prefix=config["internals"]["topic_prefix"],
            log_dir=config["internals"]["log_dir"],
            log_file=config["internals"]["log_file"],
            devices=config["internals"]["devices"].split(","),
            raid_arrays=config["internals"]["raid_arrays"].split(","),
            mdstat_path=config["internals"]["mdstat_path"],
            cpu_temp_path=config["internals"]["cpu_temp_path"],
            disk_usage_mounts=config["internals"]["disk_usage_mounts"].split(","),
            max_io_rate=config.getfloat("internals", "max_io_rate"),
            md1_usage_threshold=config.getfloat("internals", "md1_usage_threshold"),
            md1_write_spike_threshold=config.getfloat("internals", "md1_write_spike_threshold"),
            sleep_interval=config.getfloat("internals", "sleep_interval"),
            error_sleep=config.getfloat("internals", "error_sleep"),
        )
    except (configparser.NoSectionError, configparser.NoOptionError) as error:
        print(f"Config error: {error}")
        raise


disk_cache = {}


class MonitorState:
    """Holds state for the monitoring loop."""

    def __init__(self):
        self.previous_io_stats = {}
        self.previous_loop_time = None


def setup_logging(config):
    """Set up logging to file and console."""
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    logger.handlers = []
    log_file_path = os.path.join(os.path.abspath(config.log_dir), config.log_file)
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    stream_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(stream_handler)
    return logger


def log(config, logger, message, level="INFO"):
    """Log a message at the specified level."""
    try:
        log_level = getattr(logging, level.upper(), logging.INFO)
        if logger.isEnabledFor(log_level):
            getattr(logger, level.lower())(message)
        usage = psutil.disk_usage(os.path.abspath(config.log_dir))
        if usage.percent > 95:
            send_alert(
                config,
                logger,
                "Log Directory Full",
                f"Log directory {config.log_dir} is at {usage.percent}% capacity",
            )
    except RuntimeError as error:
        print(f"Logging error: {error}")


def on_connect(config, logger, client, userdata, flags, rc):  # pylint: disable=unused-argument
    """Handle the MQTT connection event."""
    if rc == 0:
        log(config, logger, "Connected to MQTT broker", "INFO")
    else:
        log(config, logger, f"Failed to connect to MQTT broker, code: {rc}", "ERROR")


def connect_mqtt(config, logger, client):
    """Connect to the MQTT broker."""
    try:
        client.username_pw_set(config.mqtt_user, config.mqtt_pass)
        client.on_connect = lambda c, u, f, r: on_connect(config, logger, c, u, f, r)
        client.connect(config.broker, config.port, 60)
        client.loop_start()
    except ConnectionError as error:
        log(config, logger, f"Failed to connect to MQTT broker: {error}", "ERROR")


def send_alert(config, logger, subject, body):
    """Send an email alert."""
    if not config.enable_email:
        log(config, logger, f"Email alerts disabled: {subject}", "INFO")
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = config.smtp_user
        msg["To"] = config.alert_email
        with smtplib.SMTP(config.smtp_server, config.smtp_port) as server:
            server.starttls()
            server.login(config.smtp_user, config.smtp_pass)
            server.sendmail(config.smtp_user, config.alert_email, msg.as_string())
        log(config, logger, f"Sent alert: {subject}", "INFO")
    except smtplib.SMTPException as error:
        log(config, logger, f"Failed to send alert {subject}: {error}", "ERROR")


def run_cmd(config, logger, cmd):
    """Run a shell command and return stdout plus the exit code."""
    try:
        log(config, logger, f"Executing command: {' '.join(cmd)}", "DEBUG")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        log(
            config,
            logger,
            f"Command {' '.join(cmd)} output: {result.stdout[:100]}... (exit_code: {result.returncode})",
            "DEBUG",
        )
        return result.stdout.strip(), result.returncode
    except (subprocess.SubprocessError, subprocess.TimeoutExpired) as error:
        stderr = error.stderr if hasattr(error, "stderr") else "N/A"
        log(config, logger, f"Command {' '.join(cmd)} error: {error} (stderr: {stderr})", "ERROR")
        return "", -1


def parse_smartctl_exit_code(exit_code):
    """Decode the smartctl bitmask exit code."""
    return {
        "command_line_parse_error": bool(exit_code & (1 << 0)),
        "device_open_failed": bool(exit_code & (1 << 1)),
        "smart_command_failed": bool(exit_code & (1 << 2)),
        "disk_failing": bool(exit_code & (1 << 3)),
        "prefail_attributes_threshold": bool(exit_code & (1 << 4)),
        "past_attributes_threshold": bool(exit_code & (1 << 5)),
        "device_error_log_errors": bool(exit_code & (1 << 6)),
        "self_test_log_errors": bool(exit_code & (1 << 7)),
    }


def parse_mdstat_devices(raid_status_lines):
    """Parse mdstat member tokens without treating [UU]/[U_] as devices."""
    devices = []
    for line in raid_status_lines:
        if "blocks" not in line:
            continue
        for part in line.split():
            if "[" not in part or "]" not in part or "/" in part or part.startswith("["):
                continue
            device, state = part.split("[", 1)
            if not device:
                continue
            devices.append({
                "device": device,
                "state": "active" if state.rstrip("]").isdigit() else "failed",
            })
    return devices


def get_capacity_gib(cached_capacity):
    """Normalize cached capacity values stored as bytes or GiB."""
    if cached_capacity is None:
        return 0
    if isinstance(cached_capacity, int):
        return cached_capacity
    if isinstance(cached_capacity, str) and cached_capacity.isdigit():
        return int(cached_capacity) // 1024 // 1024 // 1024
    return 0


def get_raid_status(config, logger):
    """Get RAID array status from mdstat."""
    try:
        with open(config.mdstat_path, "r", encoding="utf-8") as file_handle:
            mdstat = file_handle.read()
        arrays = {}
        current_array = None
        block_lines = []
        for raw_line in mdstat.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("unused devices"):
                continue
            if line.startswith("md"):
                if current_array and block_lines:
                    status = "active" if any("[UU]" in value for value in block_lines) else "degraded"
                    devices = parse_mdstat_devices(block_lines)
                    arrays[current_array] = {
                        "status": status,
                        "raw": " ".join(block_lines),
                        "failed_devices": [device["device"] for device in devices if device["state"] == "failed"],
                    }
                current_array = line.split()[0]
                block_lines = [line]
            elif current_array:
                block_lines.append(line)
        if current_array and block_lines:
            status = "active" if any("[UU]" in value for value in block_lines) else "degraded"
            devices = parse_mdstat_devices(block_lines)
            arrays[current_array] = {
                "status": status,
                "raw": " ".join(block_lines),
                "failed_devices": [device["device"] for device in devices if device["state"] == "failed"],
            }
        if not arrays:
            return {"md0": {"status": "unknown", "raw": "", "failed_devices": []}}
        log(config, logger, f"RAID status: {arrays}", "INFO")
        log(config, logger, f"Raw mdstat: {mdstat[:200]}...", "DEBUG")
        return arrays
    except (FileNotFoundError, IOError) as error:
        log(config, logger, f"Error reading mdstat: {error}", "ERROR")
        return {"md0": {"status": "unknown", "raw": "", "failed_devices": []}}


def empty_io_stats():
    """Return a zeroed disk I/O payload."""
    return {
        "read_count": 0,
        "write_count": 0,
        "read_bytes": 0,
        "write_bytes": 0,
        "read_time": 0,
        "write_time": 0,
        "busy_time": 0,
        "read_merged_count": 0,
        "write_merged_count": 0,
    }


def get_disk_io(config, logger, device_name):
    """Get disk I/O statistics for a device."""
    try:
        cached_info = disk_cache.get(device_name) or disk_cache.get(f"/dev/{device_name}", {})
        if cached_info.get("availability") == "offline":
            log(config, logger, f"Skipping I/O for offline disk {device_name}", "DEBUG")
            return empty_io_stats()
        io_counters = psutil.disk_io_counters(perdisk=True)
        if device_name not in io_counters:
            log(config, logger, f"No I/O data for {device_name}", "ERROR")
            return empty_io_stats()
        io_stats = io_counters[device_name]
        log(
            config,
            logger,
            (
                f"{device_name} I/O stats: read_count={io_stats.read_count}, "
                f"write_count={io_stats.write_count}, read_bytes={io_stats.read_bytes}, "
                f"write_bytes={io_stats.write_bytes}, read_time={io_stats.read_time}ms, "
                f"write_time={io_stats.write_time}ms, busy_time={io_stats.busy_time}ms"
            ),
            "DEBUG",
        )
        return {
            "read_count": io_stats.read_count,
            "write_count": io_stats.write_count,
            "read_bytes": io_stats.read_bytes,
            "write_bytes": io_stats.write_bytes,
            "read_time": io_stats.read_time,
            "write_time": io_stats.write_time,
            "busy_time": io_stats.busy_time,
            "read_merged_count": getattr(io_stats, "read_merged_count", 0),
            "write_merged_count": getattr(io_stats, "write_merged_count", 0),
        }
    except Exception as error:  # pylint: disable=broad-except
        log(config, logger, f"Error getting I/O for {device_name}: {error}", "ERROR")
        return empty_io_stats()


def parse_smart_output(config, logger, smart_output, device_path):
    """Parse SMART output into a dictionary."""
    smart_lines = smart_output.splitlines()
    smart_health_status = None
    for line in smart_lines:
        if "SMART overall-health self-assessment test result:" not in line:
            continue
        status = line.split(":")[-1].strip().upper()
        if status in {"PASSED", "FAILED", "UNKNOWN"}:
            smart_health_status = status
        else:
            smart_health_status = "UNKNOWN"
            log(config, logger, f"Unexpected SMART status for {device_path}: {status}", "WARNING")
        break

    smart_attributes = {
        "availability": "online",
        "smart_health_status": smart_health_status,
        "max_temperature": None,
        "power_on_hours": None,
        "reallocated_sectors": None,
        "spin_up_count": None,
    }
    if not smart_lines or not smart_health_status:
        log(config, logger, f"Invalid or empty SMART output for {device_path}: {smart_output[:100]}", "ERROR")
        smart_attributes["availability"] = "offline"
        smart_attributes["smart_health_status"] = None
        return smart_attributes

    temperature_readings = []
    power_on_hours = None
    reallocated_sectors = None
    spin_up_count = None
    for line in smart_lines:
        parts = line.split()
        if "Temperature_Celsius" in line and len(parts) >= 10 and parts[9].isdigit():
            temperature_readings.append(int(parts[9]))
        elif line.startswith("Temperature Sensor 2:") and len(parts) > 3 and parts[3].isdigit():
            temperature_readings.append(int(parts[3]))
        elif line.startswith("Temperature:") and len(parts) > 1 and parts[1].isdigit():
            temperature_readings.append(int(parts[1]))
        elif "Power_On_Hours" in line and len(parts) >= 10 and parts[9].isdigit():
            power_on_hours = int(parts[9])
        elif line.startswith("Power On Hours:") and len(parts) > 3 and parts[3].isdigit():
            power_on_hours = int(parts[3])
        elif "Reallocated_Sector_Ct" in line and len(parts) >= 10 and parts[9].isdigit():
            reallocated_sectors = int(parts[9])
        elif "Spin_Up_Time" in line and len(parts) >= 10 and parts[9].isdigit():
            spin_up_count = int(parts[9])

    smart_attributes.update({
        "max_temperature": max(temperature_readings) if temperature_readings else None,
        "power_on_hours": power_on_hours,
        "reallocated_sectors": reallocated_sectors,
        "spin_up_count": spin_up_count,
    })
    if smart_health_status == "FAILED":
        send_alert(
            config,
            logger,
            f"Disk {device_path} SMART Failure",
            f"SMART status: {smart_health_status}, Output: {smart_output[:200]}",
        )
    return smart_attributes


def get_disk_info(config, logger, device_path, full=False):
    """Get disk information (capacity and SMART attributes)."""
    device_name = device_path.split("/")[-1]
    cached_info = disk_cache.get(device_path, {})
    if not os.path.exists(device_path):
        log(config, logger, f"Device {device_path} does not exist", "ERROR")
        return {
            "name": device_name,
            "capacity": 0,
            "capacity_bytes": "0",
            "availability": "offline",
            "smart_health_status": None,
            "max_temperature": None,
            "power_on_hours": None,
            "reallocated_sectors": None,
            "spin_up_count": None,
            "command_line_parse_error": False,
            "device_open_failed": True,
            "smart_command_failed": False,
            "disk_failing": False,
            "prefail_attributes_threshold": False,
            "past_attributes_threshold": False,
            "device_error_log_errors": False,
            "self_test_log_errors": False,
        }

    if device_path not in disk_cache or not (cached_info.get("capacity_bytes") or cached_info.get("capacity")):
        size, _ = run_cmd(config, logger, ["blockdev", "--getsize64", device_path])
        if size and size.isdigit():
            disk_cache[device_path] = {"capacity_bytes": size}
        else:
            log(config, logger, f"Invalid blockdev output for {device_path}: {size}", "ERROR")
            disk_cache[device_path] = {"capacity_bytes": "0"}
    cached_info = disk_cache.get(device_path, {})
    raw_capacity = cached_info.get("capacity_bytes", cached_info.get("capacity"))
    capacity = get_capacity_gib(raw_capacity)

    if not full:
        return {
            "name": device_name,
            "capacity": capacity,
            "capacity_bytes": raw_capacity,
            "availability": cached_info.get("availability", "unknown"),
            "smart_health_status": cached_info.get("smart_health_status"),
            "max_temperature": cached_info.get("max_temperature"),
            "power_on_hours": cached_info.get("power_on_hours"),
            "reallocated_sectors": cached_info.get("reallocated_sectors"),
            "spin_up_count": cached_info.get("spin_up_count"),
            "command_line_parse_error": cached_info.get("command_line_parse_error", False),
            "device_open_failed": cached_info.get("device_open_failed", False),
            "smart_command_failed": cached_info.get("smart_command_failed", False),
            "disk_failing": cached_info.get("disk_failing", False),
            "prefail_attributes_threshold": cached_info.get("prefail_attributes_threshold", False),
            "past_attributes_threshold": cached_info.get("past_attributes_threshold", False),
            "device_error_log_errors": cached_info.get("device_error_log_errors", False),
            "self_test_log_errors": cached_info.get("self_test_log_errors", False),
        }

    if device_path == "/dev/md1":
        return {
            "name": device_name,
            "capacity": capacity,
            "capacity_bytes": raw_capacity,
            "availability": "online",
            "smart_health_status": None,
            "max_temperature": None,
            "power_on_hours": None,
            "reallocated_sectors": None,
            "spin_up_count": None,
            "command_line_parse_error": False,
            "device_open_failed": False,
            "smart_command_failed": False,
            "disk_failing": False,
            "prefail_attributes_threshold": False,
            "past_attributes_threshold": False,
            "device_error_log_errors": False,
            "self_test_log_errors": False,
        }

    device_types = ["nvme", "auto"] if "nvme" in device_path else ["sat", "scsi", "auto"]
    for device_type in device_types:
        smart_output, exit_code = run_cmd(config, logger, ["smartctl", "-a", "-d", device_type, device_path])
        if not smart_output:
            log(config, logger, f"No SMART output for {device_path} with -d {device_type}", "ERROR")
            continue

        disk_smart_info = parse_smart_output(config, logger, smart_output, device_path)
        disk_smart_info.update({
            "name": device_name,
            "capacity": capacity,
            "capacity_bytes": raw_capacity,
        })
        exit_flags = parse_smartctl_exit_code(exit_code)
        disk_smart_info.update(exit_flags)
        if exit_flags["command_line_parse_error"] or exit_flags["device_open_failed"]:
            disk_smart_info["availability"] = "offline"
        log(config, logger, f"{device_path} availability: {disk_smart_info['availability']}", "DEBUG")
        return disk_smart_info

    log(config, logger, f"Failed to get SMART data for {device_path} after all attempts", "ERROR")
    return {
        "name": device_name,
        "capacity": capacity,
        "capacity_bytes": raw_capacity,
        "availability": "offline",
        "smart_health_status": None,
        "max_temperature": None,
        "power_on_hours": None,
        "reallocated_sectors": None,
        "spin_up_count": None,
        "command_line_parse_error": False,
        "device_open_failed": True,
        "smart_command_failed": False,
        "disk_failing": False,
        "prefail_attributes_threshold": False,
        "past_attributes_threshold": False,
        "device_error_log_errors": False,
        "self_test_log_errors": False,
    }


def get_usage(config, logger, mount_point):
    """Get disk usage for a mount point."""
    try:
        command_output, _ = run_cmd(config, logger, ["df", "-h", mount_point])
        if command_output:
            for line in command_output.splitlines()[1:]:
                parts = line.split()
                if len(parts) >= 6 and parts[-1] == mount_point:
                    percent = int(parts[4].rstrip("%"))
                    free_space = parts[3]
                    log(config, logger, f"df output for {mount_point}: {percent}% used, {free_space} free", "DEBUG")
                    return percent, free_space
        log(config, logger, f"No matching df output for {mount_point}, falling back to psutil", "DEBUG")
        usage = psutil.disk_usage(mount_point)
        return int(usage.percent), f"{usage.free // 1024 // 1024 // 1024}G"
    except Exception as error:  # pylint: disable=broad-except
        log(config, logger, f"Error getting usage for {mount_point}: {error}", "ERROR")
        return 0, "N/A"


def check_package_version(config, logger):
    """Check and update the package version if needed."""
    try:
        if config.package_version == "latest":
            response = requests.get(
                "https://api.github.com/repos/joaofolino/mqtt-monitor/releases/latest",
                timeout=20,
            )
            version = response.json()["tag_name"].lstrip("v")
        else:
            version = config.package_version
        current_version, _ = run_cmd(
            config,
            logger,
            ["dpkg-query", "--showformat=${Version}", "--show", "mqtt-monitor"],
        )
        current_version = current_version.strip()
        if current_version != version:
            log(config, logger, f"Updating package from {current_version} to {version}", "INFO")
            deb_url = (
                f"https://github.com/joaofolino/mqtt-monitor/releases/download/"
                f"v{version}/mqtt-monitor_{version}.deb"
            )
            subprocess.run(["wget", "-O", "/tmp/mqtt-monitor.deb", deb_url], check=True)
            subprocess.run(["apt", "install", "-y", "/tmp/mqtt-monitor.deb"], check=True)
            log(config, logger, f"Updated to version {version}", "INFO")
            return True
    except Exception as error:  # pylint: disable=broad-except
        log(config, logger, f"Failed to check/update package version: {error}", "ERROR")
    return False


def setup_sensors(config, client):
    """Publish Home Assistant MQTT discovery payloads."""
    for sensor, unit in [
        ("cpu_load_percentage", "%"),
        ("memory_used_megabytes", "MB"),
        ("cpu_temperature_celsius", "°C"),
    ]:
        client.publish(
            f"homeassistant/sensor/server_{sensor}/config",
            json.dumps({
                "name": f"Server {sensor.replace('_', ' ').title()}",
                "state_topic": f"{config.topic_prefix}/host/{sensor}",
                "unique_id": f"server_monitor_server_{sensor}",
                "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
                "unit_of_measurement": unit,
            }),
            retain=True,
        )

    for mount_point in config.disk_usage_mounts:
        mount_name = mount_point.strip("/").replace("/", "_") or "root"
        client.publish(
            f"homeassistant/sensor/server_{mount_name}_disk_usage_percentage/config",
            json.dumps({
                "name": f"Server {mount_name.replace('_', ' ').title()} Disk Usage Percentage",
                "state_topic": f"{config.topic_prefix}/host/{mount_name}_disk_usage_percentage",
                "unique_id": f"server_monitor_server_{mount_name}_disk_usage_percentage",
                "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
                "unit_of_measurement": "%",
            }),
            retain=True,
        )

    for device_path in config.devices:
        device_name = device_path.split("/")[-1]
        for attr, unit in [
            ("max_temperature", "°C"),
            ("reallocated_sectors", ""),
            ("spin_up_count", ""),
            ("smart_health_status", None),
        ]:
            payload = {
                "name": f"Disk {device_name} {attr.replace('_', ' ').title()}",
                "state_topic": f"{config.topic_prefix}/disk/{device_name}/{attr}",
                "unique_id": f"server_monitor_disk_{device_name}_{attr}",
                "device": {"identifiers": ["server_monitor"]},
                "availability_topic": f"{config.topic_prefix}/disk/{device_name}/availability",
                "payload_available": "online",
                "payload_not_available": "offline",
            }
            if unit is not None:
                payload["unit_of_measurement"] = unit
            client.publish(
                f"homeassistant/sensor/disk_{device_name}_{attr}/config",
                json.dumps(payload),
                retain=True,
            )

        if config.enable_io_mqtt:
            for attr, unit in [
                ("read_kilobytes_per_second", "KB/s"),
                ("write_kilobytes_per_second", "KB/s"),
                ("read_count_per_second", "ops/s"),
                ("write_count_per_second", "ops/s"),
                ("busy_time_percentage", "%"),
            ]:
                client.publish(
                    f"homeassistant/sensor/disk_{device_name}_{attr}/config",
                    json.dumps({
                        "name": f"Disk {device_name} {attr.replace('_', ' ').title()}",
                        "state_topic": f"{config.topic_prefix}/disk/{device_name}/{attr}",
                        "unique_id": f"server_monitor_disk_{device_name}_{attr}",
                        "device": {"identifiers": ["server_monitor"]},
                        "unit_of_measurement": unit,
                        "availability_topic": f"{config.topic_prefix}/disk/{device_name}/availability",
                        "payload_available": "online",
                        "payload_not_available": "offline",
                    }),
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
            client.publish(
                f"homeassistant/binary_sensor/disk_{device_name}_{flag}/config",
                json.dumps({
                    "name": f"Disk {device_name} {flag.replace('_', ' ').title()}",
                    "state_topic": f"{config.topic_prefix}/disk/{device_name}/{flag}",
                    "unique_id": f"server_monitor_disk_{device_name}_{flag}",
                    "device": {"identifiers": ["server_monitor"]},
                    "payload_on": "true",
                    "payload_off": "false",
                    "availability_topic": f"{config.topic_prefix}/disk/{device_name}/availability",
                    "payload_available": "online",
                    "payload_not_available": "offline",
                }),
                retain=True,
            )

    for array in config.raid_arrays:
        client.publish(
            f"homeassistant/sensor/raid_{array}_status/config",
            json.dumps({
                "name": f"RAID {array} Status",
                "state_topic": f"{config.topic_prefix}/raid/{array}/status",
                "unique_id": f"server_monitor_raid_{array}_status",
                "device": {"identifiers": ["server_monitor"]},
                "json_attributes_topic": f"{config.topic_prefix}/raid/{array}/attributes",
            }),
            retain=True,
        )


def publish_data(config, logger, client, topic, value, retain=False):
    """Publish data to an MQTT topic."""
    try:
        if value is None:
            log(config, logger, f"Skipping MQTT publish for {topic}: value is None", "DEBUG")
            return
        if isinstance(value, bool):
            value = str(value).lower()
        elif isinstance(value, (int, float)):
            value = str(value)
        log(config, logger, f"MQTT publish: {topic} = {value}", "DEBUG")
        client.publish(topic, value, retain=retain)
    except ConnectionError as error:
        log(config, logger, f"MQTT publish error {topic}: {error}", "ERROR")
        connect_mqtt(config, logger, client)


def main():
    """Run the main monitoring loop."""
    config = load_config(os.getenv("CONFIG_PATH", "/etc/mqtt-monitor"))
    logger = setup_logging(config)
    state = MonitorState()
    client = mqtt.Client()
    connect_mqtt(config, logger, client)
    if check_package_version(config, logger):
        return
    setup_sensors(config, client)
    device_names = [device.split("/")[-1] for device in config.devices]
    last_smart_check = 0

    while True:
        try:
            current_time = time.time()
            delta_time = 60.0 if state.previous_loop_time is None else current_time - state.previous_loop_time
            state.previous_loop_time = current_time

            publish_data(
                config,
                logger,
                client,
                f"{config.topic_prefix}/host/cpu_load_percentage",
                psutil.cpu_percent(),
            )
            publish_data(
                config,
                logger,
                client,
                f"{config.topic_prefix}/host/memory_used_megabytes",
                psutil.virtual_memory().used // 1024 // 1024,
            )
            with open(config.cpu_temp_path, "r", encoding="utf-8") as file_handle:
                cpu_temperature = int(file_handle.read().strip()) / 1000
            publish_data(
                config,
                logger,
                client,
                f"{config.topic_prefix}/host/cpu_temperature_celsius",
                cpu_temperature,
            )

            current_io = {}
            for device_name in device_names:
                current_io[device_name] = get_disk_io(config, logger, device_name)
                previous_io = state.previous_io_stats.get(device_name)
                if not previous_io:
                    read_kbps = 0.0
                    write_kbps = 0.0
                    read_ops = 0.0
                    write_ops = 0.0
                    busy_percent = 0.0
                    log(config, logger, f"Priming baseline disk I/O counters for {device_name}", "DEBUG")
                else:
                    read_bytes_delta = current_io[device_name]["read_bytes"] - previous_io["read_bytes"]
                    write_bytes_delta = current_io[device_name]["write_bytes"] - previous_io["write_bytes"]
                    read_count_delta = current_io[device_name]["read_count"] - previous_io["read_count"]
                    write_count_delta = current_io[device_name]["write_count"] - previous_io["write_count"]
                    busy_time_delta = current_io[device_name]["busy_time"] - previous_io["busy_time"]
                    if min(
                        read_bytes_delta,
                        write_bytes_delta,
                        read_count_delta,
                        write_count_delta,
                        busy_time_delta,
                    ) < 0:
                        read_kbps = 0.0
                        write_kbps = 0.0
                        read_ops = 0.0
                        write_ops = 0.0
                        busy_percent = 0.0
                        log(config, logger, f"Disk I/O counters reset for {device_name}; re-priming baseline", "WARNING")
                    else:
                        read_kbps = min(read_bytes_delta / 1024 / delta_time, config.max_io_rate)
                        write_kbps = min(write_bytes_delta / 1024 / delta_time, config.max_io_rate)
                        read_ops = read_count_delta / delta_time
                        write_ops = write_count_delta / delta_time
                        busy_percent = min((busy_time_delta / 10) / delta_time, 100.0)

                if config.enable_io_mqtt:
                    publish_data(
                        config,
                        logger,
                        client,
                        f"{config.topic_prefix}/disk/{device_name}/read_kilobytes_per_second",
                        f"{read_kbps:.2f}",
                        retain=True,
                    )
                    publish_data(
                        config,
                        logger,
                        client,
                        f"{config.topic_prefix}/disk/{device_name}/write_kilobytes_per_second",
                        f"{write_kbps:.2f}",
                        retain=True,
                    )
                    publish_data(
                        config,
                        logger,
                        client,
                        f"{config.topic_prefix}/disk/{device_name}/read_count_per_second",
                        f"{read_ops:.2f}",
                        retain=True,
                    )
                    publish_data(
                        config,
                        logger,
                        client,
                        f"{config.topic_prefix}/disk/{device_name}/write_count_per_second",
                        f"{write_ops:.2f}",
                        retain=True,
                    )
                    publish_data(
                        config,
                        logger,
                        client,
                        f"{config.topic_prefix}/disk/{device_name}/busy_time_percentage",
                        f"{busy_percent:.2f}",
                        retain=True,
                    )

                if device_name == "md1" and write_kbps > config.md1_write_spike_threshold:
                    send_alert(config, logger, "md1 Write Spike", f"Write rate: {write_kbps:.2f} KB/s")
                state.previous_io_stats[device_name] = current_io[device_name]

            raid_status = get_raid_status(config, logger)
            for array, status in raid_status.items():
                if array not in config.raid_arrays:
                    config.raid_arrays.append(array)
                    publish_data(
                        config,
                        logger,
                        client,
                        f"homeassistant/sensor/raid_{array}_status/config",
                        json.dumps({
                            "name": f"RAID {array} Status",
                            "state_topic": f"{config.topic_prefix}/raid/{array}/status",
                            "unique_id": f"server_monitor_raid_{array}_status",
                            "device": {"identifiers": ["server_monitor"]},
                            "json_attributes_topic": f"{config.topic_prefix}/raid/{array}/attributes",
                        }),
                        retain=True,
                    )
                publish_data(
                    config,
                    logger,
                    client,
                    f"{config.topic_prefix}/raid/{array}/status",
                    status["status"],
                    retain=True,
                )
                publish_data(
                    config,
                    logger,
                    client,
                    f"{config.topic_prefix}/raid/{array}/attributes",
                    json.dumps({"failed_devices": status["failed_devices"]}),
                    retain=True,
                )
                if status["status"] == "degraded":
                    send_alert(config, logger, f"RAID {array} Degraded", f"Status: {status['raw']}")

            for mount_point in config.disk_usage_mounts:
                usage, free_space = get_usage(config, logger, mount_point)
                mount_name = mount_point.strip("/").replace("/", "_") or "root"
                publish_data(
                    config,
                    logger,
                    client,
                    f"{config.topic_prefix}/host/{mount_name}_disk_usage_percentage",
                    usage,
                )
                log(config, logger, f"{mount_point}: {usage}% used, {free_space} free", "INFO")
                if usage > config.md1_usage_threshold:
                    mount_title = mount_name.replace("_", " ").title()
                    send_alert(
                        config,
                        logger,
                        f"{mount_title} Capacity Critical",
                        f"{mount_point} at {usage}%, only {free_space} free",
                    )

            if current_time - last_smart_check >= config.smart_interval:
                for device_path in config.devices:
                    disk_cache[device_path] = get_disk_info(config, logger, device_path, full=True)
                last_smart_check = current_time
            else:
                for device_path in config.devices:
                    if disk_cache.get(device_path, {}).get("availability") != "online":
                        disk_cache[device_path] = get_disk_info(config, logger, device_path, full=False)

            for device_path in config.devices:
                device_name = device_path.split("/")[-1]
                disk_info = disk_cache[device_path]
                publish_data(
                    config,
                    logger,
                    client,
                    f"{config.topic_prefix}/disk/{device_name}/availability",
                    disk_info.get("availability"),
                    retain=True,
                )
                for attr in ["max_temperature", "reallocated_sectors", "spin_up_count", "smart_health_status"]:
                    publish_data(
                        config,
                        logger,
                        client,
                        f"{config.topic_prefix}/disk/{device_name}/{attr}",
                        disk_info.get(attr),
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
                        f"{config.topic_prefix}/disk/{device_name}/{flag}",
                        disk_info.get(flag, False),
                        retain=True,
                    )
                log(
                    config,
                    logger,
                    (
                        f"{device_path}: smart_health_status={disk_info.get('smart_health_status', 'N/A')}, "
                        f"max_temperature={disk_info.get('max_temperature', 'N/A')}°C, "
                        f"availability={disk_info.get('availability')}"
                    ),
                    "INFO",
                )

            time.sleep(config.sleep_interval)
        except (ConnectionError, IOError, RuntimeError) as error:
            log(config, logger, f"Main loop error: {error}", "ERROR")
            time.sleep(config.error_sleep)
        except Exception as error:  # pylint: disable=broad-except
            logger.exception("Unexpected main loop error: %s", error)
            time.sleep(config.error_sleep)


if __name__ == "__main__":
    main()
