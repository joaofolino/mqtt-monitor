"""System collection and compatibility helpers for mqtt-monitor v2."""

from __future__ import annotations

import logging
import json
import os
import smtplib
import subprocess
import time
from datetime import datetime, timezone
from email.mime.text import MIMEText

import psutil
import requests

from .models import ContainerMetric, DiskMetric, FilesystemMetric, HostMetrics, MonitorSnapshot, MonitorState, RaidMetric


disk_cache: dict[str, dict] = {}
LXC_HELPER_PATH = "/usr/share/mqtt-monitor/lxc_inventory_helper.py"


def utc_now_iso() -> str:
    """Return an RFC3339 UTC timestamp without fractional seconds."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_log_file_path(log_dir: str, log_file: str) -> str:
    """Return the effective log file path."""
    return log_file if os.path.isabs(log_file) else os.path.join(os.path.abspath(log_dir), log_file)


def unique_preserving_order(values: list[str]) -> list[str]:
    """Return values without duplicates while preserving order."""
    seen = set()
    result = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def setup_logging(config):
    """Set up logging to file and console."""
    logger = logging.getLogger("mqtt_monitor")
    logger.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    logger.handlers = []
    log_file_path = resolve_log_file_path(config.log_dir, config.log_file)
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
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
    """Send an email alert when email alerts are enabled."""
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
    except (FileNotFoundError, subprocess.SubprocessError, subprocess.TimeoutExpired) as error:
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
    except Exception as error:
        log(config, logger, f"Error getting I/O for {device_name}: {error}", "ERROR")
        return empty_io_stats()


def parse_smart_output(config, logger, smart_output, device_path):
    """Parse SMART output into a dictionary."""
    smart_lines = smart_output.splitlines()
    smart_health_status = None
    for line in smart_lines:
        if "SMART overall-health self-assessment test result:" in line:
            status = line.split(":")[-1].strip().upper()
        elif line.startswith("SMART Health Status:"):
            status = line.split(":", 1)[1].strip().upper()
            if status == "OK":
                status = "PASSED"
        else:
            continue
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
        elif line.startswith("Current Drive Temperature:") and len(parts) > 3 and parts[3].isdigit():
            temperature_readings.append(int(parts[3]))
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
    """Get disk information including capacity and SMART attributes."""
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

    if is_logical_device_path(device_path):
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
    last_result = None
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
            last_result = disk_smart_info
            log(config, logger, f"{device_path} probe with -d {device_type} failed, trying next mode", "DEBUG")
            continue
        log(config, logger, f"{device_path} availability: {disk_smart_info['availability']}", "DEBUG")
        return disk_smart_info

    log(config, logger, f"Failed to get SMART data for {device_path} after all attempts", "ERROR")
    if last_result:
        return last_result
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
    except Exception as error:
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
        try:
            comparison = subprocess.run(
                ["dpkg", "--compare-versions", current_version, "lt", version],
                check=False,
            )
            should_update = comparison.returncode == 0
            if comparison.returncode not in {0, 1}:
                should_update = current_version != version
        except FileNotFoundError:
            should_update = current_version != version
        if should_update:
            log(config, logger, f"Updating package from {current_version} to {version}", "INFO")
            deb_url = (
                f"https://github.com/joaofolino/mqtt-monitor/releases/download/"
                f"v{version}/mqtt-monitor_{version}.deb"
            )
            subprocess.run(["wget", "-O", "/tmp/mqtt-monitor.deb", deb_url], check=True)
            subprocess.run(["apt", "install", "-y", "/tmp/mqtt-monitor.deb"], check=True)
            log(config, logger, f"Updated to version {version}", "INFO")
            return True
    except Exception as error:
        log(config, logger, f"Failed to check/update package version: {error}", "ERROR")
    return False


def get_host_metrics(config, logger) -> HostMetrics:
    """Collect host-level metrics."""
    cpu_temperature = None
    try:
        with open(config.cpu_temp_path, "r", encoding="utf-8") as file_handle:
            cpu_temperature = int(file_handle.read().strip()) / 1000
    except (FileNotFoundError, ValueError, OSError) as error:
        log(config, logger, f"Unable to read CPU temperature: {error}", "DEBUG")
    return HostMetrics(
        cpu_load_percentage=psutil.cpu_percent(),
        memory_used_megabytes=psutil.virtual_memory().used // 1024 // 1024,
        cpu_temperature_celsius=cpu_temperature,
    )


def device_is_ignored(device_name: str, ignored_prefixes: list[str]) -> bool:
    """Return whether a device name should be ignored by discovery."""
    return any(device_name.startswith(prefix) for prefix in ignored_prefixes)


def normalize_device_path(device: str) -> str:
    """Normalize a configured or discovered device name into a /dev path."""
    return device if device.startswith("/dev/") else f"/dev/{device}"


def device_display_name(device_path: str) -> str:
    """Return the preferred external name for a device path."""
    return os.path.basename(device_path)


def io_counter_device_name(device_path: str) -> str:
    """Return the kernel device name used by psutil for I/O counters."""
    return os.path.basename(os.path.realpath(device_path))


def is_logical_device_path(device_path: str) -> bool:
    """Return whether a device path represents a logical block device."""
    if device_path.startswith("/dev/md") or device_path.startswith("/dev/mapper/"):
        return True
    return os.path.basename(os.path.realpath(device_path)).startswith("dm-")


def discover_mapper_devices_for_mounts(mount_points: list[str]) -> list[str]:
    """Return device-mapper paths backing monitored mount points."""
    mapper_devices = []
    monitored_mounts = set(mount_points)
    for partition in psutil.disk_partitions(all=False):
        if partition.mountpoint not in monitored_mounts:
            continue
        if not partition.device.startswith("/dev/mapper/"):
            continue
        if os.path.exists(partition.device):
            mapper_devices.append(partition.device)
    return unique_preserving_order(mapper_devices)


def discover_disk_devices(config, mount_points: list[str] | None = None) -> list[str]:
    """Discover block devices to monitor."""
    devices = [normalize_device_path(device) for device in config.devices]
    if config.auto_discover_disks and os.path.isdir("/sys/block"):
        for device_name in sorted(os.listdir("/sys/block")):
            if device_is_ignored(device_name, config.ignored_devices):
                continue
            device_path = normalize_device_path(device_name)
            if os.path.exists(device_path):
                devices.append(device_path)
    if mount_points:
        devices.extend(discover_mapper_devices_for_mounts(mount_points))
    return unique_preserving_order(devices)


def mount_is_ignored(mount_point: str, ignored_mounts: list[str]) -> bool:
    """Return whether a mount point should be ignored by discovery."""
    for ignored_mount in ignored_mounts:
        if mount_point == ignored_mount or mount_point.startswith(f"{ignored_mount}/"):
            return True
    return False


def is_internal_container_mount(mount_point: str) -> bool:
    """Return whether a mount point is an internal LXC bind mount."""
    return mount_point.startswith("/var/lib/lxc/")


def discover_mount_points(config) -> list[str]:
    """Discover mount points to monitor."""
    mount_points = list(config.disk_usage_mounts)
    if config.auto_discover_mounts:
        for partition in psutil.disk_partitions(all=False):
            if mount_is_ignored(partition.mountpoint, config.ignored_mounts):
                continue
            if is_internal_container_mount(partition.mountpoint):
                continue
            if partition.fstype.lower() in {"tmpfs", "devtmpfs", "squashfs", "overlay"}:
                continue
            mount_points.append(partition.mountpoint)
    return unique_preserving_order(mount_points)


def read_lxc_autostart(container_name: str) -> bool:
    """Return whether an LXC container is configured to autostart."""
    config_path = os.path.join("/var/lib/lxc", container_name, "config")
    if not os.path.exists(config_path):
        return False
    try:
        with open(config_path, "r", encoding="utf-8") as file_handle:
            for raw_line in file_handle:
                line = raw_line.strip()
                if line.startswith("#") or "=" not in line:
                    continue
                key, value = [part.strip() for part in line.split("=", 1)]
                if key == "lxc.start.auto":
                    return value in {"1", "true", "True", "yes", "on"}
    except OSError:
        return False
    return False


def parse_lxc_names(command_output: str) -> list[str]:
    """Parse an lxc-ls style command into a list of container names."""
    return [line.strip() for line in command_output.splitlines() if line.strip()]


def parse_lxc_inventory_helper_output(command_output: str) -> dict[str, ContainerMetric] | None:
    """Parse helper JSON output into container metrics."""
    if not command_output:
        return {}
    try:
        payload = json.loads(command_output)
    except json.JSONDecodeError:
        return None

    raw_containers = payload.get("containers", payload) if isinstance(payload, dict) else payload
    if not isinstance(raw_containers, list):
        return None

    containers: dict[str, ContainerMetric] = {}
    for raw_container in raw_containers:
        if not isinstance(raw_container, dict):
            continue
        name = str(raw_container.get("name", "")).strip()
        if not name:
            continue
        status = str(raw_container.get("status", "UNKNOWN")).strip().upper() or "UNKNOWN"
        containers[name] = ContainerMetric(
            name=name,
            status=status,
            autostart=bool(raw_container.get("autostart", False)),
        )
    return containers


def lxc_helper_command() -> list[str]:
    """Return the command used to execute the privileged LXC helper."""
    geteuid = getattr(os, "geteuid", lambda: -1)
    if geteuid() == 0:
        return [LXC_HELPER_PATH]
    return ["sudo", "-n", LXC_HELPER_PATH]


def discover_lxc_containers_with_helper(config, logger) -> dict[str, ContainerMetric] | None:
    """Discover LXC containers through the packaged privileged helper."""
    if not config.auto_discover_lxc or not os.path.exists(LXC_HELPER_PATH):
        return None

    output, exit_code = run_cmd(config, logger, lxc_helper_command())
    if exit_code != 0:
        log(config, logger, "Privileged LXC helper failed; falling back to direct discovery", "WARNING")
        return None

    containers = parse_lxc_inventory_helper_output(output)
    if containers is None:
        log(config, logger, "Privileged LXC helper returned invalid JSON; falling back to direct discovery", "WARNING")
        return None

    log(config, logger, f"Discovered {len(containers)} LXC containers via privileged helper", "DEBUG")
    return containers


def discover_lxc_containers_with_legacy_commands(config, logger) -> dict[str, ContainerMetric]:
    """Discover LXC containers using direct classic-LXC commands."""
    discovered_names = []
    if config.auto_discover_lxc:
        output, exit_code = run_cmd(config, logger, ["lxc-ls", "-1"])
        if exit_code == 0 and output:
            discovered_names = parse_lxc_names(output)

    running_names = set()
    output, exit_code = run_cmd(config, logger, ["lxc-ls", "-1", "--running"])
    if exit_code == 0 and output:
        running_names = set(parse_lxc_names(output))

    return {
        name: ContainerMetric(
            name=name,
            status="RUNNING" if name in running_names else "STOPPED",
            autostart=read_lxc_autostart(name),
        )
        for name in discovered_names
    }


def discover_lxc_containers(config, logger) -> list[ContainerMetric]:
    """Discover LXC containers and their runtime state."""
    discovered_containers = discover_lxc_containers_with_helper(config, logger)
    if discovered_containers is None:
        discovered_containers = discover_lxc_containers_with_legacy_commands(config, logger)

    all_names = unique_preserving_order(list(config.expected_lxc_containers) + list(discovered_containers))
    if not all_names:
        return []

    return [
        discovered_containers.get(name, ContainerMetric(name=name, status="STOPPED", autostart=False))
        for name in all_names
    ]


def discover_raid_arrays(config, raid_status: dict[str, dict]) -> list[str]:
    """Discover RAID arrays to monitor."""
    arrays = list(config.raid_arrays)
    if config.auto_discover_raid_arrays:
        arrays.extend(sorted(raid_status.keys()))
    return unique_preserving_order(arrays)


def calculate_disk_rates(config, logger, device_name: str, current_io: dict, previous_io: dict | None, delta_time: float):
    """Calculate per-second disk rates from cumulative counters."""
    if not previous_io:
        log(config, logger, f"Priming baseline disk I/O counters for {device_name}", "DEBUG")
        return 0.0, 0.0, 0.0, 0.0, 0.0

    read_bytes_delta = current_io["read_bytes"] - previous_io["read_bytes"]
    write_bytes_delta = current_io["write_bytes"] - previous_io["write_bytes"]
    read_count_delta = current_io["read_count"] - previous_io["read_count"]
    write_count_delta = current_io["write_count"] - previous_io["write_count"]
    busy_time_delta = current_io["busy_time"] - previous_io["busy_time"]
    if min(read_bytes_delta, write_bytes_delta, read_count_delta, write_count_delta, busy_time_delta) < 0:
        log(config, logger, f"Disk I/O counters reset for {device_name}; re-priming baseline", "WARNING")
        return 0.0, 0.0, 0.0, 0.0, 0.0

    safe_delta = max(delta_time, 1e-6)
    read_kbps = min(read_bytes_delta / 1024 / safe_delta, config.max_io_rate)
    write_kbps = min(write_bytes_delta / 1024 / safe_delta, config.max_io_rate)
    read_ops = read_count_delta / safe_delta
    write_ops = write_count_delta / safe_delta
    busy_percent = min((busy_time_delta / 10) / safe_delta, 100.0)
    return read_kbps, write_kbps, read_ops, write_ops, busy_percent


def collect_snapshot(config, logger, state: MonitorState, current_time: float | None = None) -> MonitorSnapshot:
    """Collect a full system snapshot."""
    now = current_time if current_time is not None else time.time()
    delta_time = config.sleep_interval if state.previous_loop_time is None else max(now - state.previous_loop_time, 1e-6)
    state.previous_loop_time = now

    mount_points = discover_mount_points(config)
    raid_status = get_raid_status(config, logger)
    device_paths = discover_disk_devices(config, mount_points=mount_points)
    array_names = discover_raid_arrays(config, raid_status)
    containers = discover_lxc_containers(config, logger)

    full_smart_refresh = now - state.last_smart_check_time >= config.smart_interval
    if full_smart_refresh:
        state.last_smart_check_time = now

    host = get_host_metrics(config, logger)
    filesystems = []
    for mount_point in mount_points:
        usage_percentage, free_space = get_usage(config, logger, mount_point)
        filesystems.append(FilesystemMetric(mount_point=mount_point, usage_percentage=usage_percentage, free_space=free_space))

    disks = []
    for device_path in device_paths:
        device_name = device_display_name(device_path)
        current_io = get_disk_io(config, logger, io_counter_device_name(device_path))
        read_kbps, write_kbps, read_ops, write_ops, busy_percent = calculate_disk_rates(
            config,
            logger,
            device_name,
            current_io,
            state.previous_io_stats.get(device_name),
            delta_time,
        )
        state.previous_io_stats[device_name] = current_io

        if full_smart_refresh or device_path not in disk_cache:
            disk_cache[device_path] = get_disk_info(config, logger, device_path, full=True)
        elif disk_cache.get(device_path, {}).get("availability") != "online":
            disk_cache[device_path] = get_disk_info(config, logger, device_path, full=False)
        disk_info = disk_cache.get(device_path) or get_disk_info(config, logger, device_path, full=False)

        disks.append(
            DiskMetric(
                path=device_path,
                name=device_name,
                capacity_gib=disk_info.get("capacity", 0),
                capacity_bytes=disk_info.get("capacity_bytes"),
                availability=disk_info.get("availability", "unknown"),
                smart_health_status=disk_info.get("smart_health_status"),
                max_temperature=disk_info.get("max_temperature"),
                power_on_hours=disk_info.get("power_on_hours"),
                reallocated_sectors=disk_info.get("reallocated_sectors"),
                spin_up_count=disk_info.get("spin_up_count"),
                read_kilobytes_per_second=read_kbps,
                write_kilobytes_per_second=write_kbps,
                read_count_per_second=read_ops,
                write_count_per_second=write_ops,
                busy_time_percentage=busy_percent,
                command_line_parse_error=disk_info.get("command_line_parse_error", False),
                device_open_failed=disk_info.get("device_open_failed", False),
                smart_command_failed=disk_info.get("smart_command_failed", False),
                disk_failing=disk_info.get("disk_failing", False),
                prefail_attributes_threshold=disk_info.get("prefail_attributes_threshold", False),
                past_attributes_threshold=disk_info.get("past_attributes_threshold", False),
                device_error_log_errors=disk_info.get("device_error_log_errors", False),
                self_test_log_errors=disk_info.get("self_test_log_errors", False),
            )
        )

    raids = []
    for array_name in array_names:
        array_state = raid_status.get(array_name, {"status": "unknown", "raw": "", "failed_devices": []})
        raids.append(
            RaidMetric(
                name=array_name,
                status=array_state.get("status", "unknown"),
                raw=array_state.get("raw", ""),
                failed_devices=array_state.get("failed_devices", []),
            )
        )

    return MonitorSnapshot(
        generated_at=utc_now_iso(),
        host=host,
        filesystems=filesystems,
        disks=disks,
        raids=raids,
        containers=containers,
    )
