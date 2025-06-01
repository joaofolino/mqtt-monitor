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
import signal
import sys
import time
from dataclasses import dataclass
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt
import psutil
import requests
import smtplib
import subprocess

# Constant Definitions
HOST_SENSORS = [
    ("cpu_load_percentage", "%"),
    ("memory_used_megabytes", "MB"),
    ("cpu_temperature_celsius", "°C")
]
DISK_SENSOR_ATTRIBUTES = [
    ("max_temperature", "°C"),
    ("reallocated_sectors", ""),
    ("spin_up_count", ""),
    ("smart_health_status", None)
]
DISK_IO_ATTRIBUTES = [
    ("read_kilobytes_per_second", "KB/s"),
    ("write_kilobytes_per_second", "KB/s"),
    ("read_count_per_second", "ops/s"),
    ("write_count_per_second", "ops/s"),
    ("busy_time_percentage", "%")
]
DISK_FLAGS = [
    "command_line_parse_error",
    "device_open_failed",
    "smart_command_failed",
    "disk_failing",
    "prefail_attributes_threshold",
    "past_attributes_threshold",
    "device_error_log_errors",
    "self_test_log_errors"
]

@dataclass
class MonitorConfig:
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
    devices: List[str]
    raid_arrays: List[str]
    mdstat_path: str
    cpu_temp_path: str
    disk_usage_mounts: List[str]
    max_io_rate: float
    md1_usage_threshold: float
    md1_write_spike_threshold: float
    sleep_interval: float
    error_sleep: float
    subprocess_timeout: float = 10.0  # Added configurable timeout

def load_config(config_path: str = '/etc/mqtt-monitor') -> MonitorConfig:
    """Load configuration from a file.

    Args:
        config_path: Path to the configuration directory.

    Returns:
        MonitorConfig: Parsed configuration object.

    Raises:
        FileNotFoundError: If the config file is missing or unreadable.
        ValueError: If required configuration options are missing or invalid.
    """
    config_parser = configparser.ConfigParser()
    config_file_path = f"{config_path}/mqtt-monitor.conf"
    print(f"Loading configuration from: {config_file_path}")
    if not config_parser.read(config_file_path):
        raise FileNotFoundError(f"Configuration file {config_file_path} not found or unreadable")
    try:
        config = MonitorConfig(
            enable_email=config_parser.getboolean('monitor', 'enable_email', fallback=False),
            log_level=config_parser.get('monitor', 'log_level', fallback='INFO'),
            enable_io_mqtt=config_parser.getboolean('monitor', 'enable_io_mqtt', fallback=False),
            alert_email=config_parser.get('monitor', 'alert_email', fallback='user@example.com'),
            smtp_server=config_parser.get('monitor', 'smtp_server', fallback='smtp.gmail.com'),
            smtp_port=config_parser.getint('monitor', 'smtp_port', fallback=587),
            smtp_user=config_parser.get('monitor', 'smtp_user', fallback='user@example.com'),
            smtp_pass=config_parser.get('monitor', 'smtp_pass', fallback='your-app-password'),
            package_version=config_parser.get('monitor', 'package_version', fallback='latest'),
            broker=config_parser.get('mqtt', 'broker', fallback='localhost'),
            port=config_parser.getint('mqtt', 'port', fallback=1883),
            mqtt_user=config_parser.get('mqtt', 'mqtt_user', fallback='mqtt-user'),
            mqtt_pass=config_parser.get('mqtt', 'mqtt_pass', fallback='mqtt-password'),
            self_name=config_parser.get('mqtt', 'self_name', fallback='mqtt-monitor'),
            smart_interval=config_parser.getint('internals', 'smart_interval'),
            topic_prefix=config_parser.get('internals', 'topic_prefix'),
            log_dir=config_parser.get('internals', 'log_dir'),
            log_file=config_parser.get('internals', 'log_file'),
            devices=config_parser.get('internals', 'devices').split(','),
            raid_arrays=config_parser.get('internals', 'raid_arrays', fallback='').split(','),
            mdstat_path=config_parser.get('internals', 'mdstat_path'),
            cpu_temp_path=config_parser.get('internals', 'cpu_temp_path'),
            disk_usage_mounts=config_parser.get('internals', 'disk_usage_mounts').split(','),
            max_io_rate=config_parser.getfloat('internals', 'max_io_rate'),
            md1_usage_threshold=config_parser.getfloat('internals', 'md1_usage_threshold'),
            md1_write_spike_threshold=config_parser.getfloat('internals', 'md1_write_spike_threshold'),
            sleep_interval=config_parser.getfloat('internals', 'sleep_interval'),
            error_sleep=config_parser.getfloat('internals', 'error_sleep')
        )
        if not config.devices:
            raise ValueError("No devices specified in configuration")
        if config.smtp_port < 1 or config.smtp_port > 65535:
            raise ValueError("Invalid SMTP port")
        return config
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        print(f"Configuration error: {e}")
        raise

disk_info_cache: Dict[str, Dict[str, Any]] = {}

class MonitorState:
    """Holds state for monitoring loop."""
    def __init__(self):
        self.previous_io_stats: Dict[str, Dict[str, int]] = {}
        self.previous_loop_time: Optional[float] = None

def setup_logging(monitor_config: MonitorConfig) -> logging.Logger:
    """Set up logging to file and console.

    Args:
        monitor_config: Configuration object containing log settings.

    Returns:
        logging.Logger: Configured logger instance.
    """
    monitor_logger = logging.getLogger(__name__)
    monitor_logger.setLevel(getattr(logging, monitor_config.log_level.upper(), logging.DEBUG))
    monitor_logger.handlers = []  # Clear existing handlers
    log_file_path = os.path.join(os.path.abspath(monitor_config.log_dir), monitor_config.log_file)
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(getattr(logging, monitor_config.log_level.upper(), logging.DEBUG))
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    monitor_logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(getattr(logging, monitor_config.log_level.upper(), logging.DEBUG))
    stream_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    monitor_logger.addHandler(stream_handler)
    return monitor_logger

def log(monitor_config: MonitorConfig, monitor_logger: logging.Logger, message: str, level: str = "INFO") -> None:
    """Log a message at the specified level.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        message: Message to log.
        level: Log level (e.g., INFO, ERROR).
    """
    try:
        log_level = getattr(logging, level.upper(), logging.INFO)
        if monitor_logger.isEnabledFor(log_level):
            getattr(monitor_logger, level.lower())(message)
    except RuntimeError as e:
        print(f"Logging error: {e}")

def on_connect(monitor_config: MonitorConfig, monitor_logger: logging.Logger, mqtt_client: mqtt.Client,
               userdata: Any, flags: Dict[str, Any], rc: int) -> None:
    """Handle MQTT connection event.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        mqtt_client: MQTT client instance.
        userdata: User data (unused).
        flags: Connection flags (unused).
        rc: Connection result code.
    """
    if rc == 0:
        log(monitor_config, monitor_logger, "Connected to MQTT broker", "INFO")
    else:
        log(monitor_config, monitor_logger, f"Failed to connect to MQTT broker, code: {rc}", "ERROR")

def connect_mqtt(monitor_config: MonitorConfig, monitor_logger: logging.Logger, mqtt_client: mqtt.Client,
                 max_retries: int = 5) -> bool:
    """Connect to MQTT broker with exponential backoff.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        mqtt_client: MQTT client instance.
        max_retries: Maximum number of connection retries.

    Returns:
        bool: True if connection successful, False otherwise.
    """
    try:
        mqtt_client.username_pw_set(monitor_config.mqtt_user, monitor_config.mqtt_pass)
        mqtt_client.on_connect = lambda c, u, f, r: on_connect(monitor_config, monitor_logger, c, u, f, r)
        for attempt in range(max_retries):
            try:
                mqtt_client.connect(monitor_config.broker, monitor_config.port, 60)
                mqtt_client.loop_start()
                return True
            except ConnectionError as e:
                log(monitor_config, monitor_logger, f"Retry {attempt+1}/{max_retries} failed: {e}", "ERROR")
                time.sleep(2 ** attempt)  # Exponential backoff
        log(monitor_config, monitor_logger, "Failed to connect to MQTT after retries", "ERROR")
        return False
    except ConnectionError as e:
        log(monitor_config, monitor_logger, f"Failed to connect to MQTT broker: {e}", "ERROR")
        return False

def send_alert(monitor_config: MonitorConfig, monitor_logger: logging.Logger, subject: str, body: str) -> None:
    """Send an email alert.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        subject: Email subject.
        body: Email body.
    """
    if not monitor_config.enable_email:
        log(monitor_config, monitor_logger, f"Email alerts disabled: {subject}", "INFO")
        return
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = monitor_config.smtp_user
        msg['To'] = monitor_config.alert_email
        with smtplib.SMTP(monitor_config.smtp_server, monitor_config.smtp_port, timeout=10) as server:
            server.starttls()
            server.login(monitor_config.smtp_user, monitor_config.smtp_pass)
            server.sendmail(monitor_config.smtp_user, monitor_config.alert_email, msg.as_string())
        log(monitor_config, monitor_logger, f"Sent alert: {subject}", "INFO")
    except (smtplib.SMTPException, ConnectionError, TimeoutError) as e:
        log(monitor_config, monitor_logger, f"Failed to send alert {subject}: {e}", "ERROR")

def run_cmd(monitor_config: MonitorConfig, monitor_logger: logging.Logger, command: List[str]) -> Tuple[str, int]:
    """Run a shell command and return its output and exit code.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        command: Command list to execute.

    Returns:
        Tuple[str, int]: Command output and exit code.
    """
    try:
        log(monitor_config, monitor_logger, f"Executing command: {' '.join(command)}", "DEBUG")
        command_result = subprocess.run(command, capture_output=True, text=True, check=False,
                                        timeout=monitor_config.subprocess_timeout)
        log(monitor_config, monitor_logger,
            f"Command {' '.join(command)} output: {command_result.stdout[:100]}... (exit_code: {command_result.returncode})",
            "DEBUG")
        return command_result.stdout.strip(), command_result.returncode
    except (subprocess.SubprocessError, subprocess.TimeoutExpired) as e:
        error_output = getattr(e, 'stderr', 'N/A')
        log(monitor_config, monitor_logger, f"Command {' '.join(command)} error: {e} (stderr: {error_output})", "ERROR")
        return "", -1

def parse_smartctl_exit_code(exit_code: int) -> Dict[str, bool]:
    """Parse smartctl exit code into boolean flags using bit shifts.

    Args:
        exit_code: SMART command exit code.

    Returns:
        Dict[str, bool]: Dictionary of parsed flags.
    """
    if exit_code == 0:
        return {
            "command_line_parse_error": False,
            "device_open_failed": False,
            "smart_command_failed": False,
            "disk_failing": False,
            "prefail_attributes_threshold": False,
            "past_attributes_threshold": False,
            "device_error_log_errors": False,
            "self_test_log_errors": False
        }
    return {
        "command_line_parse_error": bool(exit_code & (1 << 0)),
        "device_open_failed": bool(exit_code & (1 << 1)),
        "smart_command_failed": bool(exit_code & (1 << 2)),
        "disk_failing": bool(exit_code & (1 << 3)),
        "prefail_attributes_threshold": bool(exit_code & (1 << 4)),
        "past_attributes_threshold": bool(exit_code & (1 << 5)),
        "device_error_log_errors": bool(exit_code & (1 << 6)),
        "self_test_log_errors": bool(exit_code & (1 << 7))
    }

def get_raid_status(monitor_config: MonitorConfig, monitor_logger: logging.Logger) -> Dict[str, Dict[str, str]]:
    """Get RAID array status from mdstat.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.

    Returns:
        Dict[str, Dict[str, str]]: RAID array status dictionary.
    """
    try:
        with open(monitor_config.mdstat_path, "r", encoding="utf-8") as f:
            mdstat = f.read()
        raid_arrays: Dict[str, Dict[str, str]] = {}
        current_raid_array = None
        raid_status_lines = []
        for line in mdstat.splitlines():
            line = line.strip()
            if not line or line.startswith("unused devices"):
                continue
            if line.startswith("md"):
                if current_raid_array and raid_status_lines:
                    raid_arrays[current_raid_array] = {
                        "status": "active" if any("[UU]" in l for l in raid_status_lines) else "degraded",
                        "raw": " ".join(raid_status_lines)
                    }
                current_raid_array = line.split()[0]
                raid_status_lines = [line]
            elif current_raid_array:
                raid_status_lines.append(line)
        if current_raid_array and raid_status_lines:
            raid_arrays[current_raid_array] = {
                "status": "active" if any("[UU]" in l for l in raid_status_lines) else "degraded",
                "raw": " ".join(raid_status_lines)
            }
        if not raid_arrays:
            return {"md0": {"status": "unknown", "raw": ""}}
        log(monitor_config, monitor_logger, f"RAID status: {raid_arrays}", "INFO")
        log(monitor_config, monitor_logger, f"Raw mdstat: {mdstat[:200]}...", "DEBUG")
        return raid_arrays
    except (FileNotFoundError, IOError) as e:
        log(monitor_config, monitor_logger, f"Error reading mdstat: {e}", "ERROR")
        return {"md0": {"status": "unknown", "raw": ""}}

def get_disk_io(monitor_config: MonitorConfig, monitor_logger: logging.Logger, device_name: str) -> Dict[str, int]:
    """Get disk I/O statistics.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        device_name: Name of the disk device.

    Returns:
        Dict[str, int]: Disk I/O statistics.
    """
    try:
        io_counters = psutil.disk_io_counters(perdisk=True)
        if device_name in io_counters:
            io_stats = io_counters[device_name]
            log(monitor_config, monitor_logger, f"{device_name} I/O stats: "
                                                f"read_count={io_stats.read_count}, "
                                                f"write_count={io_stats.write_count}, "
                                                f"read_bytes={io_stats.read_bytes}, "
                                                f"write_bytes={io_stats.write_bytes}, "
                                                f"read_time={io_stats.read_time}ms, "
                                                f"write_time={io_stats.write_time}ms, "
                                                f"busy_time={io_stats.busy_time}ms, "
                                                f"read_merged_count={io_stats.read_merged_count}, "
                                                f"write_merged_count={io_stats.write_merged_count}", "DEBUG")
            return {
                "read_count": io_stats.read_count,
                "write_count": io_stats.write_count,
                "read_bytes": io_stats.read_bytes,
                "write_bytes": io_stats.write_bytes,
                "read_time": io_stats.read_time,
                "write_time": io_stats.write_time,
                "busy_time": io_stats.busy_time,
                "read_merged_count": io_stats.read_merged_count,
                "write_merged_count": io_stats.write_merged_count
            }
        log(monitor_config, monitor_logger, f"No I/O data for {device_name}", "ERROR")
        return {
            "read_count": 0,
            "write_count": 0,
            "read_bytes": 0,
            "write_bytes": 0,
            "read_time": 0,
            "write_time": 0,
            "busy_time": 0,
            "read_merged_count": 0,
            "write_merged_count": 0
        }
    except (psutil.NoSuchProcess, psutil.AccessDenied, KeyError) as e:
        log(monitor_config, monitor_logger, f"Error getting I/O for {device_name}: {e}", "ERROR")
        return {
            "read_count": 0,
            "write_count": 0,
            "read_bytes": 0,
            "write_bytes": 0,
            "read_time": 0,
            "write_time": 0,
            "busy_time": 0,
            "read_merged_count": 0,
            "write_merged_count": 0
        }

def parse_smart_output(monitor_config: MonitorConfig, monitor_logger: logging.Logger,
                       smart_output: str, device_path: str) -> Dict[str, Any]:
    """Parse SMART output into a dictionary.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        smart_output: SMART command output.
        device_path: Path to the disk device.

    Returns:
        Dict[str, Any]: Parsed SMART attributes.
    """
    smart_lines = smart_output.splitlines()
    smart_health_status = None
    for line in smart_lines:
        if "SMART overall-health self-assessment test result:" in line:
            status = line.split(":")[-1].strip().upper()
            if status in ["PASSED", "FAILED", "UNKNOWN"]:
                smart_health_status = status
            else:
                log(monitor_config, monitor_logger, f"Unexpected SMART status for {device_path}: {status}", "WARNING")
                smart_health_status = "UNKNOWN"
            break
    smart_attributes = {
        "availability": "online",
        "smart_health_status": smart_health_status,
        "max_temperature": None,
        "power_on_hours": None,
        "reallocated_sectors": None,
        "spin_up_count": None
    }
    if not smart_lines or not smart_health_status:
        log(monitor_config, monitor_logger, f"Invalid or empty SMART output for {device_path}: {smart_output[:100]}", "ERROR")
        smart_attributes.update({
            "availability": "offline",
            "smart_health_status": None,
            "max_temperature": None,
            "power_on_hours": None,
            "reallocated_sectors": None,
            "spin_up_count": None
        })
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
        elif "Current_Pending_Sector" in line and len(parts) >= 10 and parts[9].isdigit():
            log(monitor_config, monitor_logger, f"{device_path} Pending Sectors: {parts[9]}", "INFO")
    if temperature_readings:
        max_temperature = max(temperature_readings)
    else:
        max_temperature = None
    smart_attributes.update({
        "max_temperature": max_temperature,
        "power_on_hours": power_on_hours,
        "reallocated_sectors": reallocated_sectors,
        "spin_up_count": spin_up_count
    })
    if smart_health_status == "FAILED":
        send_alert(monitor_config, monitor_logger, f"Disk {device_path} SMART Failure",
                   f"SMART status: {smart_health_status}, Output: {smart_output[:200]}")
    return smart_attributes

def get_disk_info(monitor_config: MonitorConfig, monitor_logger: logging.Logger,
                  device_path: str, full: bool = False) -> Dict[str, Any]:
    """Get disk information (capacity, SMART attributes).

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        device_path: Path to the disk device.
        full: Whether to fetch full SMART data.

    Returns:
        Dict[str, Any]: Disk information dictionary.
    """
    device_name = device_path.split('/')[-1]
    if device_path not in disk_info_cache or not disk_info_cache[device_path].get('capacity'):
        try:
            size, _ = run_cmd(monitor_config, monitor_logger, ["blockdev", "--getsize64", device_path])
            if size and size.isdigit():
                disk_info_cache[device_path] = {"capacity": size}
            else:
                log(monitor_config, monitor_logger, f"Invalid blockdev output for {device_path}: {size}", "ERROR")
                disk_info_cache[device_path] = {"capacity": "0"}
        except RuntimeError as e:
            log(monitor_config, monitor_logger, f"Error getting capacity for {device_path}: {e}", "ERROR")
            disk_info_cache[device_path] = {"capacity": "0"}
    size = disk_info_cache[device_path]["capacity"]
    try:
        capacity = int(size) // 1024 // 1024 // 1024 if size and size.isdigit() else 0
    except ValueError:
        log(monitor_config, monitor_logger, f"Invalid capacity for {device_path}: {size}", "ERROR")
        capacity = 0
    if not full:
        return {
            "name": device_name,
            "capacity": capacity,
            "availability": "online",
            "command_line_parse_error": False,
            "device_open_failed": False,
            "smart_command_failed": False,
            "disk_failing": False,
            "prefail_attributes_threshold": False,
            "past_attributes_threshold": False,
            "device_error_log_errors": False,
            "self_test_log_errors": False
        }
    if device_path == "/dev/md1":
        return {
            "name": device_name,
            "capacity": capacity,
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
            "self_test_log_errors": False
        }
    device_types = ["nvme", "auto"] if "nvme" in device_path else ["sat", "scsi", "auto"]
    for device_type in device_types:
        try:
            cmd = ["smartctl", "-a", "-d", device_type, device_path]
            smart_output, exit_code = run_cmd(monitor_config, monitor_logger, cmd)
            if not smart_output:
                log(monitor_config, monitor_logger, f"No SMART output for {device_path} with -d {device_type}", "ERROR")
                continue
            disk_smart_info = parse_smart_output(monitor_config, monitor_logger, smart_output, device_path)
            disk_smart_info["name"] = device_name
            disk_smart_info["capacity"] = capacity
            exit_flags = parse_smartctl_exit_code(exit_code)
            disk_smart_info.update(exit_flags)
            if exit_flags["command_line_parse_error"] or exit_flags["device_open_failed"] or exit_flags["smart_command_failed"]:
                disk_smart_info["availability"] = "offline"
            log(monitor_config, monitor_logger, f"{device_path} availability: {disk_smart_info['availability']}", "DEBUG")
            return disk_smart_info
        except RuntimeError as e:
            log(monitor_config, monitor_logger, f"Error getting SMART data for {device_path} with -d {device_type}: {e}", "ERROR")
            continue
    log(monitor_config, monitor_logger, f"Failed to get SMART data for {device_path} after all attempts", "ERROR")
    return {
        "name": device_name,
        "capacity": capacity,
        "availability": "offline",
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
        "self_test_log_errors": False
    }

def get_usage(monitor_config: MonitorConfig, monitor_logger: logging.Logger, mount_point: str) -> Tuple[int, str]:
    """Get disk usage for a mount point.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        mount_point: Mount point path.

    Returns:
        Tuple[int, str]: Percentage used and free space.
    """
    try:
        command_output, _ = run_cmd(monitor_config, monitor_logger, ["df", "-h", mount_point])
        if command_output:
            for line in command_output.splitlines()[1:]:
                parts = line.split()
                if len(parts) >= 6 and parts[-1] == mount_point:
                    percent = int(parts[4].rstrip("%"))
                    free_space = parts[3]
                    log(monitor_config, monitor_logger, f"df output for {mount_point}: {percent}% used, {free_space} free", "DEBUG")
                    return percent, free_space
        log(monitor_config, monitor_logger, f"No matching df output for {mount_point}, falling back to psutil", "DEBUG")
        usage = psutil.disk_usage(mount_point)
        percent = int(usage.percent)
        free_space = f"{usage.free // 1024 // 1024 // 1024}G"
        return percent, free_space
    except (subprocess.SubprocessError, psutil.Error, OSError) as e:
        log(monitor_config, monitor_logger, f"Error getting usage for {mount_point}: {e}", "ERROR")
        return 0, "N/A"

def check_package_version(monitor_config: MonitorConfig, monitor_logger: logging.Logger) -> bool:
    """Check and update package version if needed.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.

    Returns:
        bool: True if update performed, False otherwise.
    """
    try:
        if monitor_config.package_version == "latest":
            response = requests.get(
                "https://api.github.com/repos/joaofolino/mqtt-monitor/releases/latest", timeout=10)
            version = response.json()['tag_name'].lstrip('v')
        else:
            version = monitor_config.package_version
        current_version, _ = run_cmd(monitor_config, monitor_logger, [
            "dpkg-query", "--showformat=${Version}", "--show", "mqtt-monitor"])
        current_version = current_version.strip()
        if current_version != version:
            log(monitor_config, monitor_logger, f"Updating package from {current_version} to {version}", "INFO")
            deb_url = (
                f"https://github.com/joaofolino/mqtt-monitor/releases/download/"
                f"v{version}/mqtt-monitor_{version}.deb")
            subprocess.run(["wget", "-O", "/tmp/mqtt-monitor.deb", deb_url], check=True)
            subprocess.run(["apt", "install", "-y", "/tmp/mqtt-monitor.deb"], check=True)
            log(monitor_config, monitor_logger, f"Updated to version {version}", "INFO")
            return True
    except (requests.RequestException, subprocess.SubprocessError, KeyError) as e:
        log(monitor_config, monitor_logger, f"Failed to check/update package version: {e}", "ERROR")
    return False

def setup_sensors(monitor_config: MonitorConfig, mqtt_client: mqtt.Client) -> None:
    """Set up Home Assistant sensor configurations.

    Args:
        monitor_config: Configuration object.
        mqtt_client: MQTT client instance.
    """
    messages = []
    for sensor, unit in HOST_SENSORS:
        messages.append({
            "topic": f"homeassistant/sensor/server_{sensor}/config",
            "payload": json.dumps({
                "name": f"Server {sensor.replace('_', ' ').title()}",
                "state_topic": f"{monitor_config.topic_prefix}/host/{sensor}",
                "unique_id": f"server_{sensor}",
                "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
                "unit_of_measurement": unit
            }),
            "retain": True
        })
    for mount_point in monitor_config.disk_usage_mounts:
        mount_name = mount_point.strip('/').replace('/', '_') or 'root'
        messages.append({
            "topic": f"homeassistant/sensor/server_{mount_name}_disk_usage_percentage/config",
            "payload": json.dumps({
                "name": f"Server {mount_name.replace('_', ' ').title()} Disk Usage Percentage",
                "state_topic": f"{monitor_config.topic_prefix}/host/{mount_name}_disk_usage_percentage",
                "unique_id": f"server_{mount_name}_disk_usage_percentage",
                "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
                "unit_of_measurement": "%"
            }),
            "retain": True
        })
    for device_path in monitor_config.devices:
        device_name = device_path.split('/')[-1]
        for attr, unit in DISK_SENSOR_ATTRIBUTES:
            config = {
                "name": f"Disk {device_name} {attr.replace('_', ' ').title()}",
                "state_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/{attr}",
                "unique_id": f"disk_{device_name}_{attr}",
                "device": {"identifiers": ["server_monitor"]},
                "availability_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/availability",
                "payload_available": "online",
                "payload_not_available": "offline"
            }
            if unit is not None:
                config["unit_of_measurement"] = unit
            messages.append({
                "topic": f"homeassistant/sensor/disk_{device_name}_{attr}/config",
                "payload": json.dumps(config),
                "retain": True
            })
        if monitor_config.enable_io_mqtt:
            for attr, unit in DISK_IO_ATTRIBUTES:
                messages.append({
                    "topic": f"homeassistant/sensor/disk_{device_name}_{attr}/config",
                    "payload": json.dumps({
                        "name": f"Disk {device_name} {attr.replace('_', ' ').title()}",
                        "state_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/{attr}",
                        "unique_id": f"disk_{device_name}_{attr}",
                        "device": {"identifiers": ["server_monitor"]},
                        "unit_of_measurement": unit,
                        "availability_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/availability",
                        "payload_available": "online",
                        "payload_not_available": "offline"
                    }),
                    "retain": True
                })
        for flag in DISK_FLAGS:
            messages.append({
                "topic": f"homeassistant/binary_sensor/disk_{device_name}_{flag}/config",
                "payload": json.dumps({
                    "name": f"Disk {device_name} {flag.replace('_', ' ').title()}",
                    "state_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/{flag}",
                    "unique_id": f"disk_{device_name}_{flag}",
                    "device": {"identifiers": ["server_monitor"]},
                    "payload_on": "true",
                    "payload_off": "false",
                    "availability_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/availability",
                    "payload_available": "online",
                    "payload_not_available": "offline"
                }),
                "retain": True
            })
    for raid_array in monitor_config.raid_arrays:
        messages.append({
            "topic": f"homeassistant/sensor/raid_{raid_array}_status/config",
            "payload": json.dumps({
                "name": f"RAID {raid_array} Status",
                "state_topic": f"{monitor_config.topic_prefix}/raid/{raid_array}/status",
                "unique_id": f"raid_{raid_array}_status",
                "device": {"identifiers": ["server_monitor"]}
            }),
            "retain": True
        })
    mqtt_client.publish_multiple(messages)

def publish_data(monitor_config: MonitorConfig, monitor_logger: logging.Logger,
                 mqtt_client: mqtt.Client, messages: List[Dict[str, Any]]) -> None:
    """Publish multiple data points to MQTT topics.

    Args:
        monitor_config: Configuration object.
        monitor_logger: Logger instance.
        mqtt_client: MQTT client instance.
        messages: List of dictionaries with topic, payload, and retain flag.
    """
    try:
        valid_messages = [
            msg for msg in messages if msg["payload"] is not None
        ]
        if valid_messages:
            log(monitor_config, monitor_logger,
                f"MQTT publish: {[(m['topic'], m['payload']) for m in valid_messages]}", "DEBUG")
            mqtt_client.publish_multiple(valid_messages)
    except ConnectionError as e:
        log(monitor_config, monitor_logger, f"MQTT publish error: {e}", "ERROR")
        connect_mqtt(monitor_config, monitor_logger, mqtt_client)

def main() -> None:
    """Run the main monitoring loop."""
    monitor_config = load_config(os.getenv('CONFIG_PATH', '/etc/mqtt-monitor'))
    monitor_logger = setup_logging(monitor_config)
    monitor_state = MonitorState()
    mqtt_client = mqtt.Client()

    def handle_sighup(signum: int, frame: Any) -> None:
        """Reload configuration on SIGHUP."""
        nonlocal monitor_config
        try:
            monitor_config = load_config(os.getenv('CONFIG_PATH', '/etc/mqtt-monitor'))
            log(monitor_config, monitor_logger, "Configuration reloaded", "INFO")
            setup_sensors(monitor_config, mqtt_client)  # Reconfigure sensors
        except (FileNotFoundError, ValueError) as e:
            log(monitor_config, monitor_logger, f"Failed to reload configuration: {e}", "ERROR")

    def handle_sigterm(signum: int, frame: Any) -> None:
        """Handle graceful shutdown on SIGTERM."""
        log(monitor_config, monitor_logger, "Received SIGTERM, shutting down", "INFO")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGHUP, handle_sighup)
    signal.signal(signal.SIGTERM, handle_sigterm)

    if not connect_mqtt(monitor_config, monitor_logger, mqtt_client):
        log(monitor_config, monitor_logger, "Initial MQTT connection failed, exiting", "ERROR")
        sys.exit(1)

    if check_package_version(monitor_config, monitor_logger):
        return

    setup_sensors(monitor_config, mqtt_client)
    device_names = [d.split('/')[-1] for d in monitor_config.devices]
    for device_name in device_names:
        monitor_state.previous_io_stats[device_name] = {
            "read_count": 0,
            "write_count": 0,
            "read_bytes": 0,
            "write_bytes": 0,
            "read_time": 0,
            "write_time": 0,
            "busy_time": 0,
            "read_merged_count": 0,
            "write_merged_count": 0
        }
    last_smart_check_time = 0
    last_log_dir_check = 0
    while True:
        try:
            current_loop_time = time.time()
            log(monitor_config, monitor_logger, f"Time counting: current_loop_time={current_loop_time}, "
                                                f"previous_loop_time={monitor_state.previous_loop_time}", "DEBUG")
            loop_time_delta = 60.0 if monitor_state.previous_loop_time is None else current_loop_time - monitor_state.previous_loop_time
            monitor_state.previous_loop_time = current_loop_time
            log(monitor_config, monitor_logger, f"Loop time delta: {loop_time_delta:.2f} seconds", "DEBUG")

            # Periodic log directory check
            if current_loop_time - last_log_dir_check >= 300:  # Check every 5 minutes
                try:
                    usage = psutil.disk_usage(os.path.abspath(monitor_config.log_dir))
                    if usage.percent > 95:
                        send_alert(monitor_config, monitor_logger, "Log Directory Full",
                                   f"Log directory {monitor_config.log_dir} is at {usage.percent}% capacity")
                    last_log_dir_check = current_loop_time
                except (psutil.Error, OSError) as e:
                    log(monitor_config, monitor_logger, f"Error checking log directory usage: {e}", "ERROR")

            # Host metrics
            messages = [
                {
                    "topic": f"{monitor_config.topic_prefix}/host/cpu_load_percentage",
                    "payload": str(psutil.cpu_percent()),
                    "retain": False
                },
                {
                    "topic": f"{monitor_config.topic_prefix}/host/memory_used_megabytes",
                    "payload": str(psutil.virtual_memory().used // 1024 // 1024),
                    "retain": False
                }
            ]
            try:
                with open(monitor_config.cpu_temp_path, "r", encoding="utf-8") as f:
                    cpu_temperature = int(f.read().strip()) / 1000
                messages.append({
                    "topic": f"{monitor_config.topic_prefix}/host/cpu_temperature_celsius",
                    "payload": str(cpu_temperature),
                    "retain": False
                })
            except (FileNotFoundError, IOError, ValueError) as e:
                log(monitor_config, monitor_logger, f"Error reading CPU temperature: {e}", "ERROR")

            # Disk I/O
            current_io_stats: Dict[str, Dict[str, int]] = {}
            for device_name in device_names:
                current_io_stats[device_name] = get_disk_io(monitor_config, monitor_logger, device_name)
                previous_io_stats = monitor_state.previous_io_stats.get(device_name, {
                    "read_count": 0,
                    "write_count": 0,
                    "read_bytes": 0,
                    "write_bytes": 0,
                    "read_time": 0,
                    "write_time": 0,
                    "busy_time": 0,
                    "read_merged_count": 0,
                    "write_merged_count": 0
                })
                read_bytes_delta = current_io_stats[device_name]["read_bytes"] - previous_io_stats["read_bytes"]
                write_bytes_delta = current_io_stats[device_name]["write_bytes"] - previous_io_stats["write_bytes"]
                read_count_delta = current_io_stats[device_name]["read_count"] - previous_io_stats["read_count"]
                write_count_delta = current_io_stats[device_name]["write_count"] - previous_io_stats["write_count"]
                busy_time_ms = current_io_stats[device_name]["busy_time"]
                if loop_time_delta > 0:
                    busy_time_seconds = busy_time_ms / 1000
                    busy_time_factor = min(busy_time_seconds / loop_time_delta, 1.0) if busy_time_ms else 1.0
                    read_kilobytes_per_second = (read_bytes_delta / 1024 / loop_time_delta) * busy_time_factor
                    write_kilobytes_per_second = (write_bytes_delta / 1024 / loop_time_delta) * busy_time_factor
                    read_count_per_second = read_count_delta / loop_time_delta
                    write_count_per_second = write_count_delta / loop_time_delta
                    busy_time_percentage = (busy_time_seconds / loop_time_delta) * 100 if busy_time_ms else 0.0
                    # Warn on unrealistic rates (>1GB/s)
                    if read_kilobytes_per_second > 1024 * 1024 or write_kilobytes_per_second > 1024 * 1024:
                        log(monitor_config, monitor_logger, f"Unrealistic I/O rate for {device_name}: "
                                                            f"read={read_kilobytes_per_second:.2f} KB/s, "
                                                            f"write={write_kilobytes_per_second:.2f} KB/s", "WARNING")
                else:
                    log(monitor_config, monitor_logger, "Invalid loop_time_delta, skipping I/O rate calculations", "WARNING")
                    read_kilobytes_per_second = write_kilobytes_per_second = read_count_per_second = write_count_per_second = busy_time_percentage = 0.0
                log(monitor_config, monitor_logger, f"{device_name} I/O: "
                                                    f"read_kilobytes_per_second={read_kilobytes_per_second:.2f} KB/s, "
                                                    f"write_kilobytes_per_second={write_kilobytes_per_second:.2f} KB/s, "
                                                    f"read_count_per_second={read_count_per_second:.2f} ops/s, "
                                                    f"write_count_per_second={write_count_per_second:.2f} ops/s, "
                                                    f"busy_time_percentage={busy_time_percentage:.2f}%", "INFO")
                if monitor_config.enable_io_mqtt:
                    messages.extend([
                        {
                            "topic": f"{monitor_config.topic_prefix}/disk/{device_name}/read_kilobytes_per_second",
                            "payload": f"{read_kilobytes_per_second:.2f}",
                            "retain": False
                        },
                        {
                            "topic": f"{monitor_config.topic_prefix}/disk/{device_name}/write_kilobytes_per_second",
                            "payload": f"{write_kilobytes_per_second:.2f}",
                            "retain": False
                        },
                        {
                            "topic": f"{monitor_config.topic_prefix}/disk/{device_name}/read_count_per_second",
                            "payload": f"{read_count_per_second:.2f}",
                            "retain": False
                        },
                        {
                            "topic": f"{monitor_config.topic_prefix}/disk/{device_name}/write_count_per_second",
                            "payload": f"{write_count_per_second:.2f}",
                            "retain": False
                        },
                        {
                            "topic": f"{monitor_config.topic_prefix}/disk/{device_name}/busy_time_percentage",
                            "payload": f"{busy_time_percentage:.2f}",
                            "retain": False
                        }
                    ])
                if device_name == "md1" and write_kilobytes_per_second > monitor_config.md1_write_spike_threshold:
                    send_alert(monitor_config, monitor_logger, "md1 Write Spike",
                               f"Write rate: {write_kilobytes_per_second:.2f} KB/s")
                monitor_state.previous_io_stats[device_name] = current_io_stats[device_name]

            # RAID status
            raid_status = get_raid_status(monitor_config, monitor_logger)
            for raid_array, status in raid_status.items():
                if raid_array not in monitor_config.raid_arrays:
                    monitor_config.raid_arrays.append(raid_array)
                    setup_sensors(monitor_config, mqtt_client)
                messages.append({
                    "topic": f"{monitor_config.topic_prefix}/raid/{raid_array}/status",
                    "payload": status["status"],
                    "retain": True
                })
                if status["status"] == "degraded":
                    send_alert(monitor_config, monitor_logger, f"RAID {raid_array} Degraded",
                               f"Status: {status['raw']}")

            # Disk usage
            for mount_point in monitor_config.disk_usage_mounts:
                usage_percentage, free_space = get_usage(monitor_config, monitor_logger, mount_point)
                mount_name = mount_point.strip('/').replace('/', '_') or 'root'
                messages.append({
                    "topic": f"{monitor_config.topic_prefix}/host/{mount_name}_disk_usage_percentage",
                    "payload": str(usage_percentage),
                    "retain": False
                })
                log(monitor_config, monitor_logger, f"{mount_point}: {usage_percentage}% used, {free_space} free", "INFO")
                if usage_percentage > monitor_config.md1_usage_threshold:
                    send_alert(monitor_config, monitor_logger, f"{mount_name.replace('_', ' ').title()} Capacity Critical",
                               f"{mount_point} at {usage_percentage}%, only {free_space} free")

            # Disk SMART data
            if current_loop_time - last_smart_check_time >= monitor_config.smart_interval:
                for device_path in monitor_config.devices:
                    disk_info_cache[device_path] = get_disk_info(monitor_config, monitor_logger, device_path, full=True)
                last_smart_check_time = current_loop_time
            else:
                for device_path in monitor_config.devices:
                    if disk_info_cache.get(device_path, {}).get("availability") != "online":
                        disk_info_cache[device_path] = get_disk_info(monitor_config, monitor_logger, device_path, full=False)
            for device_path in monitor_config.devices:
                device_name = device_path.split('/')[-1]
                disk_info = disk_info_cache[device_path]
                messages.append({
                    "topic": f"{monitor_config.topic_prefix}/disk/{device_name}/availability",
                    "payload": disk_info.get("availability"),
                    "retain": True
                })
                for key in ["max_temperature", "reallocated_sectors", "spin_up_count", "smart_health_status"]:
                    messages.append({
                        "topic": f"{monitor_config.topic_prefix}/disk/{device_name}/{key}",
                        "payload": disk_info.get(key),
                        "retain": key == "smart_health_status"  # Retain for critical state
                    })
                for flag in DISK_FLAGS:
                    messages.append({
                        "topic": f"{monitor_config.topic_prefix}/disk/{device_name}/{flag}",
                        "payload": str(disk_info.get(flag, False)).lower(),
                        "retain": True
                    })
                log(monitor_config, monitor_logger, f"{device_path}: "
                                                    f"smart_health_status={disk_info.get('smart_health_status', 'N/A')}, "
                                                    f"max_temperature={disk_info.get('max_temperature', 'N/A')}°C, "
                                                    f"availability={disk_info.get('availability')}", "INFO")

            publish_data(monitor_config, monitor_logger, mqtt_client, messages)
            time.sleep(monitor_config.sleep_interval)
        except (ConnectionError, IOError, RuntimeError) as e:
            log(monitor_config, monitor_logger, f"Main loop error: {e}", "ERROR")
            time.sleep(monitor_config.error_sleep)

if __name__ == "__main__":
    main()
