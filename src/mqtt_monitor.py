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

def load_config(config_path='/etc/mqtt-monitor'):
    """Load configuration from a file."""
    config_parser = configparser.ConfigParser()
    config_file_path = f"{config_path}/mqtt-monitor.conf"
    print(f"Loading configuration from: {config_file_path}")
    if not config_parser.read(config_file_path):
        raise FileNotFoundError(f"Configuration file {config_file_path} not found or unreadable")
    try:
        return MonitorConfig(
            enable_email=config_parser.getboolean('monitor', 'enable_email'),
            log_level=config_parser['monitor']['log_level'],
            enable_io_mqtt=config_parser.getboolean('monitor', 'enable_io_mqtt'),
            alert_email=config_parser['monitor']['alert_email'],
            smtp_server=config_parser['monitor']['smtp_server'],
            smtp_port=config_parser.getint('monitor', 'smtp_port'),
            smtp_user=config_parser['monitor']['smtp_user'],
            smtp_pass=config_parser['monitor']['smtp_pass'],
            package_version=config_parser['monitor']['package_version'],
            broker=config_parser['mqtt']['broker'],
            port=config_parser.getint('mqtt', 'port'),
            mqtt_user=config_parser['mqtt']['mqtt_user'],
            mqtt_pass=config_parser['mqtt']['mqtt_pass'],
            self_name=config_parser['mqtt']['self_name'],
            smart_interval=config_parser.getint('internals', 'smart_interval'),
            topic_prefix=config_parser['internals']['topic_prefix'],
            log_dir=config_parser['internals']['log_dir'],
            log_file=config_parser['internals']['log_file'],
            devices=config_parser['internals']['devices'].split(','),
            raid_arrays=config_parser['internals']['raid_arrays'].split(','),
            mdstat_path=config_parser['internals']['mdstat_path'],
            cpu_temp_path=config_parser['internals']['cpu_temp_path'],
            disk_usage_mounts=config_parser['internals']['disk_usage_mounts'].split(','),
            max_io_rate=config_parser.getfloat('internals', 'max_io_rate'),
            md1_usage_threshold=config_parser.getfloat('internals', 'md1_usage_threshold'),
            md1_write_spike_threshold=config_parser.getfloat('internals', 'md1_write_spike_threshold'),
            sleep_interval=config_parser.getfloat('internals', 'sleep_interval'),
            error_sleep=config_parser.getfloat('internals', 'error_sleep')
        )
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        print(f"Configuration error: {e}")
        raise

disk_info_cache = {}

class MonitorState:
    """Holds state for monitoring loop."""
    def __init__(self):
        self.previous_io_stats = {}
        self.previous_loop_time = None

def setup_logging(monitor_config):
    """Set up logging to file and console."""
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

def log(monitor_config, monitor_logger, message, level="INFO"):
    """Log a message at the specified level."""
    try:
        log_level = getattr(logging, level.upper(), logging.INFO)
        if monitor_logger.isEnabledFor(log_level):
            getattr(monitor_logger, level.lower())(message)
        usage = psutil.disk_usage(os.path.abspath(monitor_config.log_dir))
        if usage.percent > 95:
            send_alert(monitor_config, monitor_logger, "Log Directory Full",
                       f"Log directory {monitor_config.log_dir} is at {usage.percent}% capacity")
    except RuntimeError as e:
        print(f"Logging error: {e}")

def on_connect(monitor_config, monitor_logger, mqtt_client, userdata, flags, rc):  # pylint: disable=unused-argument
    """Handle MQTT connection event."""
    if rc == 0:
        log(monitor_config, monitor_logger, "Connected to MQTT broker", "INFO")
    else:
        log(monitor_config, monitor_logger, f"Failed to connect to MQTT broker, code: {rc}", "ERROR")

def connect_mqtt(monitor_config, monitor_logger, mqtt_client):
    """Connect to MQTT broker."""
    try:
        mqtt_client.username_pw_set(monitor_config.mqtt_user, monitor_config.mqtt_pass)
        mqtt_client.on_connect = lambda c, u, f, r: on_connect(monitor_config, monitor_logger, c, u, f, r)
        mqtt_client.connect(monitor_config.broker, monitor_config.port, 60)
        mqtt_client.loop_start()
    except ConnectionError as e:
        log(monitor_config, monitor_logger, f"Failed to connect to MQTT broker: {e}", "ERROR")

def send_alert(monitor_config, monitor_logger, subject, body):
    """Send an email alert."""
    if not monitor_config.enable_email:
        log(monitor_config, monitor_logger, f"Email alerts disabled: {subject}", "INFO")
        return
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = monitor_config.smtp_user
        msg['To'] = monitor_config.alert_email
        with smtplib.SMTP(monitor_config.smtp_server, monitor_config.smtp_port) as server:
            server.starttls()
            server.login(monitor_config.smtp_user, monitor_config.smtp_pass)
            server.sendmail(monitor_config.smtp_user, monitor_config.alert_email, msg.as_string())
        log(monitor_config, monitor_logger, f"Sent alert: {subject}", "INFO")
    except smtplib.SMTPException as e:
        log(monitor_config, monitor_logger, f"Failed to send alert {subject}: {e}", "ERROR")

def run_cmd(monitor_config, monitor_logger, command):
    """Run a shell command and return its output and exit code."""
    try:
        log(monitor_config, monitor_logger, f"Executing command: {' '.join(command)}", "DEBUG")
        command_result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=10)
        log(monitor_config, monitor_logger, f"Command {' '.join(command)} output: {command_result.stdout[:100]}... (exit_code: {command_result.returncode})", "DEBUG")
        return command_result.stdout.strip(), command_result.returncode
    except (subprocess.SubprocessError, subprocess.TimeoutExpired) as e:
        log(monitor_config, monitor_logger, f"Command {' '.join(command)} error: {e} (stderr: {e.stderr if hasattr(e, 'stderr') else 'N/A'})", "ERROR")
        return "", -1

def parse_smartctl_exit_code(exit_code):
    """Parse smartctl exit code into boolean flags using bit shifts."""
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

def get_raid_status(monitor_config, monitor_logger):
    """Get RAID array status from mdstat."""
    try:
        with open(monitor_config.mdstat_path, "r", encoding="utf-8") as f:
            mdstat = f.read()
        raid_arrays = {}
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

def get_disk_io(monitor_config, monitor_logger, device_name):
    """Get disk I/O statistics."""
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
    except Exception as e:  # pylint: disable=broad-except
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

def parse_smart_output(monitor_config, monitor_logger, smart_output, device_path):
    """Parse SMART output into a dictionary."""
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
    if smart_health_status in ["FAILED"]:
        send_alert(monitor_config, monitor_logger, f"Disk {device_path} SMART Failure",
                   f"SMART status: {smart_health_status}, Output: {smart_output[:200]}")
    return smart_attributes

def get_disk_info(monitor_config, monitor_logger, device_path, full=False):
    """Get disk information (capacity, SMART attributes)."""
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
    # Try multiple device types for SMART data
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

def get_usage(monitor_config, monitor_logger, mount_point):
    """Get disk usage for a mount point."""
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
    except Exception as e:  # pylint: disable=broad-except
        log(monitor_config, monitor_logger, f"Error getting usage for {mount_point}: {e}", "ERROR")
        return 0, "N/A"

def check_package_version(monitor_config, monitor_logger):
    """Check and update package version if needed."""
    try:
        if monitor_config.package_version == "latest":
            response = requests.get(  # pylint: disable=missing-timeout
                "https://api.github.com/repos/joaofolino/mqtt-monitor/releases/latest")
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
    except Exception as e:  # pylint: disable=broad-except
        log(monitor_config, monitor_logger, f"Failed to check/update package version: {e}", "ERROR")
    return False

def setup_sensors(monitor_config, mqtt_client):
    """Set up Home Assistant sensor configurations."""
    # Host sensors
    for sensor, unit in [
        ("cpu_load_percentage", "%"),
        ("memory_used_megabytes", "MB"),
        ("cpu_temperature_celsius", "°C")
    ]:
        mqtt_client.publish(f"homeassistant/sensor/server_{sensor}/config", json.dumps({
            "name": f"Server {sensor.replace('_', ' ').title()}",
            "state_topic": f"{monitor_config.topic_prefix}/host/{sensor}",
            "unique_id": f"server_{sensor}",
            "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
            "unit_of_measurement": unit
        }), retain=True)
    # Disk usage sensors for mount points
    for mount_point in monitor_config.disk_usage_mounts:
        mount_name = mount_point.strip('/').replace('/', '_') or 'root'
        mqtt_client.publish(f"homeassistant/sensor/server_{mount_name}_disk_usage_percentage/config", json.dumps({
            "name": f"Server {mount_name.replace('_', ' ').title()} Disk Usage Percentage",
            "state_topic": f"{monitor_config.topic_prefix}/host/{mount_name}_disk_usage_percentage",
            "unique_id": f"server_{mount_name}_disk_usage_percentage",
            "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
            "unit_of_measurement": "%"
        }), retain=True)
    # Disk sensors
    for device_path in monitor_config.devices:
        device_name = device_path.split('/')[-1]
        for attr, unit in [
            ("max_temperature", "°C"),
            ("reallocated_sectors", ""),
            ("spin_up_count", ""),
            ("smart_health_status", None)
        ]:
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
            mqtt_client.publish(f"homeassistant/sensor/disk_{device_name}_{attr}/config", json.dumps(config), retain=True)
        if monitor_config.enable_io_mqtt:
            for attr, unit in [
                ("read_kilobytes_per_second", "KB/s"),
                ("write_kilobytes_per_second", "KB/s"),
                ("read_count_per_second", "ops/s"),
                ("write_count_per_second", "ops/s"),
                ("busy_time_percentage", "%")
            ]:
                mqtt_client.publish(f"homeassistant/sensor/disk_{device_name}_{attr}/config", json.dumps({
                    "name": f"Disk {device_name} {attr.replace('_', ' ').title()}",
                    "state_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/{attr}",
                    "unique_id": f"disk_{device_name}_{attr}",
                    "device": {"identifiers": ["server_monitor"]},
                    "unit_of_measurement": unit,
                    "availability_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/availability",
                    "payload_available": "online",
                    "payload_not_available": "offline"
                }), retain=True)
        for flag in [
            "command_line_parse_error",
            "device_open_failed",
            "smart_command_failed",
            "disk_failing",
            "prefail_attributes_threshold",
            "past_attributes_threshold",
            "device_error_log_errors",
            "self_test_log_errors"
        ]:
            mqtt_client.publish(f"homeassistant/binary_sensor/disk_{device_name}_{flag}/config", json.dumps({
                "name": f"Disk {device_name} {flag.replace('_', ' ').title()}",
                "state_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/{flag}",
                "unique_id": f"disk_{device_name}_{flag}",
                "device": {"identifiers": ["server_monitor"]},
                "payload_on": "true",
                "payload_off": "false",
                "availability_topic": f"{monitor_config.topic_prefix}/disk/{device_name}/availability",
                "payload_available": "online",
                "payload_not_available": "offline"
            }), retain=True)
    # RAID sensors
    for raid_array in monitor_config.raid_arrays:
        mqtt_client.publish(f"homeassistant/sensor/raid_{raid_array}_status/config", json.dumps({
            "name": f"RAID {raid_array} Status",
            "state_topic": f"{monitor_config.topic_prefix}/raid/{raid_array}/status",
            "unique_id": f"raid_{raid_array}_status",
            "device": {"identifiers": ["server_monitor"]}
        }), retain=True)

def publish_data(monitor_config, monitor_logger, mqtt_client, topic, value, retain=False):
    """Publish data to MQTT topic."""
    try:
        if value is None:
            log(monitor_config, monitor_logger, f"Skipping MQTT publish for {topic}: value is None", "DEBUG")
            return  # Skip publishing if value is None
        # Convert numeric values and booleans to string
        if isinstance(value, (int, float, bool)):
            value = str(value).lower()
        log(monitor_config, monitor_logger, f"MQTT publish: {topic} = {value}", "DEBUG")
        mqtt_client.publish(topic, value, retain=retain)
    except ConnectionError as e:
        log(monitor_config, monitor_logger, f"MQTT publish error {topic}: {e}", "ERROR")
        connect_mqtt(monitor_config, monitor_logger, mqtt_client)

def main():
    """Run the main monitoring loop."""
    monitor_config = load_config(os.getenv('CONFIG_PATH', '/etc/mqtt-monitor'))
    monitor_logger = setup_logging(monitor_config)
    monitor_state = MonitorState()
    mqtt_client = mqtt.Client()
    connect_mqtt(monitor_config, monitor_logger, mqtt_client)
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
    while True:
        try:
            current_loop_time = time.time()
            log(monitor_config, monitor_logger, f"Time counting: current_loop_time={current_loop_time}, "
                                                f"previous_loop_time={monitor_state.previous_loop_time}", "DEBUG")
            loop_time_delta = 60.0 if monitor_state.previous_loop_time is None else current_loop_time - monitor_state.previous_loop_time
            monitor_state.previous_loop_time = current_loop_time
            log(monitor_config, monitor_logger, f"Loop time delta: {loop_time_delta:.2f} seconds", "DEBUG")
            # Host metrics
            publish_data(monitor_config, monitor_logger, mqtt_client,
                         f"{monitor_config.topic_prefix}/host/cpu_load_percentage", psutil.cpu_percent())
            publish_data(monitor_config, monitor_logger, mqtt_client,
                         f"{monitor_config.topic_prefix}/host/memory_used_megabytes",
                         psutil.virtual_memory().used // 1024 // 1024)
            with open(monitor_config.cpu_temp_path, "r", encoding="utf-8") as f:
                cpu_temperature = int(f.read().strip()) / 1000
            publish_data(monitor_config, monitor_logger, mqtt_client,
                         f"{monitor_config.topic_prefix}/host/cpu_temperature_celsius", cpu_temperature)
            # Disk I/O
            current_io_stats = {}
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
                if loop_time_delta:
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
                    read_kilobytes_per_second = 0.0
                    write_kilobytes_per_second = 0.0
                    read_count_per_second = 0.0
                    write_count_per_second = 0.0
                    busy_time_percentage = 0.0
                log(monitor_config, monitor_logger, f"{device_name} I/O: "
                                                    f"read_kilobytes_per_second={read_kilobytes_per_second:.2f} KB/s, "
                                                    f"write_kilobytes_per_second={write_kilobytes_per_second:.2f} KB/s, "
                                                    f"read_count_per_second={read_count_per_second:.2f} ops/s, "
                                                    f"write_count_per_second={write_count_per_second:.2f} ops/s, "
                                                    f"busy_time_percentage={busy_time_percentage:.2f}%", "INFO")
                if monitor_config.enable_io_mqtt:
                    publish_data(monitor_config, monitor_logger, mqtt_client,
                                 f"{monitor_config.topic_prefix}/disk/{device_name}/read_kilobytes_per_second",
                                 f"{read_kilobytes_per_second:.2f}")
                    publish_data(monitor_config, monitor_logger, mqtt_client,
                                 f"{monitor_config.topic_prefix}/disk/{device_name}/write_kilobytes_per_second",
                                 f"{write_kilobytes_per_second:.2f}")
                    publish_data(monitor_config, monitor_logger, mqtt_client,
                                 f"{monitor_config.topic_prefix}/disk/{device_name}/read_count_per_second",
                                 f"{read_count_per_second:.2f}")
                    publish_data(monitor_config, monitor_logger, mqtt_client,
                                 f"{monitor_config.topic_prefix}/disk/{device_name}/write_count_per_second",
                                 f"{write_count_per_second:.2f}")
                    publish_data(monitor_config, monitor_logger, mqtt_client,
                                 f"{monitor_config.topic_prefix}/disk/{device_name}/busy_time_percentage",
                                 f"{busy_time_percentage:.2f}")
                if device_name == "md1" and write_kilobytes_per_second > monitor_config.md1_write_spike_threshold:
                    send_alert(monitor_config, monitor_logger, "md1 Write Spike",
                               f"Write rate: {write_kilobytes_per_second:.2f} KB/s")
                monitor_state.previous_io_stats[device_name] = current_io_stats[device_name]
            # RAID status
            raid_status = get_raid_status(monitor_config, monitor_logger)
            for raid_array, status in raid_status.items():
                if raid_array not in monitor_config.raid_arrays:
                    monitor_config.raid_arrays.append(raid_array)
                    publish_data(monitor_config, monitor_logger, mqtt_client,
                                 f"homeassistant/sensor/raid_{raid_array}_status/config", json.dumps({
                            "name": f"RAID {raid_array} Status",
                            "state_topic": f"{monitor_config.topic_prefix}/raid/{raid_array}/status",
                            "unique_id": f"raid_{raid_array}_status",
                            "device": {"identifiers": ["server_monitor"]}
                        }), retain=True)
                publish_data(monitor_config, monitor_logger, mqtt_client,
                             f"{monitor_config.topic_prefix}/raid/{raid_array}/status", status["status"])
                if status["status"] == "degraded":
                    send_alert(monitor_config, monitor_logger, f"RAID {raid_array} Degraded",
                               f"Status: {status['raw']}")
            # Disk usage
            for mount_point in monitor_config.disk_usage_mounts:
                usage_percentage, free_space = get_usage(monitor_config, monitor_logger, mount_point)
                mount_name = mount_point.strip('/').replace('/', '_') or 'root'
                publish_data(monitor_config, monitor_logger, mqtt_client,
                             f"{monitor_config.topic_prefix}/host/{mount_name}_disk_usage_percentage", usage_percentage)
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
                # Publish availability with retain
                publish_data(monitor_config, monitor_logger, mqtt_client,
                             f"{monitor_config.topic_prefix}/disk/{device_name}/availability",
                             disk_info.get("availability"), retain=True)
                for key in ["max_temperature", "reallocated_sectors", "spin_up_count", "smart_health_status"]:
                    publish_data(monitor_config, monitor_logger, mqtt_client,
                                 f"{monitor_config.topic_prefix}/disk/{device_name}/{key}",
                                 disk_info.get(key))
                for flag in [
                    "command_line_parse_error",
                    "device_open_failed",
                    "smart_command_failed",
                    "disk_failing",
                    "prefail_attributes_threshold",
                    "past_attributes_threshold",
                    "device_error_log_errors",
                    "self_test_log_errors"
                ]:
                    publish_data(monitor_config, monitor_logger, mqtt_client,
                                 f"{monitor_config.topic_prefix}/disk/{device_name}/{flag}",
                                 str(disk_info.get(flag, False)).lower())
                log(monitor_config, monitor_logger, f"{device_path}: "
                                                    f"smart_health_status={disk_info.get('smart_health_status', 'N/A')}, "
                                                    f"max_temperature={disk_info.get('max_temperature', 'N/A')}°C, "
                                                    f"availability={disk_info.get('availability')}", "INFO")
            time.sleep(monitor_config.sleep_interval)
        except (ConnectionError, IOError, RuntimeError) as e:
            log(monitor_config, monitor_logger, f"Main loop error: {e}", "ERROR")
            time.sleep(monitor_config.error_sleep)

if __name__ == "__main__":
    main()
