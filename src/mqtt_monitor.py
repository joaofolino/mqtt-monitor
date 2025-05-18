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

def load_config(config_path='/etc/mqtt-monitor'):
    """Load configuration from a file."""
    config = configparser.ConfigParser()
    config_file_path = f"{config_path}/mqtt-monitor.conf"
    print(f"Loading config from: {config_file_path}")
    if not config.read(config_file_path):
        raise FileNotFoundError(f"Config file {config_file_path} not found or unreadable")
    try:
        return Config(
            enable_email=config.getboolean('monitor', 'enable_email'),
            log_level=config['monitor']['log_level'],
            enable_io_mqtt=config.getboolean('monitor', 'enable_io_mqtt'),
            alert_email=config['monitor']['alert_email'],
            smtp_server=config['monitor']['smtp_server'],
            smtp_port=config.getint('monitor', 'smtp_port'),
            smtp_user=config['monitor']['smtp_user'],
            smtp_pass=config['monitor']['smtp_pass'],
            package_version=config['monitor']['package_version'],
            broker=config['mqtt']['broker'],
            port=config.getint('mqtt', 'port'),
            mqtt_user=config['mqtt']['mqtt_user'],
            mqtt_pass=config['mqtt']['mqtt_pass'],
            self_name=config['mqtt']['self_name'],
            smart_interval=config.getint('internals', 'smart_interval'),
            topic_prefix=config['internals']['topic_prefix'],
            log_dir=config['internals']['log_dir'],
            log_file=config['internals']['log_file'],
            devices=config['internals']['devices'].split(','),
            raid_arrays=config['internals']['raid_arrays'].split(','),
            mdstat_path=config['internals']['mdstat_path'],
            cpu_temp_path=config['internals']['cpu_temp_path'],
            disk_usage_mounts=config['internals']['disk_usage_mounts'].split(','),
            max_io_rate=config.getfloat('internals', 'max_io_rate'),
            md1_usage_threshold=config.getfloat('internals', 'md1_usage_threshold'),
            md1_write_spike_threshold=config.getfloat('internals', 'md1_write_spike_threshold'),
            sleep_interval=config.getfloat('internals', 'sleep_interval'),
            error_sleep=config.getfloat('internals', 'error_sleep')
        )
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        print(f"Config error: {e}")
        raise

disk_cache = {}

class MonitorState:
    """Holds state for monitoring loop."""
    def __init__(self):
        self.prev_io = {}
        self.prev_time = None

def setup_logging(config):
    """Set up logging to file and console."""
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    logger.handlers = []  # Clear existing handlers
    log_file_path = os.path.join(os.path.abspath(config.log_dir), config.log_file)
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    stream_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
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
            send_alert(config, logger, "Log Directory Full",
                       f"Log directory {config.log_dir} is at {usage.percent}% capacity")
    except RuntimeError as e:
        print(f"Logging error: {e}")

def on_connect(config, logger, client, userdata, flags, rc):  # pylint: disable=unused-argument
    """Handle MQTT connection event."""
    if rc == 0:
        log(config, logger, "Connected to MQTT broker", "INFO")
    else:
        log(config, logger, f"Failed to connect to MQTT broker, code: {rc}", "ERROR")

def connect_mqtt(config, logger, client):
    """Connect to MQTT broker."""
    try:
        client.username_pw_set(config.mqtt_user, config.mqtt_pass)
        client.on_connect = lambda c, u, f, r: on_connect(config, logger, c, u, f, r)
        client.connect(config.broker, config.port, 60)
        client.loop_start()
    except ConnectionError as e:
        log(config, logger, f"Failed to connect to MQTT broker: {e}", "ERROR")

def send_alert(config, logger, subject, body):
    """Send an email alert."""
    if not config.enable_email:
        log(config, logger, f"Email alerts disabled: {subject}", "INFO")
        return
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = config.smtp_user
        msg['To'] = config.alert_email
        with smtplib.SMTP(config.smtp_server, config.smtp_port) as server:
            server.starttls()
            server.login(config.smtp_user, config.smtp_pass)
            server.sendmail(config.smtp_user, config.alert_email, msg.as_string())
        log(config, logger, f"Sent alert: {subject}", "INFO")
    except smtplib.SMTPException as e:
        log(config, logger, f"Failed to send alert {subject}: {e}", "ERROR")

def run_cmd(config, logger, cmd):
    """Run a shell command and return its output and return code."""
    try:
        log(config, logger, f"Executing command: {' '.join(cmd)}", "DEBUG")
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=10)
        log(config, logger, f"Command {' '.join(cmd)} output: {result.stdout[:100]}... (returncode: {result.returncode})", "DEBUG")
        if result.returncode == 32 and "smartctl" in cmd[0]:
            log(config, logger, f"Some attributes  have  been <= threshold at some time in the past for device {' '.join(cmd)}, processing output", "WARN")
        elif result.returncode != 0:
            log(config, logger, f"Command {' '.join(cmd)} error: returncode={result.returncode}, stderr={result.stderr}", "ERROR")
        return result.stdout.strip(), result.returncode
    except (subprocess.SubprocessError, subprocess.TimeoutExpired) as e:
        log(config, logger, f"Command {' '.join(cmd)} error: {e} (stderr: {e.stderr if hasattr(e, 'stderr') else 'N/A'})", "ERROR")
        return "", -1

def get_raid_status(config, logger):
    """Get RAID array status from mdstat."""
    try:
        with open(config.mdstat_path, "r", encoding="utf-8") as f:
            mdstat = f.read()
        arrays = {}
        current_array = None
        block_lines = []
        for line in mdstat.splitlines():
            line = line.strip()
            if not line or line.startswith("unused devices"):
                continue
            if line.startswith("md"):
                if current_array and block_lines:
                    arrays[current_array] = {
                        "state": "active" if any("[UU]" in l for l in block_lines) else "degraded",
                        "raw": " ".join(block_lines)
                    }
                current_array = line.split()[0]
                block_lines = [line]
            elif current_array:
                block_lines.append(line)
        if current_array and block_lines:
            arrays[current_array] = {
                "state": "active" if any("[UU]" in l for l in block_lines) else "degraded",
                "raw": " ".join(block_lines)
            }
        if not arrays:
            return {"md0": {"state": "unknown", "raw": ""}}
        log(config, logger, f"RAID status: {arrays}", "INFO")
        log(config, logger, f"Raw mdstat: {mdstat[:200]}...", "DEBUG")
        return arrays
    except (FileNotFoundError, IOError) as e:
        log(config, logger, f"Error reading mdstat: {e}", "ERROR")
        return {"md0": {"state": "unknown", "raw": ""}}

def get_disk_io(config, logger, device):
    """Get disk I/O statistics."""
    try:
        io = psutil.disk_io_counters(perdisk=True)
        if device in io:
            reads, writes = io[device].read_bytes, io[device].write_bytes
            log(config, logger, f"{device} raw IO: read_bytes={reads}, write_bytes={writes}", "DEBUG")
            return reads, writes
        log(config, logger, f"No I/O data for {device}", "ERROR")
        return 0, 0
    except Exception as e:  # pylint: disable=broad-except
        log(config, logger, f"Error getting I/O for {device}: {e}", "ERROR")
        return 0, 0

def parse_smart_output(config, logger, result, dev):
    """Parse SMART output into a dictionary."""
    smart_lines = result.splitlines()
    smart_status = None
    for line in smart_lines:
        if "SMART overall-health self-assessment test result:" in line:
            status = line.split(":")[-1].strip().upper()
            if status in ["PASSED", "FAILED", "UNKNOWN"]:
                smart_status = status
            else:
                log(config, logger, f"Unexpected SMART status for {dev}: {status}", "WARNING")
                smart_status = "UNKNOWN"
            break
    if not smart_lines or not smart_status:
        log(config, logger, f"Invalid or empty SMART output for {dev}: {result[:100]}", "ERROR")
        return {"state": "error", "smart_status": None, "temperature": None, "power_on_hours": None,
                "reallocated_sectors": None, "spinup_count": None}
    state = "active" if smart_status == "PASSED" else "warning" if smart_status == "FAILED" else "error"
    temp = None
    temps = []
    poh = None
    realloc = None
    spinup = None
    for line in smart_lines:
        parts = line.split()
        if "Temperature_Celsius" in line and len(parts) >= 10 and parts[9].isdigit():
            temps.append(int(parts[9]))
        elif line.startswith("Temperature Sensor 2:") and len(parts) > 3 and parts[3].isdigit():
            temps.append(int(parts[3]))
        elif line.startswith("Temperature:") and len(parts) > 1 and parts[1].isdigit():
            temps.append(int(parts[1]))
        elif "Power_On_Hours" in line and len(parts) >= 10 and parts[9].isdigit():
            poh = int(parts[9])
        elif line.startswith("Power On Hours:") and len(parts) > 3 and parts[3].isdigit():
            poh = int(parts[3])
        elif "Reallocated_Sector_Ct" in line and len(parts) >= 10 and parts[9].isdigit():
            realloc = int(parts[9])
        elif "Spin_Up_Time" in line and len(parts) >= 10 and parts[9].isdigit():
            spinup = int(parts[9])
        elif "Current_Pending_Sector" in line and len(parts) >= 10 and parts[9].isdigit():
            log(config, logger, f"{dev} Pending Sectors: {parts[9]}", "INFO")
    if temps:
        temp = max(temps)
    info = {
        "state": state,
        "smart_status": smart_status,
        "temperature": temp,
        "power_on_hours": poh,
        "reallocated_sectors": realloc,
        "spinup_count": spinup
    }
    if state in ["warning", "error"]:
        send_alert(config, logger, f"Disk {dev} SMART Issue",
                   f"SMART status: {smart_status}, Output: {result[:200]}")
    return info

def get_disk_info(config, logger, dev, full=False):
    """Get disk information (capacity, SMART attributes)."""
    disk_name = dev.split('/')[-1]
    if dev not in disk_cache or not disk_cache[dev].get('capacity'):
        try:
            size, _ = run_cmd(config, logger, ["blockdev", "--getsize64", dev])
            if size and size.isdigit():
                disk_cache[dev] = {"capacity": size}
            else:
                log(config, logger, f"Invalid blockdev output for {dev}: {size}", "ERROR")
                disk_cache[dev] = {"capacity": "0"}
        except RuntimeError as e:
            log(config, logger, f"Error getting capacity for {dev}: {e}", "ERROR")
            disk_cache[dev] = {"capacity": "0"}
    size = disk_cache[dev]["capacity"]
    try:
        capacity = int(size) // 1024 // 1024 // 1024 if size and size.isdigit() else 0
    except ValueError:
        log(config, logger, f"Invalid capacity for {dev}: {size}", "ERROR")
        capacity = 0
    if not full:
        return {"name": disk_name, "capacity": capacity, "state": "unknown"}
    if dev == "/dev/md1":
        return {"name": disk_name, "capacity": capacity, "state": "unknown",
                "smart_status": None, "temperature": None, "power_on_hours": None,
                "reallocated_sectors": None, "spinup_count": None}
    # Try multiple device types for SMART data
    dev_types = ["nvme", "auto"] if "nvme" in dev else ["sat", "auto"]
    for dev_type in dev_types:
        try:
            cmd = ["smartctl", "-a", "-d", dev_type, dev]
            result, returncode = run_cmd(config, logger, cmd)
            if not result:
                log(config, logger, f"No SMART output for {dev} with -d {dev_type}", "ERROR")
                continue
            smart_info = parse_smart_output(config, logger, result, dev)
            smart_info["name"] = disk_name
            smart_info["capacity"] = capacity
            if returncode == 32:
                smart_info["state"] = "active"
                log(config, logger, f"{dev} is in standby, setting state to idle", "INFO")
            return smart_info
        except RuntimeError as e:
            log(config, logger, f"Error getting SMART data for {dev} with -d {dev_type}: {e}", "ERROR")
            continue
    log(config, logger, f"Failed to get SMART data for {dev} after all attempts", "ERROR")
    return {"name": disk_name, "capacity": capacity, "state": "error",
            "smart_status": None, "temperature": None, "power_on_hours": None,
            "reallocated_sectors": None, "spinup_count": None}

def get_usage(config, logger, mount):
    """Get disk usage for a mount point."""
    try:
        result, _ = run_cmd(config, logger, ["df", "-h", mount])
        if result:
            for line in result.splitlines()[1:]:
                parts = line.split()
                if len(parts) >= 6 and parts[-1] == mount:
                    percent = int(parts[4].rstrip("%"))
                    free = parts[3]
                    log(config, logger, f"df output for {mount}: {percent}% used, {free} free", "DEBUG")
                    return percent, free
        log(config, logger, f"No matching df output for {mount}, falling back to psutil", "DEBUG")
        usage = psutil.disk_usage(mount)
        percent = int(usage.percent)
        free = f"{usage.free // 1024 // 1024 // 1024}G"
        return percent, free
    except Exception as e:  # pylint: disable=broad-except
        log(config, logger, f"Error getting usage for {mount}: {e}", "ERROR")
        return 0, "N/A"

def check_package_version(config, logger):
    """Check and update package version if needed."""
    try:
        if config.package_version == "latest":
            response = requests.get(  # pylint: disable=missing-timeout
                "https://api.github.com/repos/joaofolino/mqtt-monitor/releases/latest")
            version = response.json()['tag_name'].lstrip('v')
        else:
            version = config.package_version
        current_version, _ = run_cmd(config, logger, [
            "dpkg-query", "--showformat=${Version}", "--show", "mqtt-monitor"])
        current_version = current_version.strip()
        if current_version != version:
            log(config, logger, f"Updating package from {current_version} to {version}", "INFO")
            deb_url = (
                f"https://github.com/joaofolino/mqtt-monitor/releases/download/"
                f"v{version}/mqtt-monitor_{version}.deb")
            subprocess.run(["wget", "-O", "/tmp/mqtt-monitor.deb", deb_url], check=True)
            subprocess.run(["apt", "install", "-y", "/tmp/mqtt-monitor.deb"], check=True)
            log(config, logger, f"Updated to version {version}", "INFO")
            return True
    except Exception as e:  # pylint: disable=broad-except
        log(config, logger, f"Failed to check/update package version: {e}", "ERROR")
    return False

def setup_sensors(config, client):
    """Set up Home Assistant sensor configurations."""
    # Host sensors
    for sensor, unit in [
        ("cpu_usage", "%"),
        ("memory_used", "MB"),
        ("cpu_temp", "°C")
    ]:
        client.publish(f"homeassistant/sensor/server_{sensor}/config", json.dumps({
            "name": f"Server {sensor.replace('_', ' ').title()}",
            "state_topic": f"{config.topic_prefix}/host/{sensor}",
            "unique_id": f"server_{sensor}",
            "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
            "unit_of_measurement": unit
        }), retain=True)
    # Disk usage sensors for mount points
    for mount in config.disk_usage_mounts:
        mount_name = mount.strip('/').replace('/', '_') or 'root'
        client.publish(f"homeassistant/sensor/server_{mount_name}_usage/config", json.dumps({
            "name": f"Server {mount_name.replace('_', ' ').title()} Usage",
            "state_topic": f"{config.topic_prefix}/host/{mount_name}_usage",
            "unique_id": f"server_{mount_name}_usage",
            "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
            "unit_of_measurement": "%"
        }), retain=True)
    # Disk sensors
    for disk in config.devices:
        disk_name = disk.split('/')[-1]
        for attr, unit in [
            ("state", ""),
            ("temperature", "°C"),
            ("reallocated_sectors", ""),
            ("spinup_count", "")
        ]:
            client.publish(f"homeassistant/sensor/disk_{disk_name}_{attr}/config", json.dumps({
                "name": f"Disk {disk_name} {attr.replace('_', ' ').title()}",
                "state_topic": f"{config.topic_prefix}/disk/{disk_name}/{attr}",
                "unique_id": f"disk_{disk_name}_{attr}",
                "device": {"identifiers": ["server_monitor"]},
                "unit_of_measurement": unit
            }), retain=True)
        if config.enable_io_mqtt:
            for attr in ["read_rate", "write_rate"]:
                client.publish(f"homeassistant/sensor/disk_{disk_name}_{attr}/config", json.dumps({
                    "name": f"Disk {disk_name} {attr.replace('_', ' ').title()}",
                    "state_topic": f"{config.topic_prefix}/disk/{disk_name}/{attr}",
                    "unique_id": f"disk_{disk_name}_{attr}",
                    "device": {"identifiers": ["server_monitor"]},
                    "unit_of_measurement": "KB/s"
                }), retain=True)
    # RAID sensors
    for array in config.raid_arrays:
        client.publish(f"homeassistant/sensor/raid_{array}_state/config", json.dumps({
            "name": f"RAID {array} State",
            "state_topic": f"{config.topic_prefix}/raid/{array}/state",
            "unique_id": f"raid_{array}_state",
            "device": {"identifiers": ["server_monitor"]}
        }), retain=True)

def publish_data(config, logger, client, topic, value, retain=False):
    """Publish data to MQTT topic."""
    try:
        if value is None:
            log(config, logger, f"Skipping MQTT publish for {topic}: value is None", "DEBUG")
            return  # Skip publishing if value is None
        # Convert numeric values to string, handle valid values only
        if isinstance(value, (int, float)):
            value = str(value)
        log(config, logger, f"MQTT publish: {topic} = {value}", "DEBUG")
        client.publish(topic, value, retain=retain)
    except ConnectionError as e:
        log(config, logger, f"MQTT publish error {topic}: {e}", "ERROR")
        connect_mqtt(config, logger, client)

def main():
    """Run the main monitoring loop."""
    config = load_config(os.getenv('CONFIG_PATH', '/etc/mqtt-monitor'))
    logger = setup_logging(config)
    state = MonitorState()
    client = mqtt.Client()
    connect_mqtt(config, logger, client)
    if check_package_version(config, logger):
        return
    setup_sensors(config, client)
    devices = [d.split('/')[-1] for d in config.devices]
    for dev in devices:
        state.prev_io[dev] = (0, 0)
    last_smart_check = 0
    while True:
        try:
            current_time = time.time()
            delta_time = 60.0 if state.prev_time is None else current_time - state.prev_time
            state.prev_time = current_time
            log(config, logger, f"Delta time: {delta_time:.2f}s", "DEBUG")
            # Host metrics
            publish_data(config, logger, client,
                         f"{config.topic_prefix}/host/cpu_usage", psutil.cpu_percent())
            publish_data(config, logger, client,
                         f"{config.topic_prefix}/host/memory_used",
                         psutil.virtual_memory().used // 1024 // 1024)
            with open(config.cpu_temp_path, "r", encoding="utf-8") as f:
                cpu_temp = int(f.read().strip()) / 1000
            publish_data(config, logger, client,
                         f"{config.topic_prefix}/host/cpu_temp", cpu_temp)
            # Disk I/O
            current_io = {}
            for dev in devices:
                reads, writes = get_disk_io(config, logger, dev)
                current_io[dev] = (reads, writes)
                prev_reads, prev_writes = state.prev_io.get(dev, (0, 0))
                read_delta = (reads - prev_reads)
                write_delta = (writes - prev_writes)
                read_rate = min(read_delta / delta_time / 1024, config.max_io_rate)
                write_rate = min(write_delta / delta_time / 1024, config.max_io_rate)
                log(config, logger, f"{dev} IO: read_rate={read_rate:.2f} KB/s, "
                                    f"write_rate={write_rate:.2f} KB/s", "INFO")
                if config.enable_io_mqtt:
                    publish_data(config, logger, client,
                                 f"{config.topic_prefix}/disk/{dev}/read_rate", f"{read_rate:.2f}")
                    publish_data(config, logger, client,
                                 f"{config.topic_prefix}/disk/{dev}/write_rate", f"{write_rate:.2f}")
                if dev == "md1" and write_rate > config.md1_write_spike_threshold:
                    send_alert(config, logger, "md1 Write Spike",
                               f"Write rate: {write_rate:.2f} KB/s")
                state.prev_io[dev] = (reads, writes)
            # RAID status
            raid_status = get_raid_status(config, logger)
            for array, status in raid_status.items():
                if array not in config.raid_arrays:
                    config.raid_arrays.append(array)
                    publish_data(config, logger, client,
                                 f"homeassistant/sensor/raid_{array}_state/config", json.dumps({
                            "name": f"RAID {array} State",
                            "state_topic": f"{config.topic_prefix}/raid/{array}/state",
                            "unique_id": f"raid_{array}_state",
                            "device": {"identifiers": ["server_monitor"]}
                        }), retain=True)
                publish_data(config, logger, client,
                             f"{config.topic_prefix}/raid/{array}/state", status["state"])
                if status["state"] == "degraded":
                    send_alert(config, logger, f"RAID {array} Degraded",
                               f"Status: {status['raw']}")
            # Disk usage
            for mount in config.disk_usage_mounts:
                usage, free = get_usage(config, logger, mount)
                mount_name = mount.strip('/').replace('/', '_') or 'root'
                publish_data(config, logger, client,
                             f"{config.topic_prefix}/host/{mount_name}_usage", usage)
                log(config, logger, f"{mount}: {usage}% used, {free} free", "INFO")
                if usage > config.md1_usage_threshold:
                    send_alert(config, logger, f"{mount_name.replace('_', ' ').title()} Capacity Critical",
                               f"{mount} at {usage}%, only {free} free")
            # Disk SMART data
            if current_time - last_smart_check >= config.smart_interval:
                for disk in config.devices:
                    disk_cache[disk] = get_disk_info(config, logger, disk, full=True)
                last_smart_check = current_time
            else:
                for disk in config.devices:
                    if disk_cache.get(disk, {}).get("state") != "active":
                        disk_cache[disk] = get_disk_info(config, logger, disk, full=False)
            for disk in config.devices:
                disk_name = disk.split('/')[-1]
                info = disk_cache[disk]
                for key in ["state", "temperature", "reallocated_sectors", "spinup_count"]:
                    publish_data(config, logger, client,
                                 f"{config.topic_prefix}/disk/{disk_name}/{key}",
                                 info.get(key))
                log(config, logger, f"{disk}: state={info['state']}, "
                                    f"temp={info.get('temperature', 'N/A')}°C", "INFO")
            time.sleep(config.sleep_interval)
        except (ConnectionError, IOError, RuntimeError) as e:
            log(config, logger, f"Main loop error: {e}", "ERROR")
            time.sleep(config.error_sleep)

if __name__ == "__main__":
    main()
