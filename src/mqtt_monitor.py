#!/usr/bin/env python3
import subprocess
import json
import time
import os
import paho.mqtt.client as mqtt
import psutil
import configparser
import smtplib
import requests
from email.mime.text import MIMEText
from datetime import datetime
import logging
from dataclasses import dataclass

@dataclass
class Config:
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

# Global state
disk_cache = {}
prev_io = {}
prev_time = None

def setup_logging(config):
    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    logger.handlers = []  # Clear existing handlers
    # FileHandler
    log_file_path = os.path.join(os.path.abspath(config.log_dir), config.log_file)
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(file_handler)
    # StreamHandler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
    stream_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(stream_handler)
    return logger

def log(config, logger, message, level="INFO"):
    try:
        log_level = getattr(logging, level.upper(), logging.INFO)
        if logger.isEnabledFor(log_level):
            getattr(logger, level.lower())(message)
        usage = psutil.disk_usage(os.path.abspath(config.log_dir))
        if usage.percent > 95:
            send_alert(config, logger, "Log Directory Full", f"Log directory {config.log_dir} is at {usage.percent}% capacity")
    except Exception as e:
        print(f"Logging error: {e}")

def on_connect(config, logger, client, userdata, flags, rc):
    if rc == 0:
        log(config, logger, "Connected to MQTT broker", "INFO")
    else:
        log(config, logger, f"Failed to connect to MQTT broker, code: {rc}", "ERROR")

def connect_mqtt(config, logger, client):
    try:
        client.username_pw_set(config.mqtt_user, config.mqtt_pass)
        client.on_connect = lambda c, u, f, r: on_connect(config, logger, c, u, f, r)
        client.connect(config.broker, config.port, 60)
        client.loop_start()
    except Exception as e:
        log(config, logger, f"Failed to connect to MQTT broker: {e}", "ERROR")

def send_alert(config, logger, subject, body):
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
    except Exception as e:
        log(config, logger, f"Failed to send alert {subject}: {e}", "ERROR")

def run_cmd(config, logger, cmd):
    try:
        log(config, logger, f"Executing command: {' '.join(cmd)}", "DEBUG")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10)
        log(config, logger, f"Command {' '.join(cmd)} output: {result.stdout[:100]}...", "DEBUG")
        return result.stdout.strip()
    except subprocess.SubprocessError as e:
        log(config, logger, f"Command {' '.join(cmd)} error: {e}", "ERROR")
        return ""

def get_raid_status(config, logger):
    try:
        with open(config.mdstat_path, "r") as f:
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
    except Exception as e:
        log(config, logger, f"Error reading mdstat: {e}", "ERROR")
        return {"md0": {"state": "unknown", "raw": ""}}

def get_disk_io(config, logger, device):
    try:
        io = psutil.disk_io_counters(perdisk=True)
        if device in io:
            reads, writes = io[device].read_bytes, io[device].write_bytes
            log(config, logger, f"{device} raw IO: read_bytes={reads}, write_bytes={writes}", "DEBUG")
            return reads, writes
        log(config, logger, f"No I/O data for {device}", "ERROR")
        return 0, 0
    except Exception as e:
        log(config, logger, f"Error getting I/O for {device}: {e}", "ERROR")
        return 0, 0

def get_disk_info(config, logger, dev, full=False):
    if dev not in disk_cache or not disk_cache[dev].get('capacity'):
        try:
            size = run_cmd(config, logger, ["blockdev", "--getsize64", dev])
            if size and size.isdigit():
                disk_cache[dev] = {"capacity": size}
            else:
                log(config, logger, f"Invalid blockdev output for {dev}: {size}", "ERROR")
                disk_cache[dev] = {"capacity": "0"}
        except Exception as e:
            log(config, logger, f"Error getting capacity for {dev}: {e}", "ERROR")
            disk_cache[dev] = {"capacity": "0"}
    size = disk_cache[dev]["capacity"]
    try:
        capacity = int(size) // 1024 // 1024 // 1024 if size and size.isdigit() else 0
    except ValueError:
        log(config, logger, f"Invalid capacity for {dev}: {size}", "ERROR")
        capacity = 0
    if not full:
        return {"name": dev.split('/')[-1], "capacity": capacity, "state": "unknown"}
    if dev == "/dev/md1":
        return {"name": dev.split('/')[-1], "capacity": capacity, "state": "unknown", "health": "N/A", "temperature": "N/A"}
    dev_type = "nvme" if "nvme" in dev else "scsi"
    for attempt_type in [dev_type, "auto"]:
        try:
            cmd = ["smartctl", "-a", "-d", attempt_type, dev]
            result = run_cmd(config, logger, cmd)
            if not result:
                log(config, logger, f"No SMART output for {dev} with -d {attempt_type}", "DEBUG")
                return {"name": dev.split('/')[-1], "capacity": capacity, "state": "error", "health": "N/A", "temperature": "N/A"}
            smart_lines = result.splitlines()
            has_valid_status = any("PASSED" in line or "FAILED" in line for line in smart_lines)
            if not smart_lines or not has_valid_status:
                log(config, logger, f"Invalid or empty SMART output for {dev} with -d {attempt_type}: {result[:100]}", "DEBUG")
                return {"name": dev.split('/')[-1], "capacity": capacity, "state": "error", "health": "N/A", "temperature": "N/A"}
            health = any("PASSED" in line for line in smart_lines)
            state = "active" if health else "failed"
            temp = "N/A"
            poh = "N/A"
            realloc = "N/A"
            spinup = "N/A"
            for line in smart_lines:
                parts = line.split()
                if line.startswith("Temperature:") and len(parts) > 1 and parts[1].isdigit():
                    temp = parts[1]
                elif line.startswith("Current Drive Temperature:") and len(parts) > 3 and parts[3].isdigit():
                    temp = parts[3]
                elif "Power_On_Hours" in line and len(parts) >= 2:
                    poh = parts[-1] if parts[-1].isdigit() else "N/A"
                elif line.startswith("Reallocated_Sector_Ct") and len(parts) >= 2:
                    realloc = parts[-1] if parts[-1].isdigit() else "N/A"
                elif line.startswith("Spin_Up_Time") and len(parts) >= 2:
                    spinup = parts[-1] if parts[-1].isdigit() else "N/A"
            info = {
                "name": dev.split('/')[-1], "capacity": capacity, "state": state,
                "health": "OK" if health else "FAILED", "temperature": temp, "power_on_hours": poh,
                "reallocated_sectors": realloc, "spinup_count": spinup
            }
            if not health and any("FAILED" in line for line in smart_lines):
                send_alert(config, logger, f"Disk {dev} Failed", f"SMART health check failed: {result[:200]}")
            return info
        except Exception as e:
            log(config, logger, f"Error getting SMART data for {dev} with -d {attempt_type}: {e}", "ERROR")
            continue
    log(config, logger, f"Failed to get SMART data for {dev} after all attempts", "ERROR")
    return {"name": dev.split('/')[-1], "capacity": capacity, "state": "error", "health": "N/A", "temperature": "N/A"}

def get_usage(config, logger, mount):
    try:
        result = run_cmd(config, logger, ["df", "-h", mount])
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
    except Exception as e:
        log(config, logger, f"Error getting usage for {mount}: {e}", "ERROR")
        return 0, "N/A"

def check_package_version(config, logger):
    try:
        if config.package_version == "latest":
            response = requests.get("https://api.github.com/repos/joaofolino/mqtt-monitor/releases/latest")
            version = response.json()['tag_name'].lstrip('v')
        else:
            version = config.package_version
        current_version = run_cmd(config, logger, ["dpkg-query", "--showformat=${Version}", "--show", "mqtt-monitor"]).strip()
        if current_version != version:
            log(config, logger, f"Updating package from {current_version} to {version}", "INFO")
            deb_url = f"https://github.com/joaofolino/mqtt-monitor/releases/download/v{version}/mqtt-monitor_{version}.deb"
            subprocess.run(["wget", "-O", "/tmp/mqtt-monitor.deb", deb_url], check=True)
            subprocess.run(["apt", "install", "-y", "/tmp/mqtt-monitor.deb"], check=True)
            log(config, logger, f"Updated to version {version}", "INFO")
            return True
    except Exception as e:
        log(config, logger, f"Failed to check/update package version: {e}", "ERROR")
    return False

def main():
    config = load_config(os.getenv('CONFIG_PATH', '/etc/mqtt-monitor'))
    logger = setup_logging(config)

    client = mqtt.Client()
    connect_mqtt(config, logger, client)

    if check_package_version(config, logger):
        return

    for sensor in ["cpu_usage", "memory_used", "cpu_temp", "md1_usage", "nvme_usage"]:
        unit = "%" if "usage" in sensor else "°C" if "temp" in sensor else "MB"
        client.publish(f"homeassistant/sensor/server_{sensor}/config", json.dumps({
            "name": f"Server {sensor.replace('_', ' ').title()}", "state_topic": f"{config.topic_prefix}/host/{sensor}",
            "unique_id": f"server_{sensor}", "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
            "unit_of_measurement": unit
        }), retain=True)
    for disk in config.devices:
        disk_name = disk.split('/')[-1]
        for attr in ["state", "temperature", "reallocated_sectors", "spinup_count"]:
            unit = "°C" if "temp" in attr else ""
            client.publish(f"homeassistant/sensor/disk_{disk_name}_{attr}/config", json.dumps({
                "name": f"Disk {disk_name} {attr.title()}", "state_topic": f"{config.topic_prefix}/disk/{disk_name}/{attr}",
                "unique_id": f"disk_{disk_name}_{attr}", "device": {"identifiers": ["server_monitor"]},
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
    for array in config.raid_arrays:
        client.publish(f"homeassistant/sensor/raid_{array}_state/config", json.dumps({
            "name": f"RAID {array} State", "state_topic": f"{config.topic_prefix}/raid/{array}/state",
            "unique_id": f"raid_{array}_state", "device": {"identifiers": ["server_monitor"]}
        }), retain=True)

    devices = [d.split('/')[-1] for d in config.devices]
    for dev in devices:
        prev_io[dev] = (0, 0)

    last_smart_check = 0
    def publish(topic, value, retain=False):
        try:
            log(config, logger, f"MQTT publish: {topic} = {value}", "DEBUG")
            client.publish(topic, str(value), retain=retain)
        except Exception as e:
            log(config, logger, f"MQTT publish error {topic}: {e}", "ERROR")
            connect_mqtt(config, logger, client)

    global prev_time
    while True:
        try:
            log(config, logger, "Starting loop cycle", "DEBUG")
            current_time = time.time()
            delta_time = 60.0 if prev_time is None else (current_time - prev_time)
            prev_time = current_time
            log(config, logger, f"Delta time: {delta_time:.2f}s", "DEBUG")

            publish(f"{config.topic_prefix}/host/cpu_usage", psutil.cpu_percent())
            publish(f"{config.topic_prefix}/host/memory_used", psutil.virtual_memory().used // 1024 // 1024)
            cpu_temp = int(open(config.cpu_temp_path).read().strip()) / 1000
            publish(f"{config.topic_prefix}/host/cpu_temp", cpu_temp)

            current_io = {dev: get_disk_io(config, logger, dev) for dev in devices}
            for dev in current_io:
                reads, writes = current_io[dev]
                prev_reads, prev_writes = prev_io.get(dev, (0, 0))
                read_delta = reads - prev_reads
                write_delta = writes - prev_writes
                read_rate = min(read_delta / delta_time / 1024, config.max_io_rate)
                write_rate = min(write_delta / delta_time / 1024, config.max_io_rate)
                log(config, logger, f"{dev} IO: read_rate={read_rate:.2f} KB/s, write_rate={write_rate:.2f} KB/s", "INFO")
                if config.enable_io_mqtt:
                    publish(f"{config.topic_prefix}/disk/{dev}/read_rate", f"{read_rate:.2f}")
                    publish(f"{config.topic_prefix}/disk/{dev}/write_rate", f"{write_rate:.2f}")
                if dev == "md1" and write_rate > config.md1_write_spike_threshold:
                    send_alert(config, logger, "md1 Write Spike", f"Write rate: {write_rate:.2f} KB/s")
            prev_io.update(current_io)

            log(config, logger, "Checking RAID status", "DEBUG")
            raid_status = get_raid_status(config, logger)
            for array in raid_status:
                if array not in config.raid_arrays:
                    config.raid_arrays.append(array)
                    client.publish(f"homeassistant/sensor/raid_{array}_state/config", json.dumps({
                        "name": f"RAID {array} State", "state_topic": f"{config.topic_prefix}/raid/{array}/state",
                        "unique_id": f"raid_{array}_state", "device": {"identifiers": ["server_monitor"]}
                    }), retain=True)
                publish(f"{config.topic_prefix}/raid/{array}/state", raid_status[array]["state"])
                if raid_status[array]["state"] == "degraded":
                    send_alert(config, logger, f"RAID {array} Degraded", f"Status: {raid_status[array]['raw']}")

            log(config, logger, "Checking usage", "DEBUG")
            for mount in config.disk_usage_mounts:
                usage, free = get_usage(config, logger, mount)
                mount_name = mount.strip('/').replace('/', '_') or 'root'
                publish(f"{config.topic_prefix}/host/{mount_name}_usage", usage)
                log(config, logger, f"{mount}: {usage}% used, {free} free", "INFO")
                if mount == "/" and usage > config.md1_usage_threshold:
                    send_alert(config, logger, "md1 Capacity Critical", f"md1 at {usage}%, only {free} free")

            log(config, logger, "Checking disk info", "DEBUG")
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
                    publish(f"{config.topic_prefix}/disk/{disk_name}/{key}", info.get(key, "N/A"))
                log(config, logger, f"{disk}: state={info['state']}, temp={info.get('temperature', 'N/A')}°C", "INFO")

            log(config, logger, "Loop cycle complete", "DEBUG")
            time.sleep(config.sleep_interval)
        except Exception as e:
            log(config, logger, f"Main loop error: {e}", "ERROR")
            time.sleep(config.error_sleep)

if __name__ == "__main__":
    main()
