#!/usr/bin/env python3
import subprocess
import json
import time
import os
import paho.mqtt.client as mqtt
import psutil
import configparser
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

# Load configuration
config = configparser.ConfigParser()
config.read('/etc/monitor/monitor.conf')

# General configs
ENABLE_EMAIL = config.getboolean('monitor', 'enable_email')
LOG_LEVEL = config['monitor']['log_level']
ENABLE_IO_MQTT = config.getboolean('monitor', 'enable_io_mqtt')
ALERT_EMAIL = config['monitor']['alert_email']
SMTP_SERVER = config['monitor']['smtp_server']
SMTP_PORT = config.getint('monitor', 'smtp_port')
SMTP_USER = config['monitor']['smtp_user']
SMTP_PASS = config['monitor']['smtp_pass']

# MQTT configs
BROKER = config['mqtt']['broker']
PORT = config.getint('mqtt','port')
MQTT_USER = config['mqtt']['mqtt_user']
MQTT_PASS = config['mqtt']['mqtt_pass']
SELF_NAME = config['mqtt']['self_name']

# Internal configs
SMART_INTERVAL = config.getint('internals', 'smart_interval')
TOPIC_PREFIX = config['internals']['topic_prefix']
LOG_DIR = config['internals']['log_dir']
LOG_FILE = config['internals']['log_file')
DEVICES = config['internals']['devices'].split(',')
RAID_ARRAYS = config['internals']['raid_arrays'].split(',')
MDSTAT_PATH = config['internals']['mdstat_path']
CPU_TEMP_PATH = config['internals']['cpu_temp_path']
DISK_USAGE_MOUNTS = config['internals']['disk_usage_mounts'].split(',')
MAX_IO_RATE = config.getfloat('internals', 'max_io_rate')
MD1_USAGE_THRESHOLD = config.getfloat('internals','md1_usage_threshold')
MD1_WRITE_SPIKE_THRESHOLD = config.getfloat('internals', 'md1_write_spike_threshold')
SLEEP_INTERVAL = config.getfloat('internals', 'sleep_interval')
ERROR_SLEEP = config.getfloat('internals', 'error_sleep')

disk_cache = {}
prev_io = {}
prev_time = None

client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASS)
client.connect(BROKER, PORT, 60)

def log(message, level="INFO"):
    if level == "DEBUG" and LOG_LEVEL != "DEBUG":
        return
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now()} [{level}] {message}\n")

def send_alert(subject, body):
    if not ENABLE_EMAIL:
        log(f"Email alerts disabled: {subject}", "INFO")
        return
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = SMTP_USER
        msg['To'] = ALERT_EMAIL
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, ALERT_EMAIL, msg.as_string())
        log(f"Sent alert: {subject}", "INFO")
    except Exception as e:
        log(f"Failed to send alert {subject}: {e}", "ERROR")

def run_cmd(cmd):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10)
        log(f"Command {' '.join(cmd)} output: {result.stdout[:100]}...", "DEBUG")
        return result.stdout.strip()
    except subprocess.SubprocessError as e:
        log(f"Command {' '.join(cmd)} error: {e}", "ERROR")
        return ""

def get_raid_status():
    try:
        with open(MDSTAT_PATH, "r") as f:
            mdstat = f.read()
        arrays = {}
        current_array = None
        block_lines = []
        for line in mdstat.splitlines():
            line = line.strip()
            if not line or line.startswith("unused devices"):
                continue
            if line.startswith("md"):
                if current_array:
                    arrays[current_array] = {
                        "state": "active" if any("[UU]" in l for l in block_lines) else "degraded",
                        "raw": " ".join(block_lines)
                    }
                current_array = line.split()[0]
                block_lines = [line]
            elif current_array:
                block_lines.append(line)
        if current_array:
            arrays[current_array] = {
                "state": "active" if any("[UU]" in l for l in block_lines) else "degraded",
                "raw": " ".join(block_lines)
            }
        log(f"RAID status: {arrays}", "INFO")
        log(f"Raw mdstat: {mdstat[:200]}...", "DEBUG")
        return arrays
    except Exception as e:
        log(f"Error reading mdstat: {e}", "ERROR")
        return {"md0": {"state": "unknown", "raw": ""}}

def get_disk_io(device):
    try:
        io = psutil.disk_io_counters(perdisk=True)
        if device in io:
            reads, writes = io[device].read_bytes, io[device].write_bytes
            log(f"{device} raw IO: read_bytes={reads}, write_bytes={writes}", "DEBUG")
            return reads, writes
        log(f"No I/O data for {device}", "ERROR")
        return 0, 0
    except Exception as e:
        log(f"Error getting I/O for {device}: {e}", "ERROR")
        return 0, 0

def get_disk_info(dev, full=False):
    if dev not in disk_cache:
        try:
            size = run_cmd(["blockdev", "--getsize64", dev])
            disk_cache[dev] = {"capacity": size if size else "0"}
        except Exception as e:
            log(f"Error getting capacity for {dev}: {e}", "ERROR")
            disk_cache[dev] = {"capacity": "0"}
    size = disk_cache[dev]["capacity"]
    try:
        capacity = int(size) // 1024 // 1024 // 1024 if size else 0
    except ValueError:
        log(f"Invalid capacity for {dev}: {size}", "ERROR")
        capacity = 0
    if not full:
        return {"name": dev.split('/')[-1], "capacity": capacity, "state": "unknown"}
    if dev == "/dev/md1":
        return {"name": dev.split('/')[-1], "capacity": capacity, "state": "unknown", "health": "N/A", "temperature": "N/A"}
    dev_type = "nvme" if "nvme" in dev else "scsi"
    for attempt_type in [dev_type, "auto"]:
        try:
            cmd = ["smartctl", "-a", "-d", attempt_type, dev]
            result = run_cmd(cmd)
            if not result:
                log(f"No SMART output for {dev} with -d {attempt_type}", "DEBUG")
                continue
            smart_lines = result.splitlines()
            health = "PASSED" in result
            state = "active" if health else "unknown"
            temp = "N/A"
            for line in smart_lines:
                if line.startswith("Temperature:") and len(line.split()) > 1 and line.split()[1].isdigit():
                    temp = line.split()[1]
                    break
                if line.startswith("Current Drive Temperature:") and len(line.split()) > 3 and line.split()[3].isdigit():
                    temp = line.split()[3]
                    break
            poh = next((line.split()[-2] for line in smart_lines if "Power_On_Hours" in line), "N/A")
            realloc = next((line.split()[9] for line in smart_lines if line.startswith("Reallocated_Sector_Ct")), "N/A")
            spinup = next((line.split()[9] for line in smart_lines if line.startswith("Spin_Up_Time")), "N/A")
            return {
                "name": dev.split('/')[-1], "capacity": capacity, "state": state,
                "health": "OK" if health else "FAILED", "temperature": temp, "power_on_hours": poh,
                "reallocated_sectors": realloc, "spinup_count": spinup
            }
        except Exception as e:
            log(f"Error getting SMART data for {dev} with -d {attempt_type}: {e}", "ERROR")
            continue
    log(f"Failed to get SMART data for {dev} after all attempts", "ERROR")
    return {"name": dev.split('/')[-1], "capacity": capacity, "state": "error", "health": "N/A", "temperature": "N/A"}

def get_usage(mount):
    try:
        result = run_cmd(["df", "-h", mount])
        for line in result.splitlines()[1:]:
            parts = line.split()
            if len(parts) >= 6 and mount in parts[-1]:
                percent = int(parts[4].rstrip("%"))
                free = parts[3]
                return percent, free
        usage = psutil.disk_usage(mount)
        percent = int(usage.percent)
        free = f"{usage.free // 1024 // 1024 // 1024}G"
        return percent, free
    except Exception as e:
        log(f"Error getting usage for {mount}: {e}", "ERROR")
        return 0, "N/A"

def main():
    # Configure MQTT sensors
    for sensor in ["cpu_usage", "memory_used", "cpu_temp", "md1_usage", "nvme_usage"]:
        unit = "%" if "usage" in sensor else "°C" if "temp" in sensor else "MB"
        client.publish(f"homeassistant/sensor/server_{sensor}/config", json.dumps({
            "name": f"Server {sensor.replace('_', ' ').title()}", "state_topic": f"{TOPIC_PREFIX}/host/{sensor}",
            "unique_id": f"server_{sensor}", "device": {"identifiers": ["server_monitor"], "name": "Server Monitor"},
            "unit_of_measurement": unit
        }), retain=True)
    disks = ["/dev/sda", "/dev/sdb", "/dev/sdc", "/dev/sdd", "/dev/nvme0n1", "/dev/md1"]
    for disk in disks:
        disk_name = disk.split('/')[-1]
        for attr in ["state", "temperature", "reallocated_sectors", "spinup_count"]:
            unit = "°C" if "temp" in attr else ""
            client.publish(f"homeassistant/sensor/disk_{disk_name}_{attr}/config", json.dumps({
                "name": f"Disk {disk_name} {attr.title()}", "state_topic": f"{TOPIC_PREFIX}/disk/{disk_name}/{attr}",
                "unique_id": f"disk_{disk_name}_{attr}", "device": {"identifiers": ["server_monitor"]},
                "unit_of_measurement": unit
            }), retain=True)
        if ENABLE_IO_MQTT:
            for attr in ["read_rate", "write_rate"]:
                client.publish(f"homeassistant/sensor/disk_{disk_name}_{attr}/config", json.dumps({
                    "name": f"Disk {disk_name} {attr.replace('_', ' ').title()}", 
                    "state_topic": f"{TOPIC_PREFIX}/disk/{disk_name}/{attr}",
                    "unique_id": f"disk_{disk_name}_{attr}", 
                    "device": {"identifiers": ["server_monitor"]},
                    "unit_of_measurement": "KB/s"
                }), retain=True)
    for array in RAID_ARRAYS:
        client.publish(f"homeassistant/sensor/raid_{array}_state/config", json.dumps({
            "name": f"RAID {array} State", "state_topic": f"{TOPIC_PREFIX}/raid/{array}/state",
            "unique_id": f"raid_{array}_state", "device": {"identifiers": ["server_monitor"]}
        }), retain=True)

    devices = [d.split('/')[-1] for d in DEVICES]
    for dev in devices:
        prev_io[dev] = (0, 0)
    
    last_smart_check = 0
    def publish(topic, value, retain=False):
        try:
            log(f"MQTT publish: {topic} = {value}", "DEBUG")
            client.publish(topic, str(value), retain=retain)
        except Exception as e:
            log(f"MQTT publish error {topic}: {e}", "ERROR")

    global prev_time
    while True:
        try:
            log("Starting loop cycle", "DEBUG")
            current_time = time.time()
            delta_time = 60.0 if prev_time is None else (current_time - prev_time)
            prev_time = current_time
            log(f"Delta time: {delta_time:.2f}s", "DEBUG")

            publish(f"{TOPIC_PREFIX}/host/cpu_usage", psutil.cpu_percent())
            publish(f"{TOPIC_PREFIX}/host/memory_used", psutil.virtual_memory().used // 1024 // 1024)
            cpu_temp = int(open(CPU_TEMP_PATH).read().strip()) / 1000
            publish(f"{TOPIC_PREFIX}/host/cpu_temp", cpu_temp)

            current_io = {dev: get_disk_io(dev) for dev in devices}
            for dev in current_io:
                reads, writes = current_io[dev]
                prev_reads, prev_writes = prev_io.get(dev, (0, 0))
                read_delta = reads - prev_reads
                write_delta = writes - prev_writes
                read_rate = min(read_delta / delta_time / 1024, MAX_IO_RATE)  # KB/s
                write_rate = min(write_delta / delta_time / 1024, MAX_IO_RATE)  # KB/s
                log(f"{dev} IO: read_rate={read_rate:.2f} KB/s, write_rate={write_rate:.2f} KB/s", "INFO")
                if ENABLE_IO_MQTT:
                    publish(f"{TOPIC_PREFIX}/disk/{dev}/read_rate", f"{read_rate:.2f}")
                    publish(f"{TOPIC_PREFIX}/disk/{dev}/write_rate", f"{write_rate:.2f}")
                if dev == "md1" and write_rate > MD1_WRITE_SPIKE_THRESHOLD:
                    send_alert("md1 Write Spike", f"Write rate: {write_rate:.2f} KB/s")
            prev_io.update(current_io)

            log("Checking RAID status", "DEBUG")
            raid_status = get_raid_status()
            for array in raid_status:
                if array not in RAID_ARRAYS:
                    RAID_ARRAYS.append(array)
                    client.publish(f"homeassistant/sensor/raid_{array}_state/config", json.dumps({
                        "name": f"RAID {array} State", "state_topic": f"{TOPIC_PREFIX}/raid/{array}/state",
                        "unique_id": f"raid_{array}_state", "device": {"identifiers": ["server_monitor"]}
                    }), retain=True)
                publish(f"{TOPIC_PREFIX}/raid/{array}/state", raid_status[array]["state"])
                if raid_status[array]["state"] == "degraded":
                    send_alert(f"RAID {array} Degraded", f"Status: {raid_status[array]['raw']}")

            log("Checking usage", "DEBUG")
            for mount in DISK_USAGE_MOUNTS:
                usage, free = get_usage(mount)
                mount_name = mount.strip('/').replace('/', '_') or 'root'
                publish(f"{TOPIC_PREFIX}/host/{mount_name}_usage", usage)
                log(f"{mount}: {usage}% used, {free} free", "INFO")
                if mount == "/" and usage > MD1_USAGE_THRESHOLD:
                    send_alert("md1 Capacity Critical", f"md1 at {usage}%, only {free} free")

            log("Checking disk info", "DEBUG")
            if current_time - last_smart_check >= SMART_INTERVAL:
                for disk in DEVICES:
                    disk_cache[disk] = get_disk_info(disk, full=True)
                last_smart_check = current_time
            else:
                for disk in DEVICES:
                    if disk_cache.get(disk, {}).get("state") != "active":
                        disk_cache[disk] = get_disk_info(disk, full=False)
            for disk in DEVICES:
                disk_name = disk.split('/')[-1]
                info = disk_cache[disk]
                for key in ["state", "temperature", "reallocated_sectors", "spinup_count"]:
                    publish(f"{TOPIC_PREFIX}/disk/{disk_name}/{key}", info.get(key, "N/A"))
                log(f"{disk}: state={info['state']}, temp={info.get('temperature', 'N/A')}°C", "INFO")

            log("Loop cycle complete", "DEBUG")
            time.sleep(SLEEP_INTERVAL)
        except Exception as e:
            log(f"Main loop error: {e}", "ERROR")
            time.sleep(ERROR_SLEEP)

if __name__ == "__main__":
    main()
