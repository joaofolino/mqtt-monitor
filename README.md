# MQTT Monitor

A Python script to monitor disk SMART attributes, RAID status, and system metrics (CPU, memory, disk usage), publishing to MQTT for integration with Home Assistant.

## Installation

```bash
# Download latest release
wget https://github.com/joaofolino/mqtt-monitor/releases/latest/download/mqtt-monitor_*.deb
sudo apt install ./mqtt-monitor_*.deb
```

## Configuration

Edit `/etc/mqtt-monitor/mqtt-monitor.conf` to set MQTT broker, devices, and other settings.

## Service

```bash
sudo systemctl start mqtt-monitor.service
sudo systemctl status mqtt-monitor.service
```

## Development

- Branch: `master`
- PRs: Validated with linting and tests
- Releases: Tagged with `vX.Y-Z` (e.g., `v1.0-1`)
