# Setup Guide for MQTT Monitor

## Installation

1. **Download the latest release**:
   ```bash
   wget https://github.com/joaofolino/mqtt-monitor/releases/latest/download/mqtt-monitor_*.deb
   sudo apt install ./mqtt-monitor_*.deb
   ```

2. **Verify service**:
   ```bash
   sudo systemctl status mqtt-monitor.service
   ```

## Configuration

Edit `/etc/mqtt-monitor/mqtt-monitor.conf`:
- **MQTT Broker**: Set `broker`, `port`, `mqtt_user`, `mqtt_pass`.
- **Devices**: List devices in `devices` (e.g., `/dev/sda,/dev/sdb`).
- **Alerts**: Enable `enable_email` and configure `smtp_*` settings.

Example:
```ini
[monitor]
enable_email = true
alert_email = user@example.com
[mqtt]
broker = 10.0.0.120
port = 1883
[internals]
devices = /dev/sda,/dev/sdb
```

## Starting the Service

```bash
sudo systemctl restart mqtt-monitor.service
tail -f /var/log/mqtt-monitor.log
```

## Verifying MQTT

Subscribe to topics:
```bash
mosquitto_sub -h 10.0.0.120 -u mqtt-user -P mqtt-password -t 'home/server/mqtt-monitor/#'
```

## Upgrading

Edit `package_version` in `/etc/mqtt-monitor/mqtt-monitor.conf`:
```ini
package_version = 2.0-1
```
Restart the service:
```bash
sudo systemctl restart mqtt-monitor.service
```

The existing config is preserved, with new options added from the packaged config.
