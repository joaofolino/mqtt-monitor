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
- **MQTT Broker**: Set `broker`, `port`, `mqtt_user`, `mqtt_pass`
- **Devices**: List devices in `devices` (for example `/dev/sda,/dev/sdb,/dev/nvme0n1,/dev/md1`)
- **RAID Arrays**: List Linux MD arrays in `raid_arrays`
- **Mount Usage**: Set `disk_usage_mounts`
- **Alerts**: Enable `enable_email` and configure `smtp_*` settings if you want email alerts
- **Upgrade policy**: Set `package_version` to `latest` or a specific release

Example:
```ini
[monitor]
enable_email = false
package_version = latest

[mqtt]
broker = 10.0.0.120
port = 1883
mqtt_user = mqtt-user
mqtt_pass = mqtt-password

[internals]
topic_prefix = home/server/mqtt-monitor
devices = /dev/sda,/dev/sdb
raid_arrays = md0
disk_usage_mounts = /,/var/log
```

For the full packaged template, see [`src/mqtt-monitor.conf`](../src/mqtt-monitor.conf).

## Starting The Service

```bash
sudo systemctl restart mqtt-monitor.service
sudo systemctl status mqtt-monitor.service
tail -f /var/log/mqtt-monitor.log
```

## Verifying MQTT

Subscribe to the state topics:
```bash
mosquitto_sub -h 10.0.0.120 -u mqtt-user -P mqtt-password -t 'home/server/mqtt-monitor/#'
```

You should also see discovery topics under:

```bash
mosquitto_sub -h 10.0.0.120 -u mqtt-user -P mqtt-password -t 'homeassistant/#'
```

## Home Assistant

The monitor uses MQTT discovery, so Home Assistant does not need manual sensor YAML for the core entities.

To connect it cleanly:

1. Configure the Home Assistant MQTT integration against the same broker.
2. Verify the discovered entities appear in Home Assistant.
3. Optionally register the example YAML dashboard from [`examples/home-assistant`](../examples/home-assistant).

See [Home Assistant Integration](home-assistant.md) for the full walkthrough.

## Upgrading

If `package_version = latest`, restarting the service allows it to compare the installed package with the latest GitHub release.

To pin a specific version instead:

```ini
[monitor]
package_version = 1.0.1-2
```

Then restart the service:

```bash
sudo systemctl restart mqtt-monitor.service
```

The existing config is preserved, with new options merged in from the packaged config when the package is upgraded.

## Architecture

For a complete description of the v1 design, see [architecture.md](architecture.md).
