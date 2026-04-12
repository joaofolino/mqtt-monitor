# Setup Guide for MQTT Monitor

## Installation

1. Download the latest release:

```bash
wget https://github.com/joaofolino/mqtt-monitor/releases/latest/download/mqtt-monitor_*.deb
sudo apt install ./mqtt-monitor_*.deb
```

2. Verify the service:

```bash
sudo systemctl status mqtt-monitor.service
```

## Configuration

Edit `/etc/mqtt-monitor/mqtt-monitor.conf`.

The main areas to review are:

- MQTT broker access in `[mqtt]`
- any explicitly pinned disks, arrays, or mount points in `[internals]`
- discovery behavior in `[discovery]`
- Home Assistant dashboard and notification options in `[home_assistant]`

Minimal example:

```ini
[monitor]
enable_email = false
package_version = latest

[mqtt]
broker = 10.0.0.120
port = 1883
mqtt_user = mqtt-user
mqtt_pass = mqtt-password
self_name = server

[internals]
topic_prefix = home/server/mqtt-monitor
devices = /dev/sda,/dev/sdb,/dev/nvme0n1,/dev/md1
raid_arrays = md0,md1
disk_usage_mounts = /,/var/log

[discovery]
auto_discover_disks = true
auto_discover_raid_arrays = true
auto_discover_mounts = true
auto_discover_lxc = true

[home_assistant]
generate_dashboard = true
dashboard_output_path = /var/lib/mqtt-monitor/home-assistant-dashboard.yaml
```

For the full packaged template, see [`src/mqtt-monitor.conf`](../src/mqtt-monitor.conf).

## Discovery Strategy

V2 can auto-discover:

- disks
- RAID arrays
- mount points
- LXC containers through the packaged privileged helper

Good defaults for most hosts are to leave discovery enabled and only pin or ignore the exceptions.

Two especially useful options are:

- `ignored_devices` to skip pseudo devices
- `expected_lxc_containers` to declare which containers should raise issues if they stop

If `expected_lxc_containers` is empty, V2 can still learn expected containers from autostarted or previously running LXC containers during runtime.

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

The monitor uses MQTT discovery, so Home Assistant does not need manual YAML sensor definitions for the core entities.

V2 adds two setup conveniences:

- a generated dashboard YAML file at `dashboard_output_path`
- optional notifications through the Home Assistant REST API
- MQTT discovery payloads with `state_class` for key monitor time-series sensors

If you want to use the generated dashboard as-is, add the required custom card resources in Home Assistant:

HACS is the simplest path:

- install `Mini Graph Card`
- install `Plotly Graph Card`

If you prefer a manual install, copy the release bundles into `/config/www` first:

```bash
mkdir -p /config/www
wget -O /config/www/mini-graph-card-bundle.js \
  https://github.com/kalkih/mini-graph-card/releases/download/v0.13.0/mini-graph-card-bundle.js
wget -O /config/www/plotly-graph-card.js \
  https://github.com/dbuezas/lovelace-plotly-graph-card/releases/download/v3.3.5/plotly-graph-card.js
```

Then register the local resources:

```yaml
lovelace:
  resources:
    - url: /local/mini-graph-card-bundle.js
      type: module
    - url: /local/plotly-graph-card.js
      type: module
```

To enable notifications, set:

- `url`
- `token`
- optionally `notify_service`
- optionally `display_name` if you want Home Assistant to show a host-friendly name across entity pickers and history

Persistent notifications can be used on their own, even without a mobile `notify` service.

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

The existing config is preserved, and new packaged options are merged into the local config during package upgrades.
