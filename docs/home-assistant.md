# Home Assistant Integration

`mqtt-monitor` integrates with Home Assistant through MQTT discovery.

That means the monitor publishes discovery payloads automatically and Home Assistant creates entities from them without manual sensor YAML.

This guide covers:

- the minimum Home Assistant setup required
- how to use the example YAML files in this repo
- how to adapt the dashboard example for your own disks and RAID arrays

## What Home Assistant Needs

At minimum, Home Assistant needs:

1. a working MQTT integration connected to the same broker as `mqtt-monitor`
2. the monitored host publishing discovery and state topics

On a clean setup, once the monitor starts, Home Assistant should create entities like:

- `sensor.server_monitor_server_cpu_load_percentage`
- `sensor.server_monitor_server_root_disk_usage_percentage`
- `sensor.server_monitor_disk_sda_max_temperature`
- `binary_sensor.server_monitor_disk_sda_disk_failing`
- `sensor.server_monitor_raid_md0_status`

## Repo Examples

This repo includes Home Assistant examples under [`examples/home-assistant`](../examples/home-assistant):

- [`configuration.yaml`](../examples/home-assistant/configuration.yaml)
- [`dashboard/mqtt-monitor-dashboard.yaml`](../examples/home-assistant/dashboard/mqtt-monitor-dashboard.yaml)

The examples are intentionally conservative:

- built with standard Lovelace cards
- no custom cards required
- assume a clean Home Assistant install with unsuffixed entity IDs

## Clean Install Vs Existing Home Assistant

On a clean Home Assistant instance, example entity IDs should match exactly.

On an existing Home Assistant instance, stale entities may already occupy the base names. In that case, Home Assistant may create suffixed entity IDs such as:

- `sensor.server_monitor_disk_sda_max_temperature_2`
- `sensor.server_monitor_server_cpu_load_percentage_2`

If that happens, either:

- clean up the old MQTT discovery entities and reuse the unsuffixed names
- or adjust the example dashboard YAML to the entity IDs that Home Assistant actually created

## Recommended Setup Flow

### 1. Configure MQTT in Home Assistant

If you already configured MQTT through the Home Assistant UI, keep using that.

If you prefer YAML-managed MQTT, use the commented example in [`configuration.yaml`](../examples/home-assistant/configuration.yaml) as a starting point.

### 2. Install and configure `mqtt-monitor`

On the monitored host:

1. install the Debian package
2. edit `/etc/mqtt-monitor/mqtt-monitor.conf`
3. point it at the same MQTT broker used by Home Assistant
4. restart the service

Verify MQTT activity before touching dashboards:

```bash
mosquitto_sub -h <broker> -u <user> -P <password> -t 'home/server/mqtt-monitor/#'
```

### 3. Register the YAML dashboard

If you want a separate YAML dashboard, add the `lovelace_dashboards:` block from [`configuration.yaml`](../examples/home-assistant/configuration.yaml) to your Home Assistant configuration.

That example registers a dashboard file at:

- `dashboards/mqtt-monitor-dashboard.yaml`

### 4. Copy the dashboard example

Copy [`dashboard/mqtt-monitor-dashboard.yaml`](../examples/home-assistant/dashboard/mqtt-monitor-dashboard.yaml) into your Home Assistant config directory under `dashboards/`.

### 5. Reload Home Assistant

Reload Lovelace, or restart Home Assistant if needed.

## What The Example Dashboard Assumes

The example dashboard assumes:

- host metrics are enabled
- mount usage is tracked for `/` and `/var/log`
- disks `sda` and `sdb` exist
- arrays `md0` and `md1` exist

That matches the packaged sample config better than a more complex real-world setup.

If your system has more devices, duplicate the disk cards and replace the device names.

Examples:

- change `sda` to `nvme0n1`
- change `md0` to `md2`
- add another entities card for `sdc`

## How To Extend The Dashboard

Good next steps for a custom dashboard are:

- add cards for every disk listed in `devices`
- add more RAID cards for every array in `raid_arrays`
- add history graphs for write spikes and busy time
- add conditional or filtered cards for only active issues
- add Home Assistant automations based on the discovered entities

## Entity Families Created By Discovery

### Host sensors

- CPU load percentage
- memory used megabytes
- CPU temperature celsius
- per-mount disk usage percentages

### Disk sensors

- SMART health status
- max temperature
- reallocated sectors
- spin up count
- read and write throughput
- read and write operation rates
- busy time percentage

### Disk binary sensors

- disk failing
- SMART command failed
- device open failed
- command line parse error
- prefail threshold passed
- past threshold passed
- error log flags
- self-test log flags

### RAID sensors

- one status sensor per array

## Notes About Availability

Disk entities are wired to MQTT availability topics.

That means Home Assistant will mark them unavailable when the monitor reports a disk as offline. This is expected and makes dashboards much easier to reason about than publishing placeholder strings for a dead device.

## Troubleshooting

If discovery or the dashboard does not look right:

- verify MQTT discovery topics are present under `homeassistant/...`
- verify state topics are present under your `topic_prefix`
- confirm entity names in Developer Tools before trusting the example YAML
- check for stale suffixed entities from earlier experiments

See also [troubleshooting.md](troubleshooting.md).
