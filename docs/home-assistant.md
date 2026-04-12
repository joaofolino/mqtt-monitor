# Home Assistant Integration

`mqtt-monitor` integrates with Home Assistant in two ways:

- MQTT discovery for entities
- optional Home Assistant service calls for notifications

## What V2 Adds

Compared with the original prototype-oriented setup, V2 now gives Home Assistant:

- host, disk, RAID, filesystem, and LXC container entities
- measurement-style `state_class` metadata for time-series monitor sensors
- an active-issue count sensor
- a highest-issue-severity sensor
- per-issue binary sensors
- a generated dashboard YAML file

## Minimum Requirements

At minimum, Home Assistant needs:

1. a working MQTT integration connected to the same broker as `mqtt-monitor`
2. the monitor publishing discovery and state topics
3. the dashboard resources for `mini-graph-card` and `plotly-graph` if you want the generated V2 dashboard to render fully

For notifications, it also needs:

4. a valid Home Assistant base URL and long-lived access token in `mqtt-monitor.conf`

## Common Entity IDs

On a clean setup, V2 should create stable entity IDs such as:

- `sensor.server_cpu_load_percentage`
- `sensor.server_root_disk_usage_percentage`
- `sensor.server_active_issue_count`
- `sensor.server_highest_issue_severity`
- `sensor.raid_md0_status`
- `binary_sensor.server_container_web_running`
- `sensor.disk_sda_smart_health_status`

If your Home Assistant instance already has stale entities occupying those names, Home Assistant may suffix them with `_2`.

## Dashboard Options

### Option 1: Use the generated dashboard

V2 can write a dashboard YAML file automatically to the configured `dashboard_output_path`.

Default:

- `/var/lib/mqtt-monitor/home-assistant-dashboard.yaml`

This is the easiest path if you want the monitor to own the dashboard shape.

### Option 2: Use the repo example

Example files live under [`examples/home-assistant`](../examples/home-assistant):

- [`configuration.yaml`](../examples/home-assistant/configuration.yaml)
- [`dashboard/mqtt-monitor-dashboard.yaml`](../examples/home-assistant/dashboard/mqtt-monitor-dashboard.yaml)

This is useful if you want a starting point you can copy into your Home Assistant config and then customize manually.

## Registering A YAML Dashboard

If you want a separate YAML dashboard, use the `lovelace_dashboards:` example from [`configuration.yaml`](../examples/home-assistant/configuration.yaml).

Then point `filename` at either:

- the generated dashboard file you copy into Home Assistant
- or the example dashboard checked into this repo

## Notifications

V2 can notify Home Assistant when issues start and resolve.

Relevant config options:

- `display_name`
- `url`
- `token`
- `notify_service`
- `persistent_notifications`
- `notify_on_new_issue`
- `notify_on_resolve`
- `notification_minimum_severity`

Two supported patterns are:

- persistent notifications only
- persistent notifications plus a `notify.*` service such as a mobile app

## Recommended Setup Flow

1. Configure Home Assistant MQTT against the same broker as `mqtt-monitor`.
2. Start or restart `mqtt-monitor`.
3. Confirm the discovered entities appear in Home Assistant.
4. Decide whether you want the generated dashboard or the repo example.
5. If using notifications, add `url` and `token` and restart the service.

## Dashboard Shape

The generated dashboard focuses on:

- host overview
- active issues
- short-range sparkline cards for live signals, storage, disk heat, and disk load
- RAID arrays
- expected versus optional LXC containers
- Plotly detail graphs for 24h and 7d trends
- one detailed card per discovered disk

The generated dashboard expects these Lovelace resources:

- `mini-graph-card`
- `lovelace-plotly-graph-card`

## Troubleshooting

If discovery or notifications do not look right:

- verify MQTT discovery topics under `homeassistant/...`
- verify state topics under your configured `topic_prefix`
- verify the HA token works against `/api/`
- confirm the actual entity IDs in Developer Tools before editing dashboard YAML

See also [troubleshooting.md](troubleshooting.md).
