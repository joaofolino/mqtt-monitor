# MQTT Monitor

`mqtt-monitor` is a Debian-packaged monitoring agent for Linux hosts and Home Assistant.

It now ships with a V2 runtime that focuses on user-visible outcomes instead of raw metrics alone:

- automatic discovery of disks, RAID arrays, mount points, and LXC containers
- first-class issues with start and resolve tracking
- Home Assistant MQTT discovery for metrics, containers, and issue state
- Home Assistant `state_class` metadata for graph-friendly time-series sensors
- optional Home Assistant notifications for new and resolved issues
- automatic generation of a Lovelace dashboard YAML file

## What It Monitors

- disk SMART and NVMe health
- Linux software RAID state
- host CPU, memory, and temperature
- filesystem usage
- per-disk I/O rates
- LXC container state

## Docs

- [Setup Guide](docs/setup.md)
- [Architecture](docs/architecture.md)
- [Home Assistant Integration](docs/home-assistant.md)
- [Troubleshooting Guide](docs/troubleshooting.md)

## Home Assistant Examples

Repo examples live under [`examples/home-assistant`](examples/home-assistant):

- `configuration.yaml` for dashboard registration
- `dashboard/mqtt-monitor-dashboard.yaml` for a starter Lovelace view

At runtime, V2 can also generate a dashboard automatically at the configured `dashboard_output_path`.
The generated dashboard uses `mini-graph-card` for live sparklines and `lovelace-plotly-graph-card` for longer trend views.
Install those cards through HACS or copy the release bundles into `/config/www` and register them as `/local/mini-graph-card-bundle.js` and `/local/plotly-graph-card.js`.

## Development

- Branch: `master`
- PRs: validated with linting and tests
- Releases: tagged with `vX.Y.Z-N` (for example `v1.0.1-2`)
