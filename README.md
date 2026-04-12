# MQTT Monitor

A Python service for Linux hosts that monitors:

- disk SMART and NVMe health
- Linux software RAID state
- host CPU, memory, and temperature
- filesystem usage
- per-disk I/O rates

It publishes the results to MQTT, registers Home Assistant entities through MQTT
discovery, and can optionally send email alerts for severe conditions.

## Docs

- [Setup Guide](docs/setup.md)
- [Architecture](docs/architecture.md)
- [Home Assistant Integration](docs/home-assistant.md)
- [Troubleshooting Guide](docs/troubleshooting.md)

## Installation

See [Setup Guide](docs/setup.md).

## Configuration

Edit `/etc/mqtt-monitor/mqtt-monitor.conf`. See [Setup Guide](docs/setup.md) for details.

## Troubleshooting

See [Troubleshooting Guide](docs/troubleshooting.md).

## Development

- Branch: `master`
- PRs: Validated with linting and tests
- Releases: Tagged with `vX.Y.Z-N` (e.g., `v1.0.1-2`)

## Home Assistant Examples

Example Home Assistant files live under [`examples/home-assistant`](examples/home-assistant):

- `configuration.yaml` for dashboard registration
- `dashboard/mqtt-monitor-dashboard.yaml` for a starting Lovelace dashboard
