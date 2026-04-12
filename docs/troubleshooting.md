# Troubleshooting MQTT Monitor

## Service Not Starting

Check status:

```bash
sudo systemctl status mqtt-monitor.service
```

View logs:

```bash
tail -f /var/log/mqtt-monitor.log
```

Common issues:

- config syntax problems in `/etc/mqtt-monitor/mqtt-monitor.conf`
- missing runtime tools such as `smartctl`
- device permission problems
- an old service unit still loaded by `systemd`

## MQTT Not Publishing

Check broker connectivity:

```bash
mosquitto_sub -h <broker> -u <user> -P <pass> -t 'test'
```

Then verify:

- `broker`, `port`, `mqtt_user`, and `mqtt_pass`
- `topic_prefix`
- Home Assistant is pointed at the same broker

## Home Assistant Notifications Not Working

Verify:

- `url` points to the correct Home Assistant base URL
- `token` is a valid long-lived access token
- `notify_service` is a real `notify.*` service if you configured one
- `notification_minimum_severity` is not filtering out the issue you expect to see

Persistent notifications can still work even when `notify_service` is blank.

## Config Not Updating

Compare:

- `/etc/mqtt-monitor/mqtt-monitor.conf`
- `/etc/mqtt-monitor/mqtt-monitor.conf_old`
- `/etc/mqtt-monitor/mqtt-monitor.conf_new`

Package upgrades preserve the current config and merge in missing packaged options.

## Dashboard Looks Empty

Usually one of these is true:

- the MQTT integration is not configured in Home Assistant
- the monitor is publishing to a different broker or topic prefix than expected
- the dashboard YAML references entity IDs that do not match your instance

Check Home Assistant Developer Tools first, then compare against:

- the generated dashboard file from `dashboard_output_path`
- or the example files under `examples/home-assistant/`

## Entities Have `_2` Suffixes

That usually means Home Assistant already had stale MQTT entities occupying the original IDs.

Options:

- remove the stale entities and let discovery recreate them cleanly
- or keep them and update the dashboard YAML to the entity IDs Home Assistant actually created

## NVMe SMART Shows As Unavailable

The packaged service needs `CAP_SYS_ADMIN` and `CAP_SYS_RAWIO` to read NVMe SMART data while still running as the dedicated `mqtt-monitor` user.

Check the service definition:

```bash
systemctl show -p AmbientCapabilities -p CapabilityBoundingSet mqtt-monitor
```

Version `1.0.1-2` and newer include those capabilities in the packaged service file.

## RAID Status Looks Wrong

Check what the host sees in `/proc/mdstat`:

```bash
cat /proc/mdstat
```

Then confirm:

- the array names are correct
- discovery is enabled if you expect auto-detection
- the array is not being filtered out by old static config assumptions

## Containers Are Missing

Verify that `lxc-ls` works on the monitored host and that:

- `auto_discover_lxc = true`
- or `expected_lxc_containers` includes the containers you care about

Autostarted containers are also treated as expected-running resources by V2.

## State Or Dashboard Files Are Missing

By default V2 writes runtime artifacts under:

- `/var/lib/mqtt-monitor`

Check:

```bash
ls -la /var/lib/mqtt-monitor
```

That directory is created by the package post-install step. If you changed `state_dir` or `dashboard_output_path`, verify permissions there too.
