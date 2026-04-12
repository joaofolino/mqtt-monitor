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
- **Missing dependencies**: Ensure `smartmontools`, `python3-pip`, etc., are installed.
- **Config errors**: Verify `/etc/mqtt-monitor/mqtt-monitor.conf` syntax.
- **Device permissions**: Confirm the packaged service file is present and `systemd` reloaded it after upgrades.

## MQTT Not Publishing

- Check broker connectivity:
  ```bash
  mosquitto_sub -h <broker> -u <user> -P <pass> -t 'test'
  ```
- Ensure `broker`, `port`, `mqtt_user`, `mqtt_pass` are correct.

## Alerts Not Sending

- Verify `enable_email = true` and `smtp_*` settings.
- Test SMTP:
  ```bash
  python3 -c "import smtplib; smtplib.SMTP('smtp.gmail.com', 587).starttls()"
  ```

## Config Not Updating

- Check `/etc/mqtt-monitor/mqtt-monitor.conf_old` for backups.
- Compare with `/etc/mqtt-monitor/mqtt-monitor.conf_new` for new options.

## Home Assistant Entities Have `_2` Suffixes

That usually means Home Assistant already had stale entities occupying the original entity IDs.

Options:

- remove the stale MQTT discovery entities from Home Assistant and let discovery recreate them cleanly
- or keep them and update your dashboard YAML to the entity IDs Home Assistant actually created

The example dashboard in this repo assumes a clean install with unsuffixed entity IDs.

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

Then confirm the arrays listed in `raid_arrays` match the real array names on the host.

## Dashboard Looks Empty

Usually one of these is true:

- the MQTT integration is not configured in Home Assistant
- the monitor is publishing to a different broker or topic prefix than expected
- the dashboard example uses different entity IDs than your instance created

Check Home Assistant Developer Tools first, then compare with the example files under `examples/home-assistant/`.
