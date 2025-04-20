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
