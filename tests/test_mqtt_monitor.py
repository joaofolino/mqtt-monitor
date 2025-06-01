import unittest

import pytest
import paho.mqtt.client as mqtt
import logging
import subprocess
import os
from unittest.mock import patch, MagicMock

from mqtt_monitor import (
    get_disk_info, get_raid_status, get_disk_io, get_usage, send_alert, log,
    run_cmd, check_package_version, disk_info_cache, setup_logging, MonitorConfig, publish_data
)

@pytest.fixture
def setup_mdstat(tmp_path):
    mdstat_path = os.getenv('CONFIG_PATH', './tests') + "/mdstat"
    os.makedirs(os.path.dirname(mdstat_path), exist_ok=True)
    def _write_mdstat(content):
        with open(mdstat_path, "w") as f:
            f.write(content)
    return _write_mdstat

@pytest.fixture
def setup_monitor(tmp_path):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    # Create a test config
    config = MonitorConfig(
        enable_email=True,
        log_level="DEBUG",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="test_user",
        smtp_pass="test_password",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=dir_path,
        log_file="mqtt-monitor.log",
        devices=["/dev/sda", "/dev/sdb"],
        raid_arrays=["md0"],
        mdstat_path=os.path.join(dir_path, "tests/mdstat"),
        cpu_temp_path="/sys/class/thermal/thermal_zone0/temp",
        disk_usage_mounts=["/"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0
    )
    # Setup logging
    logger = setup_logging(config)
    # Clear global state
    disk_info_cache.clear()
    yield config, logger
    # Teardown
    disk_info_cache.clear()
    logger.handlers = []

class TestMqttMonitor:
    def test_disk_failure_alert(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.run_cmd') as mock_run_cmd, patch('mqtt_monitor.send_alert') as mock_send_alert:
            def run_cmd_side_effect(config, logger, cmd):
                cmd_str = ' '.join(cmd)
                if 'blockdev --getsize64 /dev/sda' in cmd_str:
                    return "4294967296", 0
                if 'smartctl -a -d scsi /dev/sda' in cmd_str:
                    return "SMART overall-health self-assessment test result: FAILED\nTemperature: 50\nReallocated_Sector_Ct: 10\nSpin_Up_Time: 5", 0
                if 'smartctl -a -d auto /dev/sda' in cmd_str:
                    return "", -1
                return "", -1
            mock_run_cmd.side_effect = run_cmd_side_effect
            result = get_disk_info(config, logger, '/dev/sda', full=True)
            assert result['smart_health_status'] == 'FAILED'
            mock_send_alert.assert_called_with(config, logger, "Disk /dev/sda SMART Failure", unittest.mock.ANY)

    def test_get_disk_info_smart_passed(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.run_cmd') as mock_run_cmd:
            mock_run_cmd.side_effect = [
                ("4294967296", 0),
                ("SMART overall-health self-assessment test result: PASSED\nTemperature: 33\nPower_On_Hours: 1000\nReallocated_Sector_Ct: 0\nSpin_Up_Time: 0", 0)
            ]
            result = get_disk_info(config, logger, '/dev/sda', full=True)
            assert result['smart_health_status'] == 'PASSED'
            assert result['max_temperature'] == 33
            assert result['power_on_hours'] == 1000
            assert result['reallocated_sectors'] == 0
            assert result['spin_up_count'] == 0
            assert result['capacity'] == 4

    def test_get_disk_info_capacity(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.run_cmd') as mock_run_cmd:
            mock_run_cmd.return_value = ("4294967296", 0)
            result = get_disk_info(config, logger, '/dev/sda', full=False)
            assert result['capacity'] == 4

    def test_get_raid_status(self, setup_monitor, setup_mdstat):
        config, logger = setup_monitor
        setup_mdstat("""
        md0 : active raid1 sda[0] sdb[1]
              1048576 blocks [2/2] [UU]
        """)
        result = get_raid_status(config, logger)
        assert result['md0']['status'] == 'active'

    def test_get_raid_status_degraded(self, setup_monitor, setup_mdstat):
        config, logger = setup_monitor
        setup_mdstat("""
        md0 : active raid1 sda[0]
              1048576 blocks [2/1] [U_]
        """)
        result = get_raid_status(config, logger)
        assert result['md0']['status'] == 'degraded'

    def test_get_disk_io(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.psutil.disk_io_counters') as mock_io_counters:
            mock_io_counters.return_value = {
                'sda': MagicMock(
                    read_bytes=1000,
                    write_bytes=2000,
                    read_count=100,
                    write_count=200,
                    read_time=500,
                    write_time=600,
                    busy_time=700,
                    read_merged_count=10,
                    write_merged_count=20
                )
            }
            result = get_disk_io(config, logger, 'sda')
            assert result['read_bytes'] == 1000
            assert result['write_bytes'] == 2000
            assert result['read_count'] == 100
            assert result['write_count'] == 200

    def test_get_usage(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.run_cmd') as mock_run_cmd, patch('mqtt_monitor.psutil.disk_usage') as mock_disk_usage:
            mock_run_cmd.return_value = (
                """
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1       100G   50G   50G  50% /
""", 0
            )
            mock_disk_usage.return_value = MagicMock(percent=75, free=1073741824)
            result = get_usage(config, logger, '/')
            assert result == (50, '50G')

    def test_send_alert_enabled(self, setup_monitor):
        config, logger = setup_monitor
        with patch('smtplib.SMTP') as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server
            send_alert(config, logger, "Test Alert", "Test Body")
            mock_smtp.assert_called_with('smtp.example.com', 587, timeout=10)
            mock_server.starttls.assert_called()
            mock_server.login.assert_called_with('test_user', 'test_password')
            mock_server.sendmail.assert_called()

    def test_send_alert_disabled(self, setup_monitor):
        config, logger = setup_monitor
        config.enable_email = False
        with patch('smtplib.SMTP') as mock_smtp:
            send_alert(config, logger, "Test Alert", "Test Body")
            mock_smtp.assert_not_called()

    def test_log_info(self, setup_monitor):
        config, logger = setup_monitor
        with patch.object(logger, 'info') as mock_log:
            log(config, logger, "Test info", "INFO")
            mock_log.assert_called_with("Test info")

    def test_log_debug_disabled(self, setup_monitor):
        config, logger = setup_monitor
        config.log_level = "INFO"
        logger.setLevel(logging.INFO)
        with patch.object(logger, 'debug') as mock_log:
            log(config, logger, "Test debug", "DEBUG")
            mock_log.assert_not_called()

    def test_run_cmd_success(self, setup_monitor):
        config, logger = setup_monitor
        with patch('subprocess.run') as mock_subprocess_run:
            mock_subprocess_run.return_value = MagicMock(stdout="success", returncode=0)
            result, exit_code = run_cmd(config, logger, ["ls"])
            assert result == "success"
            assert exit_code == 0

    def test_run_cmd_failure(self, setup_monitor):
        config, logger = setup_monitor
        with patch('subprocess.run') as mock_subprocess_run:
            mock_subprocess_run.side_effect = subprocess.SubprocessError("error")
            result, exit_code = run_cmd(config, logger, ["ls"])
            assert result == ""
            assert exit_code == -1

    def test_check_package_version_latest(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.requests.get') as mock_requests, patch('mqtt_monitor.run_cmd') as mock_run_cmd, patch('subprocess.run') as mock_subprocess_run:
            mock_requests.return_value.json.return_value = {'tag_name': 'v1.0.1'}
            mock_run_cmd.return_value = ("1.0.0", 0)
            mock_subprocess_run.return_value = MagicMock(returncode=0)
            result = check_package_version(config, logger)
            mock_requests.assert_called_with("https://api.github.com/repos/joaofolino/mqtt-monitor/releases/latest", timeout=10)
            mock_subprocess_run.assert_called()
            assert result is True

    def test_get_disk_io_no_data(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.psutil.disk_io_counters') as mock_io_counters:
            mock_io_counters.return_value = {}
            result = get_disk_io(config, logger, 'sda')
            assert result['read_bytes'] == 0
            assert result['write_bytes'] == 0

    def test_get_usage_fallback(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.run_cmd') as mock_run_cmd, patch('mqtt_monitor.psutil.disk_usage') as mock_disk_usage:
            mock_run_cmd.return_value = ("", -1)
            mock_disk_usage.return_value = MagicMock(percent=75, free=1073741824)
            result = get_usage(config, logger, '/')
            assert result == (75, '1G')

    def test_mqtt_publish_integration(self, setup_monitor):
        config, logger = setup_monitor
        client = MagicMock()
        client.connected = True
        messages = [{"topic": "test/mqtt-monitor/test", "payload": "test_payload", "retain": False}]
        with patch('mqtt_monitor.connect_mqtt') as mock_connect:
            publish_data(config, logger, client, messages)
            client.publish_multiple.assert_called_with(messages)

    def test_get_raid_status_empty(self, setup_monitor, setup_mdstat):
        config, logger = setup_monitor
        setup_mdstat("")
        result = get_raid_status(config, logger)
        assert result == {"md0": {"status": "unknown", "raw": ""}}

    def test_get_disk_info_no_smart(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.run_cmd') as mock_run_cmd:
            mock_run_cmd.side_effect = [("4294967296", 0), ("", -1)]
            result = get_disk_info(config, logger, '/dev/sda', full=True)
            assert result['smart_health_status'] is None
            assert result['capacity'] == 4

    def test_get_disk_info_invalid_capacity(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.run_cmd') as mock_run_cmd:
            mock_run_cmd.return_value = ("invalid", -1)
            result = get_disk_info(config, logger, '/dev/sda', full=False)
            assert result['capacity'] == 0

    def test_get_raid_status_invalid(self, setup_monitor, setup_mdstat):
        config, logger = setup_monitor
        setup_mdstat("")
        result = get_raid_status(config, logger)
        assert result == {"md0": {"status": "unknown", "raw": ""}}

    def test_get_disk_io_error(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.psutil.disk_io_counters') as mock_io_counters:
            mock_io_counters.side_effect = psutil.Error("io error")
            result = get_disk_io(config, logger, 'sda')
            assert result['read_bytes'] == 0
            assert result['write_bytes'] == 0

    def test_run_cmd_timeout(self, setup_monitor):
        config, logger = setup_monitor
        with patch('subprocess.run') as mock_subprocess_run:
            mock_subprocess_run.side_effect = subprocess.TimeoutExpired(cmd=["ls"], timeout=10)
            result, exit_code = run_cmd(config, logger, ["ls"])
            assert result == ""
            assert exit_code == -1

    def test_check_package_version_error(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.requests.get') as mock_requests:
            mock_requests.side_effect = requests.RequestException("network error")
            result = check_package_version(config, logger)
            assert result is False

    def test_get_usage_error(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.run_cmd') as mock_run_cmd, patch('mqtt_monitor.psutil.disk_usage') as mock_disk_usage:
            mock_run_cmd.side_effect = subprocess.SubprocessError("df error")
            mock_disk_usage.side_effect = psutil.Error("disk error")
            result = get_usage(config, logger, '/')
            assert result == (0, "N/A")

    def test_get_disk_info_md1(self, setup_monitor):
        config, logger = setup_monitor
        with patch('mqtt_monitor.run_cmd') as mock_run_cmd:
            mock_run_cmd.return_value = ("4294967296", 0)
            result = get_disk_info(config, logger, '/dev/md1', full=True)
            assert result['smart_health_status'] is None
            assert result['capacity'] == 4
