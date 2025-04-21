import pytest
import paho.mqtt.client as mqtt
import logging
import subprocess
import unittest
import os
from unittest.mock import patch, MagicMock
from mqtt_monitor import get_disk_info, get_raid_status, get_disk_io, get_usage, send_alert, log, run_cmd, check_package_version, disk_cache

@pytest.fixture
def setup_mdstat(tmp_path):
    mdstat_path = "/app/tests/mdstat"
    def _write_mdstat(content):
        with open(mdstat_path, "w") as f:
            f.write(content)
    return _write_mdstat

class TestMqttMonitor:
    @patch('mqtt_monitor.run_cmd')
    def test_get_disk_info_smart_passed(self, mock_run_cmd):
        mock_run_cmd.side_effect = [
            "4294967296",  # blockdev --getsize64 /dev/sda
            "SMART overall-health self-assessment test result: PASSED\nTemperature: 33\nPower_On_Hours: 1000\nReallocated_Sector_Ct: 0\nSpin_Up_Time: 0"
        ]
        result = get_disk_info('/dev/sda', full=True)
        assert result['health'] == 'OK'
        assert result['temperature'] == '33'
        assert result['power_on_hours'] == '1000'
        assert result['reallocated_sectors'] == '0'
        assert result['spinup_count'] == '0'
        assert result['capacity'] == 4

    @patch('mqtt_monitor.run_cmd')
    def test_get_disk_info_capacity(self, mock_run_cmd):
        mock_run_cmd.return_value = "4294967296"  # 4GB
        result = get_disk_info('/dev/sda', full=False)
        assert result['capacity'] == 4

    def test_get_raid_status(self, setup_mdstat):
        setup_mdstat("""
        md0 : active raid1 sda[0] sdb[1]
              1048576 blocks [2/2] [UU]
        """)
        result = get_raid_status()
        assert result['md0']['state'] == 'active'

    def test_get_raid_status_degraded(self, setup_mdstat):
        setup_mdstat("""
        md0 : active raid1 sda[0]
              1048576 blocks [2/1] [U_]
        """)
        result = get_raid_status()
        assert result['md0']['state'] == 'degraded'

    @patch('mqtt_monitor.psutil.disk_io_counters')
    def test_get_disk_io(self, mock_io_counters):
        mock_io_counters.return_value = {'sda': MagicMock(read_bytes=1000, write_bytes=2000)}
        reads, writes = get_disk_io('sda')
        assert reads == 1000
        assert writes == 2000

    @patch('mqtt_monitor.run_cmd')
    @patch('mqtt_monitor.psutil.disk_usage')
    def test_get_usage(self, mock_disk_usage, mock_run_cmd):
        mock_run_cmd.return_value = """
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1       100G   50G   50G  50% /
"""
        mock_disk_usage.return_value = MagicMock(percent=75, free=1073741824)  # 1GB free
        result = get_usage('/')
        assert result == (50, '50G')

    @patch('smtplib.SMTP')
    def test_send_alert_enabled(self, mock_smtp):
        global ENABLE_EMAIL
        ENABLE_EMAIL = True
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__.return_value = mock_server
        send_alert("Test Alert", "Test Body")
        mock_smtp.assert_called_with('smtp.example.com', 587)
        mock_server.starttls.assert_called()
        mock_server.login.assert_called_with('test_user', 'test_password')
        mock_server.sendmail.assert_called()

    @patch('mqtt_monitor.send_alert')
    def test_send_alert_disabled(self, mock_send_alert):
        global ENABLE_EMAIL
        ENABLE_EMAIL = False
        send_alert("Test Alert", "Test Body")
        mock_send_alert.assert_not_called()

    @patch('logging.Logger.info')
    def test_log_info(self, mock_log):
        log("Test info", "INFO")
        mock_log.assert_called_with("Test info")

    @patch('logging.Logger.debug')
    def test_log_debug_disabled(self, mock_log):
        global LOG_LEVEL
        LOG_LEVEL = "INFO"
        with patch('logging.getLogger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_logger.getEffectiveLevel.return_value = logging.INFO
            log("Test debug", "DEBUG")
            mock_logger.debug.assert_not_called()

    @patch('subprocess.run')
    def test_run_cmd_success(self, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(stdout="success")
        result = run_cmd(["ls"])
        assert result == "success"

    @patch('subprocess.run')
    def test_run_cmd_failure(self, mock_subprocess_run):
        mock_subprocess_run.side_effect = subprocess.SubprocessError("error")
        result = run_cmd(["ls"])
        assert result == ""

    @patch('mqtt_monitor.requests.get')
    @patch('mqtt_monitor.run_cmd')
    @patch('subprocess.run')
    def test_check_package_version_latest(self, mock_subprocess_run, mock_run_cmd, mock_requests):
        mock_requests.return_value.json.return_value = {'tag_name': 'v1.0-1'}
        mock_run_cmd.return_value = "0.9-1"
        check_package_version()
        mock_requests.assert_called_with("https://api.github.com/repos/joaofolino/mqtt-monitor/releases/latest")
        mock_subprocess_run.assert_called()

    @patch('mqtt_monitor.psutil.disk_io_counters')
    def test_get_disk_io_no_data(self, mock_io_counters):
        mock_io_counters.return_value = {}
        reads, writes = get_disk_io('sda')
        assert reads == 0
        assert writes == 0

    @patch('mqtt_monitor.run_cmd')
    @patch('mqtt_monitor.psutil.disk_usage')
    def test_get_usage_fallback(self, mock_disk_usage, mock_run_cmd):
        mock_run_cmd.return_value = ""
        mock_disk_usage.return_value = MagicMock(percent=75, free=1073741824)  # 1GB free
        result = get_usage('/')
        assert result == (75, "1G")

    @patch('mqtt_monitor.run_cmd')
    @patch('mqtt_monitor.send_alert')
    def test_disk_failure_alert(self, mock_send_alert, mock_run_cmd):
        mock_run_cmd.side_effect = [
            "4294967296",  # blockdev --getsize64 /dev/sda
            "SMART overall-health self-assessment test result: FAILED\nTemperature: 50\nReallocated_Sector_Ct: 10\nSpin_Up_Time: 5"
        ]
        result = get_disk_info('/dev/sda', full=True)
        assert result['health'] == 'FAILED'
        mock_send_alert.assert_called_with("Disk /dev/sda Failed", unittest.mock.ANY)

    def test_mqtt_publish_integration(self):
        client = mqtt.Client()
        client.username_pw_set("test", "test")
        client.connect("localhost", 1883, 60)
        client.loop_start()
        client.publish("test/mqtt-monitor/test", "test_payload")
        client.loop_stop()
        client.disconnect()

    def test_get_raid_status_empty(self, setup_mdstat):
        setup_mdstat("")
        result = get_raid_status()
        assert result == {"md0": {"state": "unknown", "raw": ""}}

    @patch('mqtt_monitor.run_cmd')
    def test_get_disk_info_no_smart(self, mock_run_cmd):
        mock_run_cmd.side_effect = ["4294967296", ""]
        result = get_disk_info('/dev/sda', full=True)
        assert result['health'] == 'N/A'
        assert result['capacity'] == 4

    @patch('mqtt_monitor.run_cmd')
    def test_get_disk_info_invalid_capacity(self, mock_run_cmd):
        disk_cache.clear()  # Clear cache to avoid interference
        mock_run_cmd.return_value = "invalid"
        result = get_disk_info('/dev/sda', full=False)
        assert result['capacity'] == 0

    def test_get_raid_status_invalid(self, setup_mdstat):
        setup_mdstat("")  # Simulate file exists but is empty or invalid
        result = get_raid_status()
        assert result == {"md0": {"state": "unknown", "raw": ""}}

    @patch('mqtt_monitor.psutil.disk_io_counters')
    def test_get_disk_io_error(self, mock_io_counters):
        mock_io_counters.side_effect = Exception("io error")
        reads, writes = get_disk_io('sda')
        assert reads == 0
        assert writes == 0

    @patch('subprocess.run')
    def test_run_cmd_timeout(self, mock_subprocess_run):
        mock_subprocess_run.side_effect = subprocess.TimeoutExpired(cmd=["ls"], timeout=10)
        result = run_cmd(["ls"])
        assert result == ""

    @patch('mqtt_monitor.requests.get')
    def test_check_package_version_error(self, mock_requests):
        mock_requests.side_effect = Exception("network error")
        result = check_package_version()
        assert result == False

    @patch('mqtt_monitor.run_cmd')
    def test_get_usage_error(self, mock_run_cmd):
        mock_run_cmd.side_effect = Exception("df error")
        result = get_usage('/')
        assert result == (0, "N/A")

    @patch('mqtt_monitor.run_cmd')
    def test_get_disk_info_md1(self, mock_run_cmd):
        mock_run_cmd.return_value = "4294967296"
        result = get_disk_info('/dev/md1', full=True)
        assert result['health'] == 'N/A'
        assert result['capacity'] == 4
