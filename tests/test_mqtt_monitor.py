import unittest
from unittest.mock import patch, mock_open, MagicMock
import paho.mqtt.client as mqtt
import time
import subprocess
from src.mqtt_monitor import get_disk_info, get_raid_status, get_disk_io, get_usage, send_alert, log, run_cmd, check_package_version

class TestMqttMonitor(unittest.TestCase):
    @patch('mqtt_monitor.run_cmd')
    def test_get_disk_info_smart_passed(self, mock_run_cmd):
        mock_run_cmd.return_value = "PASSED\nTemperature: 33\nPower_On_Hours 1000\nReallocated_Sector_Ct 0"
        result = get_disk_info('/dev/sda', full=True)
        self.assertEqual(result['health'], 'OK')
        self.assertEqual(result['temperature'], '33')

    @patch('mqtt_monitor.run_cmd')
    def test_get_disk_info_no_smart(self, mock_run_cmd):
        mock_run_cmd.return_value = ""
        result = get_disk_info('/dev/sda', full=True)
        self.assertEqual(result['health'], 'N/A')
        self.assertEqual(result['state'], 'error')

    @patch('mqtt_monitor.run_cmd')
    def test_get_disk_info_capacity(self, mock_run_cmd):
        mock_run_cmd.return_value = "4294967296"  # 4GB
        result = get_disk_info('/dev/sda', full=False)
        self.assertEqual(result['capacity'], 4)

    @patch('builtins.open', new_callable=mock_open, read_data="md0 : active raid1 sda[0] sdb[1]\n      [UU]")
    def test_get_raid_status_active(self, mock_file):
        result = get_raid_status()
        self.assertEqual(result['md0']['state'], 'active')

    @patch('builtins.open', new_callable=mock_open, read_data="md0 : active raid1 sda[0]\n      [U_]")
    def test_get_raid_status_degraded(self, mock_file):
        result = get_raid_status()
        self.assertEqual(result['md0']['state'], 'degraded')

    @patch('psutil.disk_io_counters')
    def test_get_disk_io_valid(self, mock_io):
        mock_io.return_value = {'sda': MagicMock(read_bytes=1000, write_bytes=2000)}
        reads, writes = get_disk_io('sda')
        self.assertEqual(reads, 1000)
        self.assertEqual(writes, 2000)

    @patch('psutil.disk_io_counters')
    def test_get_disk_io_missing(self, mock_io):
        mock_io.return_value = {}
        reads, writes = get_disk_io('sda')
        self.assertEqual(reads, 0)
        self.assertEqual(writes, 0)

    @patch('mqtt_monitor.run_cmd')
    def test_get_usage_df(self, mock_run_cmd):
        mock_run_cmd.return_value = "Filesystem Size Used Avail Use% Mounted\n/dev/sda 100G 50G 50G 50% /"
        percent, free = get_usage('/')
        self.assertEqual(percent, 50)
        self.assertEqual(free, '50G')

    @patch('psutil.disk_usage')
    def test_get_usage_psutil(self, mock_usage):
        mock_usage.return_value = MagicMock(percent=75, free=5368709120)  # 5GB
        percent, free = get_usage('/var/log')
        self.assertEqual(percent, 75)
        self.assertEqual(free, '5G')

    @patch('smtplib.SMTP')
    def test_send_alert_success(self, mock_smtp):
        send_alert("Test Alert", "Test Body")
        mock_smtp.assert_not_called()  # Disabled by default

    @patch('smtplib.SMTP')
    def test_send_alert_enabled(self, mock_smtp):
        global ENABLE_EMAIL
        ENABLE_EMAIL = True
        send_alert("Test Alert", "Test Body")
        mock_smtp.assert_called()

    @patch('mqtt_monitor.run_cmd')
    def test_run_cmd_timeout(self, mock_run_cmd):
        mock_run_cmd.side_effect = subprocess.TimeoutExpired(cmd=['test'], timeout=10)
        result = run_cmd(['test'])
        self.assertEqual(result, "")

    @patch('logging.Logger.info')
    def test_log_info(self, mock_log):
        log("Test message", "INFO")
        mock_log.assert_called_with("Test message")

    @patch('logging.Logger.debug')
    def test_log_debug_disabled(self, mock_log):
        global LOG_LEVEL
        LOG_LEVEL = "INFO"
        log("Test debug", "DEBUG")
        mock_log.assert_not_called()

    @patch('requests.get')
    @patch('subprocess.run')
    def test_check_package_version_latest(self, mock_subprocess, mock_requests):
        mock_requests.return_value.json.return_value = {'tag_name': 'v2.0-1'}
        mock_subprocess.side_effect = [
            MagicMock(stdout='1.0-1'),  # Current version
            MagicMock(returncode=0),    # wget
            MagicMock(returncode=0)     # apt install
        ]
        global PACKAGE_VERSION
        PACKAGE_VERSION = "latest"
        result = check_package_version()
        self.assertTrue(result)

    @patch('requests.get')
    @patch('subprocess.run')
    def test_check_package_version_specific(self, mock_subprocess, mock_requests):
        mock_subprocess.side_effect = [
            MagicMock(stdout='1.0-1'),  # Current version
            MagicMock(returncode=0),    # wget
            MagicMock(returncode=0)     # apt install
        ]
        global PACKAGE_VERSION
        PACKAGE_VERSION = "2.0-1"
        result = check_package_version()
        self.assertTrue(result)

    def test_get_disk_info_md1(self):
        result = get_disk_info('/dev/md1', full=True)
        self.assertEqual(result['health'], 'N/A')
        self.assertEqual(result['temperature'], 'N/A')

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_get_raid_status_error(self, mock_file):
        result = get_raid_status()
        self.assertEqual(result['md0']['state'], 'unknown')

    @patch('psutil.disk_io_counters', side_effect=Exception("IO error"))
    def test_get_disk_io_error(self, mock_io):
        reads, writes = get_disk_io('sda')
        self.assertEqual(reads, 0)
        self.assertEqual(writes, 0)

    @patch('mqtt_monitor.run_cmd')
    def test_get_disk_info_invalid_capacity(self, mock_run_cmd):
        mock_run_cmd.return_value = "invalid"
        result = get_disk_info('/dev/sda', full=False)
        self.assertEqual(result['capacity'], 0)

    @patch('subprocess.run')
    def test_install_deb(self, mock_subprocess):
        mock_subprocess.return_value = MagicMock(returncode=0)
        subprocess.run(["apt", "install", "-y", "/tmp/mqtt-monitor.deb"], check=True)
        mock_subprocess.assert_called_with(
            ["apt", "install", "-y", "/tmp/mqtt-monitor.deb"], check=True
        )

    @patch('subprocess.run')
    def test_remove_deb(self, mock_subprocess):
        mock_subprocess.return_value = MagicMock(returncode=0)
        subprocess.run(["apt", "remove", "-y", "mqtt-monitor"], check=True)
        mock_subprocess.assert_called_with(
            ["apt", "remove", "-y", "mqtt-monitor"], check=True
        )

    @patch('subprocess.run')
    def test_purge_deb(self, mock_subprocess):
        mock_subprocess.return_value = MagicMock(returncode=0)
        subprocess.run(["apt", "purge", "-y", "mqtt-monitor"], check=True)
        mock_subprocess.assert_called_with(
            ["apt", "purge", "-y", "mqtt-monitor"], check=True
        )

    @patch('requests.get')
    @patch('subprocess.run')
    def test_update_to_latest(self, mock_subprocess, mock_requests):
        mock_requests.return_value.json.return_value = {'tag_name': 'v2.0-1'}
        mock_subprocess.side_effect = [
            MagicMock(stdout='1.0-1'),  # Current version
            MagicMock(returncode=0),    # wget
            MagicMock(returncode=0)     # apt install
        ]
        global PACKAGE_VERSION
        PACKAGE_VERSION = "latest"
        result = check_package_version()
        self.assertTrue(result)
        mock_subprocess.assert_any_call(
            ["apt", "install", "-y", "/tmp/mqtt-monitor.deb"], check=True
        )

    @patch('mqtt_monitor.run_cmd')
    @patch('mqtt_monitor.send_alert')
    def test_disk_failure_alert(self, mock_send_alert, mock_run_cmd):
        mock_run_cmd.return_value = "FAILED\nTemperature: 50\nReallocated_Sector_Ct 10"
        result = get_disk_info('/dev/sda', full=True)
        self.assertEqual(result['health'], 'FAILED')
        mock_send_alert.assert_called_with(
            "Disk /dev/sda Failed", unittest.mock.ANY
        )

    def test_mqtt_publish_integration(self):
        client = mqtt.Client()
        client.username_pw_set("test", "test")
        client.connect("mosquitto", 1883, 60)
        client.loop_start()

        topic = "test/mqtt-monitor"
        payload = "test_value"
        client.publish(topic, payload)

        received = []
        def on_message(client, userdata, msg):
            received.append((msg.topic, msg.payload.decode()))
        client.subscribe(topic)
        client.on_message = on_message

        time.sleep(1)  # Wait for message
        client.loop_stop()
        client.disconnect()

        self.assertIn((topic, payload), received)
