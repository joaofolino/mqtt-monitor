import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, "/app/src")
sys.path.insert(0, "./src")

from mqtt_monitor import Config  # pylint: disable=wrong-import-position
from mqtt_monitor_v2.home_assistant import setup_sensors  # pylint: disable=wrong-import-position
from mqtt_monitor_v2.models import ContainerMetric, FilesystemMetric, HostMetrics, MonitorSnapshot  # pylint: disable=wrong-import-position
from mqtt_monitor_v2.system import (  # pylint: disable=wrong-import-position
    check_package_version,
    disk_cache,
    get_capacity_gib,
    get_disk_info,
    get_disk_io,
    get_raid_status,
    get_usage,
    log,
    parse_smartctl_exit_code,
    setup_logging,
)


@pytest.fixture
def setup_mdstat():
    mdstat_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "mdstat")

    def _write_mdstat(content):
        with open(mdstat_path, "w", encoding="utf-8") as file_handle:
            file_handle.write(content)

    return _write_mdstat


@pytest.fixture
def setup_monitor():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config = Config(
        enable_email=False,
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
        devices=["/dev/sda", "/dev/sdb", "/dev/nvme0n1", "/dev/md1"],
        raid_arrays=["md0", "md1"],
        mdstat_path="./tests/mdstat",
        cpu_temp_path="/sys/class/thermal/thermal_zone0/temp",
        disk_usage_mounts=["/", "/var/log"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
    )
    logger = setup_logging(config)
    disk_cache.clear()
    yield config, logger
    disk_cache.clear()
    logger.handlers = []


class TestMqttMonitor:
    def test_parse_smartctl_exit_code_flags(self):
        flags = parse_smartctl_exit_code((1 << 2) | (1 << 5))
        assert flags["smart_command_failed"] is True
        assert flags["past_attributes_threshold"] is True
        assert flags["device_open_failed"] is False

    def test_get_capacity_gib_accepts_int_and_string(self):
        assert get_capacity_gib(4) == 4
        assert get_capacity_gib(str(4 * 1024 * 1024 * 1024)) == 4
        assert get_capacity_gib("invalid") == 0

    def test_get_raid_status_tracks_failed_devices_without_blank_entries(self, setup_monitor, setup_mdstat):
        config, logger = setup_monitor
        setup_mdstat(
            """
            md0 : active raid1 sda[0]
                  1048576 blocks [2/1] [U_]
            """
        )
        result = get_raid_status(config, logger)
        assert result["md0"]["status"] == "degraded"
        assert "" not in result["md0"]["failed_devices"]

    def test_get_disk_info_handles_cached_integer_capacity(self, setup_monitor):
        config, logger = setup_monitor
        disk_cache["/dev/sda"] = {"capacity": 4}
        with patch("mqtt_monitor_v2.system.os.path.exists", return_value=True):
            result = get_disk_info(config, logger, "/dev/sda", full=False)
        assert result["capacity"] == 4

    def test_get_disk_info_keeps_nvme_online_with_valid_smart_output(self, setup_monitor):
        config, logger = setup_monitor
        smart_output = "\n".join(
            [
                "SMART overall-health self-assessment test result: PASSED",
                "Temperature: 65",
                "Power On Hours: 200",
            ]
        )
        with patch("mqtt_monitor_v2.system.os.path.exists", return_value=True), patch("mqtt_monitor_v2.system.run_cmd") as mock_run_cmd:
            mock_run_cmd.side_effect = [
                ("1099511627776", 0),
                (smart_output, 1 << 2),
            ]
            result = get_disk_info(config, logger, "/dev/nvme0n1", full=True)
        assert result["availability"] == "online"
        assert result["smart_health_status"] == "PASSED"
        assert result["smart_command_failed"] is True

    def test_get_disk_info_falls_back_to_scsi_when_sat_probe_fails(self, setup_monitor):
        config, logger = setup_monitor
        sat_error_output = "\n".join(
            [
                "Read Device Identity failed: scsi error unsupported scsi opcode",
                "A mandatory SMART command failed: exiting.",
            ]
        )
        scsi_output = "\n".join(
            [
                "SMART support is:     Enabled",
                "SMART Health Status: OK",
                "Current Drive Temperature:     28 C",
            ]
        )
        with patch("mqtt_monitor_v2.system.os.path.exists", return_value=True), patch("mqtt_monitor_v2.system.run_cmd") as mock_run_cmd:
            mock_run_cmd.side_effect = [
                ("1099511627776", 0),
                (sat_error_output, 1 << 1),
                (scsi_output, 1 << 2),
            ]
            result = get_disk_info(config, logger, "/dev/sda", full=True)
        assert result["availability"] == "online"
        assert result["smart_health_status"] == "PASSED"
        assert result["max_temperature"] == 28
        assert result["smart_command_failed"] is True

    def test_get_disk_info_treats_mapper_device_as_logical_volume(self, setup_monitor):
        config, logger = setup_monitor
        with patch("mqtt_monitor_v2.system.os.path.exists", return_value=True), patch(
            "mqtt_monitor_v2.system.os.path.realpath",
            return_value="/dev/dm-1",
        ), patch("mqtt_monitor_v2.system.run_cmd") as mock_run_cmd:
            mock_run_cmd.side_effect = [
                ("10995116277760", 0),
            ]
            result = get_disk_info(config, logger, "/dev/mapper/mpatha-part1", full=True)
        assert result["availability"] == "online"
        assert result["smart_health_status"] is None
        assert result["capacity"] == 10240

    def test_get_disk_io_returns_counter_dict(self, setup_monitor):
        config, logger = setup_monitor
        io_stats = MagicMock(
            read_count=10,
            write_count=20,
            read_bytes=1000,
            write_bytes=2000,
            read_time=30,
            write_time=40,
            busy_time=50,
            read_merged_count=1,
            write_merged_count=2,
        )
        with patch("mqtt_monitor_v2.system.psutil.disk_io_counters", return_value={"sda": io_stats}):
            result = get_disk_io(config, logger, "sda")
        assert result["read_bytes"] == 1000
        assert result["busy_time"] == 50

    def test_get_usage_falls_back_to_psutil(self, setup_monitor):
        config, logger = setup_monitor
        with patch("mqtt_monitor_v2.system.run_cmd", return_value=("", 0)), patch("mqtt_monitor_v2.system.psutil.disk_usage") as mock_disk_usage:
            mock_disk_usage.return_value = MagicMock(percent=75, free=1073741824)
            result = get_usage(config, logger, "/")
        assert result == (75, "1G")

    def test_setup_sensors_publishes_new_schema(self, setup_monitor):
        config, _ = setup_monitor
        client = MagicMock()
        setup_sensors(config, client)
        published_topics = [call.args[0] for call in client.publish.call_args_list]
        assert "homeassistant/sensor/server_cpu_load_percentage/config" in published_topics
        assert "homeassistant/sensor/disk_sda_busy_time_percentage/config" in published_topics
        assert "homeassistant/binary_sensor/disk_sda_smart_command_failed/config" in published_topics
        assert "homeassistant/sensor/raid_md0_status/config" in published_topics

    def test_setup_sensors_sanitizes_filesystem_and_container_object_ids(self, setup_monitor):
        config, _ = setup_monitor
        client = MagicMock()
        snapshot = MonitorSnapshot(
            generated_at="2026-04-12T12:00:00Z",
            host=HostMetrics(cpu_load_percentage=10.0, memory_used_megabytes=512, cpu_temperature_celsius=45.0),
            filesystems=[FilesystemMetric(mount_point="/data/ds3512/acceleration-nextcloud", usage_percentage=1, free_space="9.5T")],
            disks=[],
            raids=[],
            containers=[ContainerMetric(name="acceleration-nextcloud", status="RUNNING", autostart=True)],
        )
        setup_sensors(config, client, snapshot=snapshot)
        published_topics = [call.args[0] for call in client.publish.call_args_list]
        assert "homeassistant/sensor/server_data_ds3512_acceleration_nextcloud_disk_usage_percentage/config" in published_topics
        assert "homeassistant/sensor/server_data_ds3512_acceleration-nextcloud_disk_usage_percentage/config" not in published_topics
        assert "homeassistant/sensor/server_container_acceleration_nextcloud_status/config" in published_topics
        assert "homeassistant/sensor/server_container_acceleration-nextcloud_status/config" not in published_topics

    def test_check_package_version_reads_current_version_from_tuple(self, setup_monitor):
        config, logger = setup_monitor
        with patch("mqtt_monitor_v2.system.requests.get") as mock_requests, patch("mqtt_monitor_v2.system.run_cmd") as mock_run_cmd, patch("mqtt_monitor_v2.system.subprocess.run") as mock_subprocess_run:
            mock_requests.return_value.json.return_value = {"tag_name": "v1.0.1-2"}
            mock_run_cmd.return_value = ("1.0.1-1", 0)
            mock_subprocess_run.side_effect = [
                MagicMock(returncode=0),
                MagicMock(returncode=0),
                MagicMock(returncode=0),
            ]
            updated = check_package_version(config, logger)
        assert updated is True
        assert mock_subprocess_run.call_count == 3

    def test_check_package_version_does_not_downgrade_newer_rc(self, setup_monitor):
        config, logger = setup_monitor
        with patch("mqtt_monitor_v2.system.requests.get") as mock_requests, patch("mqtt_monitor_v2.system.run_cmd") as mock_run_cmd, patch("mqtt_monitor_v2.system.subprocess.run") as mock_subprocess_run:
            mock_requests.return_value.json.return_value = {"tag_name": "v1.0.1-2"}
            mock_run_cmd.return_value = ("1.1.0-0rc1", 0)
            mock_subprocess_run.return_value = MagicMock(returncode=1)
            updated = check_package_version(config, logger)
        assert updated is False
        assert mock_subprocess_run.call_count == 1

    def test_log_level_can_disable_debug(self, setup_monitor):
        config, logger = setup_monitor
        config.log_level = "INFO"
        logger.setLevel(logging.INFO)
        with patch.object(logger, "debug") as mock_debug:
            log(config, logger, "hidden", "DEBUG")
        mock_debug.assert_not_called()

    def test_service_unit_grants_nvme_capabilities(self):
        service_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "debian", "mqtt-monitor.service")
        service_text = open(service_path, "r", encoding="utf-8").read()
        assert "AmbientCapabilities=CAP_SYS_ADMIN CAP_SYS_RAWIO" in service_text
        assert "CapabilityBoundingSet=CAP_SYS_ADMIN CAP_SYS_RAWIO" in service_text

    def test_postinst_installs_lxc_helper_sudoers(self):
        postinst_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "debian", "postinst")
        postinst_text = open(postinst_path, "r", encoding="utf-8").read()
        assert "/etc/sudoers.d/mqtt-monitor-lxc-helper" in postinst_text
        assert "mqtt-monitor-lxc-helper.sudoers" in postinst_text

    def test_package_depends_on_sudo_for_lxc_helper(self):
        control_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "debian", "control")
        control_text = open(control_path, "r", encoding="utf-8").read()
        assert "sudo" in control_text
