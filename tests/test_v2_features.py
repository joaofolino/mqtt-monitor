import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, "/app/src")
sys.path.insert(0, "./src")

from mqtt_monitor import Config  # pylint: disable=wrong-import-position
from mqtt_monitor_v2.home_assistant import build_dashboard_yaml, entity_prefix  # pylint: disable=wrong-import-position
from mqtt_monitor_v2.issues import apply_issue_transitions, evaluate_snapshot_issues  # pylint: disable=wrong-import-position
from mqtt_monitor_v2.models import (  # pylint: disable=wrong-import-position
    ContainerMetric,
    DiskMetric,
    FilesystemMetric,
    HostMetrics,
    MonitorSnapshot,
    MonitorState,
    RaidMetric,
)
from mqtt_monitor_v2.system import discover_disk_devices, discover_lxc_containers, discover_mount_points  # pylint: disable=wrong-import-position


def test_issue_engine_tracks_new_and_resolved_issues(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=["/dev/sda"],
        raid_arrays=["md0"],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
    )
    state = MonitorState(discovered_expected_lxc_containers=["web"])

    unhealthy_snapshot = MonitorSnapshot(
        generated_at="2026-04-12T12:00:00Z",
        host=HostMetrics(cpu_load_percentage=10.0, memory_used_megabytes=512, cpu_temperature_celsius=55.0),
        filesystems=[FilesystemMetric(mount_point="/", usage_percentage=96, free_space="12G")],
        disks=[
            DiskMetric(
                path="/dev/sda",
                name="sda",
                capacity_gib=1024,
                capacity_bytes="1099511627776",
                availability="online",
                smart_health_status="PASSED",
                max_temperature=40,
                power_on_hours=200,
                reallocated_sectors=0,
                spin_up_count=1,
                read_kilobytes_per_second=0.0,
                write_kilobytes_per_second=0.0,
                read_count_per_second=0.0,
                write_count_per_second=0.0,
                busy_time_percentage=0.0,
                command_line_parse_error=False,
                device_open_failed=False,
                smart_command_failed=False,
                disk_failing=False,
                prefail_attributes_threshold=False,
                past_attributes_threshold=False,
                device_error_log_errors=False,
                self_test_log_errors=False,
            )
        ],
        raids=[RaidMetric(name="md0", status="degraded", raw="md0 degraded", failed_devices=["sdb"])],
        containers=[ContainerMetric(name="web", status="STOPPED", autostart=True)],
    )

    unhealthy_issues = apply_issue_transitions(
        unhealthy_snapshot,
        evaluate_snapshot_issues(config, unhealthy_snapshot, state),
        state,
    )

    active_keys = {issue.key for issue in unhealthy_issues if issue.active}
    assert "filesystem/root/critical_usage" in active_keys
    assert "raid/md0/status" in active_keys
    assert "container/web/stopped" in active_keys
    assert all(issue.newly_active for issue in unhealthy_issues if issue.active)

    healthy_snapshot = MonitorSnapshot(
        generated_at="2026-04-12T12:05:00Z",
        host=HostMetrics(cpu_load_percentage=10.0, memory_used_megabytes=512, cpu_temperature_celsius=45.0),
        filesystems=[FilesystemMetric(mount_point="/", usage_percentage=55, free_space="100G")],
        disks=unhealthy_snapshot.disks,
        raids=[RaidMetric(name="md0", status="active", raw="md0 active", failed_devices=[])],
        containers=[ContainerMetric(name="web", status="RUNNING", autostart=True)],
    )
    resolved_issues = apply_issue_transitions(
        healthy_snapshot,
        evaluate_snapshot_issues(config, healthy_snapshot, state),
        state,
    )

    resolved_keys = {issue.key for issue in resolved_issues if issue.newly_resolved}
    assert "filesystem/root/critical_usage" in resolved_keys
    assert "raid/md0/status" in resolved_keys
    assert "container/web/stopped" in resolved_keys


def test_build_dashboard_yaml_includes_v2_sections(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=["/dev/sda"],
        raid_arrays=["md0"],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
    )
    snapshot = MonitorSnapshot(
        generated_at="2026-04-12T12:00:00Z",
        host=HostMetrics(cpu_load_percentage=10.0, memory_used_megabytes=512, cpu_temperature_celsius=45.0),
        filesystems=[FilesystemMetric(mount_point="/", usage_percentage=55, free_space="100G")],
        disks=[
            DiskMetric(
                path="/dev/sda",
                name="sda",
                capacity_gib=1024,
                capacity_bytes="1099511627776",
                availability="online",
                smart_health_status="PASSED",
                max_temperature=40,
                power_on_hours=200,
                reallocated_sectors=0,
                spin_up_count=1,
                read_kilobytes_per_second=1.0,
                write_kilobytes_per_second=2.0,
                read_count_per_second=3.0,
                write_count_per_second=4.0,
                busy_time_percentage=5.0,
                command_line_parse_error=False,
                device_open_failed=False,
                smart_command_failed=False,
                disk_failing=False,
                prefail_attributes_threshold=False,
                past_attributes_threshold=False,
                device_error_log_errors=False,
                self_test_log_errors=False,
            )
        ],
        raids=[RaidMetric(name="md0", status="active", raw="md0 active", failed_devices=[])],
        containers=[ContainerMetric(name="web", status="RUNNING", autostart=True)],
    )

    dashboard_yaml = build_dashboard_yaml(config, snapshot)

    assert "type: sections" in dashboard_yaml
    assert "sensor.server_active_issue_count" in dashboard_yaml
    assert "sensor.server_root_disk_usage_percentage" in dashboard_yaml
    assert "binary_sensor.server_container_web_running" in dashboard_yaml
    assert "sensor.disk_sda_capacity_gib" in dashboard_yaml
    assert "heading: Expected Services" in dashboard_yaml
    assert "heading: Disk Heat" in dashboard_yaml
    assert "heading: Disk I/O" in dashboard_yaml
    assert "heading: Physical Disk Detail" in dashboard_yaml
    assert "name: SMART Health" in dashboard_yaml
    assert "name: Max Temp" in dashboard_yaml
    assert "name: I/O Busy" in dashboard_yaml
    assert "name: I/O Read" in dashboard_yaml
    assert "color: green" in dashboard_yaml
    assert "color: amber" in dashboard_yaml
    assert "mdi:harddisk-check" in dashboard_yaml
    assert "<th>Disk</th><th>Current I/O</th>" in dashboard_yaml
    assert "<strong>Transfer</strong>" in dashboard_yaml
    assert "<strong>Load</strong>" in dashboard_yaml
    assert "Load is the share of the latest sample window" in dashboard_yaml
    assert "serving I/O." in dashboard_yaml


def test_build_dashboard_yaml_colors_degraded_raids_red(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=[],
        raid_arrays=["md0"],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
    )
    snapshot = MonitorSnapshot(
        generated_at="2026-04-12T12:00:00Z",
        host=HostMetrics(cpu_load_percentage=10.0, memory_used_megabytes=512, cpu_temperature_celsius=45.0),
        filesystems=[FilesystemMetric(mount_point="/", usage_percentage=55, free_space="100G")],
        disks=[],
        raids=[RaidMetric(name="md0", status="degraded", raw="md0 degraded", failed_devices=["sdb"])],
        containers=[],
    )

    dashboard_yaml = build_dashboard_yaml(config, snapshot)

    assert "sensor.raid_md0_status" in dashboard_yaml
    assert "mdi:harddisk-remove" in dashboard_yaml
    assert "color: red" in dashboard_yaml


def test_dashboard_entity_prefix_is_sanitized(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="mqtt-monitor",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=["/dev/sda"],
        raid_arrays=["md0"],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
    )
    snapshot = MonitorSnapshot(
        generated_at="2026-04-12T12:00:00Z",
        host=HostMetrics(cpu_load_percentage=10.0, memory_used_megabytes=512, cpu_temperature_celsius=45.0),
        filesystems=[FilesystemMetric(mount_point="/", usage_percentage=55, free_space="100G")],
        disks=[],
        raids=[],
        containers=[],
    )

    dashboard_yaml = build_dashboard_yaml(config, snapshot)

    assert entity_prefix(config) == "mqtt_monitor"
    assert "sensor.mqtt_monitor_active_issue_count" in dashboard_yaml
    assert "sensor.mqtt-monitor_active_issue_count" not in dashboard_yaml


def test_discover_mount_points_skips_lxc_bind_mounts(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=["/dev/sda"],
        raid_arrays=["md0"],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
    )

    partitions = [
        SimpleNamespace(mountpoint="/", fstype="ext4"),
        SimpleNamespace(mountpoint="/data", fstype="ext4"),
        SimpleNamespace(mountpoint="/var/lib/lxc/web/rootfs/data", fstype="ext4"),
    ]
    with patch("mqtt_monitor_v2.system.psutil.disk_partitions", return_value=partitions):
        mounts = discover_mount_points(config)

    assert "/" in mounts
    assert "/data" in mounts
    assert "/var/lib/lxc/web/rootfs/data" not in mounts


def test_discover_disk_devices_includes_mapper_backed_monitored_mounts(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=["/dev/sda"],
        raid_arrays=["md0"],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/data/ds3512/acceleration-nextcloud"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
        auto_discover_disks=False,
    )

    partitions = [
        SimpleNamespace(
            device="/dev/mapper/mpatha-part1",
            mountpoint="/data/ds3512/acceleration-nextcloud",
            fstype="ext4",
        )
    ]
    with patch("mqtt_monitor_v2.system.psutil.disk_partitions", return_value=partitions), patch(
        "mqtt_monitor_v2.system.os.path.exists",
        return_value=True,
    ):
        devices = discover_disk_devices(config, mount_points=config.disk_usage_mounts)

    assert "/dev/sda" in devices
    assert "/dev/mapper/mpatha-part1" in devices


def test_build_dashboard_yaml_sanitizes_disk_names(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=["/dev/mapper/mpatha-part1"],
        raid_arrays=[],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/data/ds3512/acceleration-nextcloud"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
    )
    snapshot = MonitorSnapshot(
        generated_at="2026-04-12T12:00:00Z",
        host=HostMetrics(cpu_load_percentage=10.0, memory_used_megabytes=512, cpu_temperature_celsius=45.0),
        filesystems=[FilesystemMetric(mount_point="/data/ds3512/acceleration-nextcloud", usage_percentage=55, free_space="100G")],
        disks=[
            DiskMetric(
                path="/dev/mapper/mpatha-part1",
                name="mpatha-part1",
                capacity_gib=10240,
                capacity_bytes="10995116277760",
                availability="online",
                smart_health_status=None,
                max_temperature=None,
                power_on_hours=None,
                reallocated_sectors=None,
                spin_up_count=None,
                read_kilobytes_per_second=1.0,
                write_kilobytes_per_second=2.0,
                read_count_per_second=3.0,
                write_count_per_second=4.0,
                busy_time_percentage=5.0,
                command_line_parse_error=False,
                device_open_failed=False,
                smart_command_failed=False,
                disk_failing=False,
                prefail_attributes_threshold=False,
                past_attributes_threshold=False,
                device_error_log_errors=False,
                self_test_log_errors=False,
            )
        ],
        raids=[],
        containers=[],
    )

    dashboard_yaml = build_dashboard_yaml(config, snapshot)

    assert "sensor.disk_mpatha_part1_capacity_gib" in dashboard_yaml
    assert "sensor.disk_mpatha-part1_capacity_gib" not in dashboard_yaml
    assert "sensor.disk_mpatha_part1_smart_health_status" not in dashboard_yaml
    assert "sensor.server_data_ds3512_acceleration_nextcloud_disk_usage_percentage" in dashboard_yaml


def test_build_dashboard_yaml_sanitizes_container_names(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=[],
        raid_arrays=[],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
    )
    snapshot = MonitorSnapshot(
        generated_at="2026-04-12T12:00:00Z",
        host=HostMetrics(cpu_load_percentage=10.0, memory_used_megabytes=512, cpu_temperature_celsius=45.0),
        filesystems=[FilesystemMetric(mount_point="/", usage_percentage=55, free_space="100G")],
        disks=[],
        raids=[],
        containers=[ContainerMetric(name="acceleration-nextcloud", status="RUNNING", autostart=True)],
    )

    dashboard_yaml = build_dashboard_yaml(config, snapshot)

    assert "binary_sensor.server_container_acceleration_nextcloud_running" in dashboard_yaml
    assert "binary_sensor.server_container_acceleration-nextcloud_running" not in dashboard_yaml


def test_discover_lxc_containers_uses_privileged_helper_when_available(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=[],
        raid_arrays=[],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
        auto_discover_lxc=True,
        expected_lxc_containers=["web", "mail"],
    )
    logger = MagicMock()
    helper_output = """
    {
      "containers": [
        {"name": "web", "status": "RUNNING", "autostart": true},
        {"name": "torrent", "status": "STOPPED", "autostart": false}
      ]
    }
    """

    with patch("mqtt_monitor_v2.system.os.path.exists", return_value=True), patch(
        "mqtt_monitor_v2.system.run_cmd",
        return_value=(helper_output, 0),
    ) as mock_run_cmd:
        containers = discover_lxc_containers(config, logger)

    assert [container.name for container in containers] == ["web", "mail", "torrent"]
    assert containers[0].status == "RUNNING"
    assert containers[1].status == "STOPPED"
    assert containers[2].autostart is False
    assert mock_run_cmd.call_count == 1
    assert mock_run_cmd.call_args.args[2][-1] == "/usr/share/mqtt-monitor/lxc_inventory_helper.py"


def test_discover_lxc_containers_falls_back_to_legacy_commands_without_helper(tmp_path):
    config = Config(
        enable_email=False,
        log_level="INFO",
        enable_io_mqtt=True,
        alert_email="alert@example.com",
        smtp_server="smtp.example.com",
        smtp_port=587,
        smtp_user="user",
        smtp_pass="pass",
        package_version="latest",
        broker="localhost",
        port=1883,
        mqtt_user="test",
        mqtt_pass="test",
        self_name="server",
        smart_interval=3600,
        topic_prefix="test/mqtt-monitor",
        log_dir=str(tmp_path),
        log_file="mqtt-monitor.log",
        devices=[],
        raid_arrays=[],
        mdstat_path=str(tmp_path / "mdstat"),
        cpu_temp_path=str(tmp_path / "cpu_temp"),
        disk_usage_mounts=["/"],
        max_io_rate=1000000.0,
        md1_usage_threshold=90.0,
        md1_write_spike_threshold=10000.0,
        sleep_interval=60.0,
        error_sleep=5.0,
        state_dir=str(tmp_path),
        auto_discover_lxc=True,
    )
    logger = MagicMock()

    with patch("mqtt_monitor_v2.system.os.path.exists", return_value=False), patch(
        "mqtt_monitor_v2.system.run_cmd",
        side_effect=[("web\ntorrent", 0), ("web", 0)],
    ):
        containers = discover_lxc_containers(config, logger)

    assert [(container.name, container.status) for container in containers] == [
        ("web", "RUNNING"),
        ("torrent", "STOPPED"),
    ]
