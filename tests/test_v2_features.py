import sys

sys.path.insert(0, "/app/src")
sys.path.insert(0, "./src")

from mqtt_monitor import Config  # pylint: disable=wrong-import-position
from mqtt_monitor_v2.home_assistant import build_dashboard_yaml  # pylint: disable=wrong-import-position
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

    assert "sensor.server_active_issue_count" in dashboard_yaml
    assert "sensor.server_root_disk_usage_percentage" in dashboard_yaml
    assert "binary_sensor.server_container_web_running" in dashboard_yaml
    assert "sensor.disk_sda_capacity_gib" in dashboard_yaml
