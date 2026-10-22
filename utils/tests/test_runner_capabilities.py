from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml

from redis_benchmarks_specification.__common__.runner_capabilities import (
    filter_specs_by_runner_capabilities,
)
from redis_benchmarks_specification.__self_contained_coordinator__.args import (
    create_self_contained_coordinator_args,
)
from redis_benchmarks_specification.__self_contained_coordinator__ import (
    self_contained_coordinator as coordinator,
)

SUITES = Path("redis_benchmarks_specification/test-suites")
DISK_SPECS = sorted(SUITES.glob("memtier_benchmark-rdb-*.yml")) + sorted(
    SUITES.glob("memtier_benchmark-12Mkeys-string-1KiB*bgsave-duration.yml")
)


def test_all_rdb_specs_require_local_disk_and_default_runners_skip_them():
    assert len(DISK_SPECS) == 26
    for path in DISK_SPECS:
        assert yaml.safe_load(path.read_text())["required-runner-capabilities"] == [
            "local-disk"
        ]
    assert filter_specs_by_runner_capabilities(DISK_SPECS) == []
    assert (
        filter_specs_by_runner_capabilities(DISK_SPECS, "profiler,network-disk") == []
    )
    assert filter_specs_by_runner_capabilities(DISK_SPECS, "local-disk") == DISK_SPECS


def test_ordinary_specs_preserve_admission_on_every_runner(tmp_path):
    spec = tmp_path / "ordinary.yml"
    spec.write_text("name: ordinary\n")
    assert filter_specs_by_runner_capabilities([spec]) == [spec]
    assert filter_specs_by_runner_capabilities([spec], "local-disk") == [spec]


@pytest.mark.parametrize("invalid", [None, "local-disk", True, [True], [""]])
def test_invalid_requirements_fail_closed(tmp_path, invalid):
    spec = tmp_path / "invalid.yml"
    spec.write_text(yaml.safe_dump({"required-runner-capabilities": invalid}))
    with pytest.raises(ValueError, match="Invalid required-runner-capabilities"):
        filter_specs_by_runner_capabilities([spec], "local-disk")


def test_capabilities_require_all_declared_names_and_use_exact_matches(tmp_path):
    spec = tmp_path / "multiple.yml"
    spec.write_text("required-runner-capabilities: [local-disk, dedicated]\n")
    assert filter_specs_by_runner_capabilities([spec], "local-disk") == []
    assert filter_specs_by_runner_capabilities([spec], "not-local-disk,dedicated") == []
    assert filter_specs_by_runner_capabilities([spec], " local-disk, dedicated ") == [
        spec
    ]


def test_coordinator_capability_is_an_explicit_startup_option():
    parser = create_self_contained_coordinator_args("test")
    assert parser.parse_args([]).runner_capabilities == ""
    assert (
        parser.parse_args(["--runner-capabilities", "local-disk"]).runner_capabilities
        == "local-disk"
    )


@pytest.mark.parametrize("explicit_target", [False, True])
def test_coordinator_skips_disk_specs_before_queue_or_containers(explicit_target):
    conn, docker = MagicMock(), MagicMock()
    fields = {b"run_image": b"image", b"runner_capabilities": b"local-disk"}
    if explicit_target:
        fields[b"target_platform"] = b"ordinary"
    entry = [(b"stream", [(b"1234-0", fields)])]
    build = ("build", {}, [], "hash", "unstable", "8.10", "image", False, None, "amd64")
    with patch.object(
        coordinator, "extract_build_info_from_streamdata", return_value=build
    ), patch.object(
        coordinator, "filter_test_files", return_value=DISK_SPECS
    ), patch.object(
        coordinator, "data_prepopulation_step"
    ) as preload:
        result = coordinator.process_self_contained_coordinator_stream(
            conn,
            False,
            docker,
            "/tmp",
            entry,
            None,
            DISK_SPECS,
            {},
            "ordinary",
            args=SimpleNamespace(runner_capabilities="", topology=None),
        )
    assert result == ("1234-0", True, 0)
    conn.pipeline.return_value.lpush.assert_not_called()
    docker.containers.run.assert_not_called()
    preload.assert_not_called()


def test_legacy_client_runner_cannot_admit_disk_specs():
    from redis_benchmarks_specification.__runner__ import runner

    args = SimpleNamespace(
        dry_run=False,
        memory_comparison_only=False,
        dry_run_include_preload=False,
        defaults_filename="defaults.yml",
        override_test_runs=1,
    )
    docker = MagicMock()
    with patch.object(
        runner, "get_defaults", return_value=(None, None, [])
    ), patch.object(runner, "get_final_benchmark_config") as load:
        runner.process_self_contained_coordinator_stream(
            args,
            False,
            docker,
            "/tmp",
            None,
            None,
            DISK_SPECS,
            {},
            "local-disk",
        )
    load.assert_not_called()
    docker.containers.run.assert_not_called()
