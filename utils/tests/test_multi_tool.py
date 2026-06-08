#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Unit tests for the shared multi-tool engine
``redis_benchmarks_specification.__common__.multi_tool``.

These exercise the documented API of the I1 module (see
``approaches/coordinator-multitool-design.md``). They use MagicMock docker
containers (``docker_client.containers.run.side_effect=[...]``) exactly like
``test_bcast_listener.py`` -- NO real docker/redis is touched.

If the I1 module has not been created yet, every test is skipped with a clear
reason rather than erroring, so this file stays collectable + ``black``-clean in
CI. Once the module lands, the tests run for real.
"""

import json

import pytest

# The module under test is created by implementer I1. Until then we skip the
# whole file (collectable, not an import-time error).
multi_tool = pytest.importorskip(
    "redis_benchmarks_specification.__common__.multi_tool",
    reason="I1 module __common__/multi_tool.py not created yet",
)


# --------------------------------------------------------------------------- #
# benchmark_config fixtures (mirrors test_bcast_listener._multi_cfg)
# --------------------------------------------------------------------------- #
MEMTIER_ARGS = (
    '--test-time 1 --command "SET __key__ __data__" -c 1 -t 1 --hide-histogram'
)


def _memtier_clientconfig():
    return {
        "clientconfig": {
            "run_image": "redislabs/memtier_benchmark:edge",
            "tool": "memtier_benchmark",
            "arguments": MEMTIER_ARGS,
            "resources": {"requests": {"cpus": "1", "memory": "1g"}},
        }
    }


def _memtier_plus_bcast_clientconfigs():
    return {
        "clientconfigs": [
            {
                "run_image": "redislabs/memtier_benchmark:edge",
                "tool": "memtier_benchmark",
                "arguments": MEMTIER_ARGS,
                "resources": {"requests": {"cpus": "1", "memory": "1g"}},
            },
            {
                "run_image": "redis/bcast-tracking-bench:latest",
                "tool": "bcast-listener",
                "arguments": "--listener-count 2 --tracking-prefix bench:",
                "resources": {"requests": {"cpus": "1", "memory": "1g"}},
            },
        ]
    }


# Keyword arguments common to every prepare_client_run_specs() call. Kept in one
# place so the per-test calls stay readable.
_PREP_KWARGS = dict(
    host="127.0.0.1",
    port=6379,
    password=None,
    oss_cluster_api_enabled=False,
    tls_enabled=False,
    tls_skip_verify=False,
    test_tls_cert=None,
    test_tls_key=None,
    test_tls_cacert=None,
    resp_version="2",
    override_memtier_test_time=0,
    override_test_runs=1,
    unix_socket="",
    benchmark_tool_workdir="/mnt/client/",
)


def _prepare(benchmark_config, **overrides):
    kwargs = dict(_PREP_KWARGS)
    kwargs.update(overrides)
    return multi_tool.prepare_client_run_specs(benchmark_config, **kwargs)


# --------------------------------------------------------------------------- #
# 1. single clientconfig (memtier) -> 1 ClientRunSpec, foreground, memtier args
# --------------------------------------------------------------------------- #
def test_prepare_single_clientconfig_memtier():
    specs = _prepare(_memtier_clientconfig())

    assert len(specs) == 1
    spec = specs[0]
    assert spec.client_index == 0
    assert "memtier_benchmark" in spec.client_tool
    assert spec.client_image == "redislabs/memtier_benchmark:edge"
    assert spec.is_background is False
    # The fully-prepared command must carry the memtier arguments through.
    assert "--test-time 1" in spec.command_str
    assert "SET __key__ __data__" in spec.command_str
    assert "-c 1" in spec.command_str
    assert spec.output_filename == "benchmark_output_0.json"


# --------------------------------------------------------------------------- #
# 2. multi clientconfigs (memtier + bcast) -> 2 specs, bcast is_background True
# --------------------------------------------------------------------------- #
def test_prepare_multi_clientconfigs_marks_bcast_background():
    specs = _prepare(_memtier_plus_bcast_clientconfigs())

    assert len(specs) == 2

    memtier_spec = specs[0]
    bcast_spec = specs[1]

    assert "memtier_benchmark" in memtier_spec.client_tool
    assert memtier_spec.is_background is False

    assert "bcast-listener" in bcast_spec.client_tool
    assert bcast_spec.is_background is True
    assert bcast_spec.client_index == 1
    # The bcast helper command carries its listener arguments through.
    assert "--listener-count 2" in bcast_spec.command_str


# --------------------------------------------------------------------------- #
# helpers to build ClientRunSpec mocks for run_client_configs tests
# --------------------------------------------------------------------------- #
def _spec(index, tool, image, command_str, is_background=False):
    """Build a real ClientRunSpec when the dataclass is importable; this keeps
    the run_client_configs tests independent of prepare_client_run_specs."""
    return multi_tool.ClientRunSpec(
        client_index=index,
        client_tool=tool,
        client_image=image,
        command_str=command_str,
        output_filename=f"benchmark_output_{index}.json",
        is_background=is_background,
    )


def _memtier_spec():
    return _spec(
        0,
        "memtier_benchmark",
        "redislabs/memtier_benchmark:edge",
        "/usr/local/bin/memtier_benchmark " + MEMTIER_ARGS,
        is_background=False,
    )


def _bcast_spec():
    return _spec(
        1,
        "bcast-listener",
        "redis/bcast-tracking-bench:latest",
        "manual --redis-url redis://127.0.0.1:6379/0 --listener-count 2",
        is_background=True,
    )


def _write_memtier_output(tmp_path, ops=12345.6):
    """Write a minimal memtier JSON the aggregator will read back."""
    payload = {
        "ALL STATS": {
            "Totals": {"Ops/sec": ops},
        }
    }
    (tmp_path / "benchmark_output_0.json").write_text(json.dumps(payload))
    return payload


# --------------------------------------------------------------------------- #
# 3. run_client_configs: memtier (fg, exit 0) + bcast (bg, running)
#    -> run() twice detach=True; fg waited; bg .stop; all removed(force=True);
#       MultiToolResult aggregates the memtier JSON.
# --------------------------------------------------------------------------- #
def test_run_client_configs_foreground_and_background(tmp_path):
    from unittest.mock import MagicMock

    payload = _write_memtier_output(tmp_path, ops=999.0)

    fg = MagicMock()
    fg.wait.return_value = {"StatusCode": 0}
    fg.logs.return_value = b"{}"
    bg = MagicMock()
    bg.status = "running"
    bg.logs.return_value = b"ready: 2 listeners"

    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [fg, bg]

    result = multi_tool.run_client_configs(
        docker_client,
        [_memtier_spec(), _bcast_spec()],
        cpuset_cpus="0",
        temporary_dir_client=str(tmp_path),
    )

    # Both clients launched, detached.
    assert docker_client.containers.run.call_count == 2
    for call in docker_client.containers.run.call_args_list:
        assert call.kwargs.get("detach") is True

    # Foreground was waited on; background helper was stopped (never waited).
    fg.wait.assert_called_once()
    bg.stop.assert_called_once()
    bg.wait.assert_not_called()

    # No container leak: every container force-removed.
    fg.remove.assert_called_with(force=True)
    bg.remove.assert_called_with(force=True)

    # The aggregated stdout is the memtier JSON we wrote on disk.
    assert isinstance(result, multi_tool.MultiToolResult)
    agg = json.loads(result.aggregated_stdout)
    assert agg == payload
    assert agg["ALL STATS"]["Totals"]["Ops/sec"] == 999.0
    assert result.memtier_output_filename == "benchmark_output_0.json"
    assert not result.background_failures


# --------------------------------------------------------------------------- #
# 3b. SAFETY: a foreground (measuring) client failure must STILL stop+remove
#     the background helper (the finally) -> no leaked tracking listener.
# --------------------------------------------------------------------------- #
def test_run_client_configs_stops_background_when_foreground_fails(tmp_path):
    from unittest.mock import MagicMock

    fg = MagicMock()
    fg.wait.return_value = {"StatusCode": 1}  # measuring client fails
    fg.logs.return_value = b"boom"
    bg = MagicMock()
    bg.status = "running"  # helper still alive when fg blew up
    bg.logs.return_value = b""

    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [fg, bg]

    with pytest.raises(RuntimeError):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
        )

    # The background helper must NOT leak even though the foreground raised.
    bg.stop.assert_called_once()
    bg.remove.assert_called_with(force=True)
    fg.remove.assert_called_with(force=True)


# --------------------------------------------------------------------------- #
# 4. abort-on-helper-death: bg exited(1) -> RuntimeError + still removed
# --------------------------------------------------------------------------- #
def test_run_client_configs_aborts_when_background_helper_exited(tmp_path):
    from unittest.mock import MagicMock

    _write_memtier_output(tmp_path)

    fg = MagicMock()
    fg.wait.return_value = {"StatusCode": 0}
    fg.logs.return_value = b"{}"
    bg = MagicMock()
    bg.status = "exited"
    bg.attrs = {"State": {"ExitCode": 1}}
    bg.logs.return_value = b""

    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [fg, bg]

    with pytest.raises(RuntimeError):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
        )

    # Even on abort the helper is cleaned up (no leak).
    bg.remove.assert_called_with(force=True)


# --------------------------------------------------------------------------- #
# 5. mid-launch failure: [started, Exception] -> RuntimeError + started removed
# --------------------------------------------------------------------------- #
def test_run_client_configs_cleans_up_on_midlaunch_failure(tmp_path):
    from unittest.mock import MagicMock

    started = MagicMock()
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [started, Exception("pull failed")]

    with pytest.raises(RuntimeError):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
        )

    started.remove.assert_called_once_with(force=True)


# --------------------------------------------------------------------------- #
# 6. single-clientconfig backward-compat: exactly one run(), no bg handling
# --------------------------------------------------------------------------- #
def test_run_client_configs_single_clientconfig_backward_compat(tmp_path):
    from unittest.mock import MagicMock

    payload = _write_memtier_output(tmp_path, ops=42.0)

    fg = MagicMock()
    fg.wait.return_value = {"StatusCode": 0}
    fg.logs.return_value = b"{}"

    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [fg]

    result = multi_tool.run_client_configs(
        docker_client,
        [_memtier_spec()],
        cpuset_cpus="0",
        temporary_dir_client=str(tmp_path),
    )

    # Exactly one container, foreground waited, no background machinery touched.
    assert docker_client.containers.run.call_count == 1
    fg.wait.assert_called_once()
    fg.remove.assert_called_with(force=True)

    agg = json.loads(result.aggregated_stdout)
    assert agg == payload
    assert not result.background_failures


# --------------------------------------------------------------------------- #
# 7. moved helpers (only if they live in multi_tool):
#    stop_background_helper running -> stop/no-failure;
#    remove_started_containers tolerates a remove error.
# --------------------------------------------------------------------------- #
def _bg_info(container, index=1, tool="bcast-listener"):
    return {"container": container, "client_index": index, "client_tool": tool}


@pytest.mark.skipif(
    not hasattr(multi_tool, "stop_background_helper"),
    reason="stop_background_helper not (re)exported from multi_tool",
)
def test_stop_background_helper_running_is_stopped_no_failure():
    from unittest.mock import MagicMock

    c = MagicMock()
    c.status = "running"
    c.logs.return_value = b"ready: 2 listeners"

    failure = multi_tool.stop_background_helper(_bg_info(c))

    assert failure is None
    c.stop.assert_called_once()
    c.remove.assert_called_once_with(force=True)


@pytest.mark.skipif(
    not hasattr(multi_tool, "remove_started_containers"),
    reason="remove_started_containers not (re)exported from multi_tool",
)
def test_remove_started_containers_tolerates_remove_error():
    from unittest.mock import MagicMock

    c0, c1 = MagicMock(), MagicMock()
    c1.remove.side_effect = RuntimeError("already gone")  # must be tolerated
    started = [
        {"container": c0, "client_index": 0},
        {"container": c1, "client_index": 1},
    ]

    multi_tool.remove_started_containers(started)  # must not raise

    c0.remove.assert_called_once_with(force=True)
    c1.remove.assert_called_once_with(force=True)


# --------------------------------------------------------------------------- #
# cpuset split: memtier and the background helper must get DISJOINT cores
# --------------------------------------------------------------------------- #
def _cpu_spec(index, tool, cpus, is_background=False):
    return multi_tool.ClientRunSpec(
        client_index=index,
        client_tool=tool,
        client_image="img",
        command_str="cmd",
        output_filename=f"benchmark_output_{index}.json",
        is_background=is_background,
        cpus=cpus,
    )


def test_partition_cpuset_disjoint_when_it_fits():
    specs = [
        _cpu_spec(0, "memtier_benchmark", 4),
        _cpu_spec(1, "bcast-listener", 4, is_background=True),
    ]
    part = multi_tool.partition_cpuset("4-11", specs)
    assert part == {0: "4,5,6,7", 1: "8,9,10,11"}
    # disjoint
    assert set(part[0].split(",")).isdisjoint(part[1].split(","))


def test_partition_cpuset_shares_when_not_possible():
    s = [_cpu_spec(0, "memtier_benchmark", 4), _cpu_spec(1, "bcast-listener", 4, True)]
    assert multi_tool.partition_cpuset("", s) is None  # no cpuset
    assert multi_tool.partition_cpuset("0-7", [s[0]]) is None  # single client
    # missing cpu requests -> share
    no_cpu = [
        _cpu_spec(0, "memtier_benchmark", 0),
        _cpu_spec(1, "bcast-listener", 0, True),
    ]
    assert multi_tool.partition_cpuset("0-7", no_cpu) is None
    # doesn't fit (5+5 > 8) -> share
    big = [
        _cpu_spec(0, "memtier_benchmark", 5),
        _cpu_spec(1, "bcast-listener", 5, True),
    ]
    assert multi_tool.partition_cpuset("0-7", big) is None


def test_run_client_configs_pins_disjoint_cpusets(tmp_path):
    from unittest.mock import MagicMock

    _write_memtier_output(tmp_path)
    fg = MagicMock()
    fg.wait.return_value = {"StatusCode": 0}
    fg.logs.return_value = b"{}"
    bg = MagicMock()
    bg.status = "running"
    bg.logs.return_value = b""
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [fg, bg]

    specs = [
        _cpu_spec(0, "memtier_benchmark", 4),
        _cpu_spec(1, "bcast-listener", 4, is_background=True),
    ]
    multi_tool.run_client_configs(
        docker_client,
        specs,
        cpuset_cpus="4-11",
        temporary_dir_client=str(tmp_path),
    )
    calls = docker_client.containers.run.call_args_list
    cpusets = [c.kwargs.get("cpuset_cpus") for c in calls]
    assert cpusets == ["4,5,6,7", "8,9,10,11"]  # disjoint, not the shared "4-11"
