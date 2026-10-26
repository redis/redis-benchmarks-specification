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


def _ready_line(listeners=2):
    """The line bcast-listener prints once all its listeners are attached, as
    captured from redis/bcast-tracking-bench:latest (one flat JSON object)."""
    return (
        '{"status":"ok","acl_user_count":0,"listener_count":%d,'
        '"message":"listeners are ready; press Ctrl-C to stop","mode":"manual",'
        '"startup_duration_ms":211.052034,"tracking_prefix":"bench:"}' % listeners
    ).encode()


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
    bg.logs.return_value = _ready_line()

    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [bg, fg]

    result = multi_tool.run_client_configs(
        docker_client,
        [_memtier_spec(), _bcast_spec()],
        cpuset_cpus="0",
        temporary_dir_client=str(tmp_path),
    )

    # Both clients launched, detached, the background helper first.
    assert docker_client.containers.run.call_count == 2
    for call in docker_client.containers.run.call_args_list:
        assert call.kwargs.get("detach") is True
    images = [c.kwargs["image"] for c in docker_client.containers.run.call_args_list]
    assert images == [
        "redis/bcast-tracking-bench:latest",
        "redislabs/memtier_benchmark:edge",
    ]

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
    bg.logs.return_value = _ready_line()

    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [bg, fg]

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
    # Ready, then exited during the measured window.
    bg.status = "exited"
    bg.attrs = {"State": {"ExitCode": 1}}
    bg.logs.return_value = _ready_line()

    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [bg, fg]

    with pytest.raises(RuntimeError, match="exited before the benchmark finished"):
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
    started.status = "running"
    started.logs.return_value = _ready_line()
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [started, Exception("pull failed")]

    with pytest.raises(RuntimeError, match="Failed to start client 0"):
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
    bg.logs.return_value = _ready_line()
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [bg, fg]

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
    # Helper launched first; the split still follows client_index.
    assert cpusets == ["8,9,10,11", "4,5,6,7"]  # disjoint, not the shared "4-11"


def test_make_client_output_dir_is_writable_by_non_root_client_images(tmp_path):
    """The production helper must leave the client dir writable by a foreign uid.

    Client containers bind-mount this dir to write their --json-out-file. mkdtemp()
    is 0700 owned by the coordinator's user, and pubsub-sub-bench's image runs as uid
    1001, so before this every pubsub-mixed suite ran to completion and then died with
    "open benchmark_output_1.json: permission denied", exit 1.

    This calls the real helper both call sites use, so deleting the chmod from it fails
    here.
    """
    import os
    import stat

    from redis_benchmarks_specification.__common__.multi_tool import (
        make_client_output_dir,
    )

    client_dir = make_client_output_dir(str(tmp_path))
    mode = stat.S_IMODE(os.stat(client_dir).st_mode)

    assert mode == 0o777, f"expected 0o777, got {oct(mode)}"
    # The bits an arbitrary container uid actually needs.
    assert mode & stat.S_IWOTH, "other-write is required to create the output file"
    assert mode & stat.S_IXOTH, "other-execute is required to traverse into the dir"


def test_make_client_output_dir_stays_private_when_it_will_hold_tls_material(tmp_path):
    """Under TLS the same dir receives copies of the cert/key, so it must NOT be widened.

    The standalone runner calls cp_to_workdir(temporary_dir_client, tls_key); making that
    directory world-writable/traversable would let any local user read the private key.
    """
    import os
    import stat

    from redis_benchmarks_specification.__common__.multi_tool import (
        make_client_output_dir,
    )

    client_dir = make_client_output_dir(str(tmp_path), world_writable=False)
    mode = stat.S_IMODE(os.stat(client_dir).st_mode)

    assert mode == 0o700, f"expected mkdtemp default 0o700, got {oct(mode)}"
    assert not mode & stat.S_IROTH, "TLS material must not be world-readable"
    assert not mode & stat.S_IWOTH, "TLS material dir must not be world-writable"


def test_allow_client_output_writes_widens_an_existing_private_dir(tmp_path):
    """The runner creates the dir private and widens it only once TLS is ruled out.

    `--uri` can flip tls_enabled on after the directory already exists, so the decision
    cannot be made at creation time; this is the deferred step.
    """
    import os
    import stat

    from redis_benchmarks_specification.__common__.multi_tool import (
        allow_client_output_writes,
        make_client_output_dir,
    )

    client_dir = make_client_output_dir(str(tmp_path), world_writable=False)
    assert stat.S_IMODE(os.stat(client_dir).st_mode) == 0o700

    allow_client_output_writes(client_dir)
    mode = stat.S_IMODE(os.stat(client_dir).st_mode)
    assert mode == 0o777, f"expected 0o777, got {oct(mode)}"
    assert mode & stat.S_IWOTH and mode & stat.S_IXOTH


# --------------------------------------------------------------------------- #
# readiness barrier (#584): the measuring client starts only after the
# background helper reports every listener attached, and the server-side
# tracking_clients count closes the measured window.
# --------------------------------------------------------------------------- #
def _info(tracking_clients, mem_clients_normal=0, run_id="node-1"):
    return {
        "run_id": run_id,
        "tracking_clients": tracking_clients,
        "mem_clients_normal": mem_clients_normal,
    }


def _tracking_conn(*infos):
    """A connection whose INFO answers ``infos`` in order: the baseline read
    before the helpers start, then the read at the end of the window."""
    from unittest.mock import MagicMock

    conn = MagicMock()
    conn.info.side_effect = list(infos)
    return conn


def _bg_and_fg(listeners=2):
    from unittest.mock import MagicMock

    fg = MagicMock()
    fg.wait.return_value = {"StatusCode": 0}
    fg.logs.return_value = b"{}"
    bg = MagicMock()
    bg.status = "running"
    bg.logs.return_value = _ready_line(listeners)
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [bg, fg]
    return bg, fg, docker_client


def test_parse_background_ready_returns_the_ready_payload():
    logs = (
        'Subscribing for the following prefixes: ["bench:"]\n'
        + _ready_line(1000).decode()
        + "\n"
    )
    payload = multi_tool.parse_background_ready(logs)
    assert payload["listener_count"] == 1000


def test_parse_background_ready_ignores_other_lines():
    error_line = '{"status":"error","message":"failed to initialize fred client"}'
    logs = "Subscribing for the following prefixes\n{not json\n" + error_line + "\n"
    assert multi_tool.parse_background_ready(logs) is None
    assert multi_tool.parse_background_ready("") is None


def test_run_client_configs_starts_measuring_client_after_helper_is_ready(
    tmp_path, monkeypatch
):
    import time
    from unittest.mock import MagicMock

    _write_memtier_output(tmp_path)
    events = []
    monkeypatch.setattr(time, "sleep", lambda s: events.append("sleep"))

    fg = MagicMock()
    fg.wait.return_value = {"StatusCode": 0}
    fg.logs.return_value = b"{}"
    bg = MagicMock()
    bg.status = "running"
    attaching = b'Subscribing for the following prefixes: ["bench:"]\n'
    bg_logs = [attaching, attaching + _ready_line()]

    def logs_so_far():
        events.append("bg-logs")
        return bg_logs.pop(0) if len(bg_logs) > 1 else bg_logs[0]

    bg.logs.side_effect = logs_so_far

    def run(**kwargs):
        events.append("run " + kwargs["image"])
        return bg if "bcast" in kwargs["image"] else fg

    docker_client = MagicMock()
    docker_client.containers.run.side_effect = run

    result = multi_tool.run_client_configs(
        docker_client,
        [_memtier_spec(), _bcast_spec()],
        cpuset_cpus="0",
        temporary_dir_client=str(tmp_path),
    )

    assert events[:5] == [
        "run redis/bcast-tracking-bench:latest",
        "bg-logs",  # not ready yet: attach in progress
        "sleep",
        "bg-logs",  # ready line
        "run redislabs/memtier_benchmark:edge",
    ]
    assert result.attached_listeners is None  # no redis_conns given


def test_run_client_configs_fails_fast_when_helper_dies_before_ready(tmp_path):
    from unittest.mock import MagicMock

    bg = MagicMock()
    bg.status = "exited"
    bg.attrs = {"State": {"ExitCode": 1}}
    bg.logs.return_value = (
        b"Error: failed to initialize fred client\n"
        b"Caused by: Timeout Error: Request timed out.\n"
    )
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [bg]

    with pytest.raises(RuntimeError, match="stopped before it was ready"):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
        )

    # The measuring client never started, and the helper did not leak.
    assert docker_client.containers.run.call_count == 1
    bg.remove.assert_called_once_with(force=True)


def test_run_client_configs_fails_on_a_ready_line_without_listener_count(tmp_path):
    from unittest.mock import MagicMock

    bg = MagicMock()
    bg.status = "running"
    bg.logs.return_value = (
        b'{"status":"ok","message":"listeners are ready; press Ctrl-C to stop"}'
    )
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [bg]

    with pytest.raises(RuntimeError, match="without a listener count"):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
        )

    assert docker_client.containers.run.call_count == 1
    bg.remove.assert_called_once_with(force=True)


def test_run_client_configs_fails_when_helper_is_never_ready(tmp_path):
    from unittest.mock import MagicMock

    bg = MagicMock()
    bg.status = "running"
    bg.logs.return_value = b'Subscribing for the following prefixes: ["bench:"]\n'
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [bg]

    with pytest.raises(RuntimeError, match="was not ready after 0s"):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
            background_ready_timeout=0,
        )

    assert docker_client.containers.run.call_count == 1
    bg.remove.assert_called_once_with(force=True)


def test_run_client_configs_counts_listeners_at_the_end_of_the_window(tmp_path):
    _write_memtier_output(tmp_path, ops=999.0)
    memtier_inode = (tmp_path / "benchmark_output_0.json").stat().st_ino
    bg, fg, docker_client = _bg_and_fg(listeners=2)
    infos = iter([_info(0), _info(2, mem_clients_normal=40960)])
    seen_at_each_read = []

    def info():
        seen_at_each_read.append((fg.wait.called, bg.stop.called))
        return next(infos)

    conn = _tracking_conn()
    conn.info.side_effect = info

    result = multi_tool.run_client_configs(
        docker_client,
        [_memtier_spec(), _bcast_spec()],
        cpuset_cpus="0",
        temporary_dir_client=str(tmp_path),
        redis_conns=[conn],
    )

    # Read before anything starts, then after the measuring client finished and
    # while the helper still ran: the end of the measured window.
    assert seen_at_each_read == [(False, False), (True, False)]
    assert result.attached_listeners == 2
    # Recorded with the measured client's results, in the aggregate and in the
    # JSON on disk that the coordinator reads.
    on_disk = json.loads((tmp_path / "benchmark_output_0.json").read_text())
    for results in (json.loads(result.aggregated_stdout), on_disk):
        totals = results["ALL STATS"]["Totals"]
        assert totals["Ops/sec"] == 999.0
        assert totals["TrackingListenersAttached"] == 2
        assert totals["MemClientsNormalAtWindowEnd"] == 40960
    # Replaced, not written into: the client container may have created it as
    # root, and the coordinator does not always run as root.
    assert (tmp_path / "benchmark_output_0.json").stat().st_ino != memtier_inode


def test_run_client_configs_leaves_out_a_missing_mem_clients_normal(tmp_path):
    _write_memtier_output(tmp_path)
    bg, fg, docker_client = _bg_and_fg(listeners=2)
    without_mem = {"run_id": "node-1", "tracking_clients": 0}

    result = multi_tool.run_client_configs(
        docker_client,
        [_memtier_spec(), _bcast_spec()],
        cpuset_cpus="0",
        temporary_dir_client=str(tmp_path),
        redis_conns=[
            _tracking_conn(without_mem, dict(without_mem, tracking_clients=2))
        ],
    )

    totals = json.loads(result.aggregated_stdout)["ALL STATS"]["Totals"]
    assert totals["TrackingListenersAttached"] == 2
    assert "MemClientsNormalAtWindowEnd" not in totals


def test_bcast_specs_export_the_listener_keys():
    """Each spec with a bcast-listener helper exports the keys the engine adds,
    so a renamed key or a mistyped path cannot silently drop the series."""
    import pathlib

    import yaml

    expected = {
        f'$."ALL STATS".Totals.{multi_tool.TRACKING_LISTENERS_ATTACHED}',
        f'$."ALL STATS".Totals.{multi_tool.MEM_CLIENTS_NORMAL_AT_WINDOW_END}',
    }
    checked = []
    for path in sorted(
        pathlib.Path("./redis_benchmarks_specification/test-suites").glob("*.yml")
    ):
        spec = yaml.safe_load(path.read_text())
        tools = [c.get("tool", "") for c in spec.get("clientconfigs", [])]
        if not any("bcast-listener" in tool for tool in tools):
            continue
        metrics = set(spec["exporter"]["redistimeseries"]["metrics"])
        assert expected <= metrics, path.name
        checked.append(path.name)
    assert len(checked) == 4


def test_run_client_configs_aborts_when_listeners_are_missing(tmp_path):
    _write_memtier_output(tmp_path)
    bg, fg, docker_client = _bg_and_fg(listeners=2)

    with pytest.raises(RuntimeError, match="Only 1 of 2 tracking listeners"):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
            redis_conns=[_tracking_conn(_info(0), _info(1))],
        )

    bg.remove.assert_called_with(force=True)


def test_run_client_configs_does_not_count_other_tracking_clients(tmp_path, caplog):
    """On a shared server, tracking clients that were there before the helper
    started must not hide listeners the helper failed to keep."""
    _write_memtier_output(tmp_path)
    bg, fg, docker_client = _bg_and_fg(listeners=2)

    with pytest.raises(RuntimeError, match="Only 1 of 2 tracking listeners"):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
            redis_conns=[_tracking_conn(_info(50), _info(51))],
        )

    assert "50 tracking clients were connected before" in caplog.text


def test_run_client_configs_aborts_when_the_end_read_fails(tmp_path):
    """Once the baseline was read, a failed read at the end of the window must
    not let the run through without its listener check."""
    import redis

    _write_memtier_output(tmp_path)
    bg, fg, docker_client = _bg_and_fg(listeners=2)
    conn = _tracking_conn(_info(0), redis.exceptions.ConnectionError("gone"))

    with pytest.raises(RuntimeError, match="Could not read the tracking listeners"):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
            redis_conns=[conn],
        )

    bg.remove.assert_called_with(force=True)


def test_run_client_configs_skips_count_when_server_lacks_tracking_clients(tmp_path):
    payload = _write_memtier_output(tmp_path)
    bg, fg, docker_client = _bg_and_fg(listeners=2)
    no_field = {"run_id": "node-1", "connected_clients": 3}

    result = multi_tool.run_client_configs(
        docker_client,
        [_memtier_spec(), _bcast_spec()],
        cpuset_cpus="0",
        temporary_dir_client=str(tmp_path),
        redis_conns=[_tracking_conn(no_field, no_field)],
    )

    assert result.attached_listeners is None
    assert json.loads(result.aggregated_stdout) == payload


def test_run_client_configs_removes_the_helper_on_interrupt_while_waiting(tmp_path):
    from unittest.mock import MagicMock

    bg = MagicMock()
    bg.logs.side_effect = KeyboardInterrupt
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [bg]

    with pytest.raises(KeyboardInterrupt):
        multi_tool.run_client_configs(
            docker_client,
            [_memtier_spec(), _bcast_spec()],
            cpuset_cpus="0",
            temporary_dir_client=str(tmp_path),
        )

    bg.remove.assert_called_once_with(force=True)


def test_read_listener_state_sums_distinct_servers():
    conns = [
        _tracking_conn(_info(600, 10, run_id="node-1")),
        _tracking_conn(_info(400, 20, run_id="node-2")),
        # The same node again: a cluster runner keeps one connection per slot
        # range.
        _tracking_conn(_info(600, 10, run_id="node-1")),
    ]
    assert multi_tool.read_listener_state(conns) == {
        "tracking_clients": 1000,
        "mem_clients_normal": 30,
    }
