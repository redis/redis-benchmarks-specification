#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from unittest.mock import MagicMock

import docker

from redis_benchmarks_specification.__runner__.runner import (
    prepare_bcast_listener_parameters,
    stop_background_helper,
)


def _bg(container, index=1, tool="bcast-listener"):
    return {"container": container, "client_index": index, "client_tool": tool}


def _prep(
    clientconfig,
    host="127.0.0.1",
    port=6379,
    password=None,
    username=None,
    tls_enabled=False,
    override_test_time=0,
):
    return prepare_bcast_listener_parameters(
        clientconfig,
        "bcast-listener",
        port,
        host,
        password,
        "benchmark_output_0.json",
        tls_enabled=tls_enabled,
        override_test_time=override_test_time,
        username=username,
    )


def test_bcast_listener_no_auth_builds_manual_url():
    cfg = {"arguments": "--listener-count 100 --tracking-prefix bench:"}
    _, cmd, _ = _prep(cfg)
    assert cmd == (
        "manual --redis-url redis://127.0.0.1:6379/0 "
        "--listener-count 100 --tracking-prefix bench:"
    )


def test_bcast_listener_password_only_embeds_in_url():
    cfg = {"arguments": "--listener-count 64"}
    _, cmd, _ = _prep(cfg, password="s3cret")
    assert "redis://:s3cret@127.0.0.1:6379/0" in cmd
    assert cmd.endswith("--listener-count 64")


def test_bcast_listener_acl_user_password():
    cfg = {"arguments": "--listener-count 1"}
    _, cmd, _ = _prep(cfg, username="hot", password="pw")
    assert "redis://hot:pw@127.0.0.1:6379/0" in cmd


def test_bcast_listener_percent_encodes_special_chars():
    cfg = {"arguments": "--listener-count 1"}
    _, cmd, _ = _prep(cfg, password="p@ss:/word")
    # @ : / must be percent-encoded so they don't corrupt the URL authority.
    assert "p%40ss%3A%2Fword" in cmd
    assert "p@ss:/word" not in cmd


def test_bcast_listener_tls_uses_rediss_scheme():
    cfg = {"arguments": "--listener-count 1"}
    _, cmd, _ = _prep(cfg, tls_enabled=True)
    assert cmd.startswith("manual --redis-url rediss://")


def test_bcast_listener_no_arguments_key():
    _, cmd, _ = _prep({})
    assert cmd == "manual --redis-url redis://127.0.0.1:6379/0"


def test_stop_background_helper_running_is_stopped_no_failure():
    """A healthy helper (still running at the end) is stopped + removed and
    reported as no failure."""
    c = MagicMock()
    c.status = "running"
    c.logs.return_value = b"ready: 100 listeners"
    failure = stop_background_helper(_bg(c))
    assert failure is None
    c.stop.assert_called_once()
    c.remove.assert_called_once_with(force=True)


def test_stop_background_helper_exited_is_a_failure():
    """A helper that exited on its own (crash/OOM/auth) is reported as a
    failure so the run aborts, and is still removed (no leak)."""
    c = MagicMock()
    c.status = "exited"
    c.attrs = {"State": {"ExitCode": 137}}
    c.logs.return_value = b"OOM"
    failure = stop_background_helper(_bg(c, index=2))
    assert failure is not None
    idx, tool, status, code = failure
    assert idx == 2 and status == "exited" and code == 137
    c.stop.assert_not_called()  # already exited; nothing to stop
    c.remove.assert_called_once_with(force=True)


def test_stop_background_helper_not_found_is_a_failure():
    """If the container vanished (reload -> NotFound) it's a failure but we
    still attempt removal."""
    c = MagicMock()
    c.reload.side_effect = docker.errors.NotFound("gone")
    failure = stop_background_helper(_bg(c, index=3))
    assert failure is not None
    assert failure[0] == 3 and failure[2] == "not-found"
    c.stop.assert_not_called()
    c.remove.assert_called_once_with(force=True)


def test_stop_background_helper_always_removes_even_if_stop_raises():
    """No container leak: remove is attempted even when stop() raises."""
    c = MagicMock()
    c.status = "running"
    c.stop.side_effect = RuntimeError("docker daemon hiccup")
    c.logs.return_value = b""
    failure = stop_background_helper(_bg(c))
    # stop failing is not itself a measurement failure (helper was running)
    assert failure is None
    c.remove.assert_called_once_with(force=True)


def test_remove_started_containers_removes_all_and_tolerates_errors():
    """A mid-launch failure cleanup must force-remove every started container
    and not raise if one removal fails."""
    from redis_benchmarks_specification.__runner__.runner import (
        remove_started_containers,
    )

    c0, c1, c2 = MagicMock(), MagicMock(), MagicMock()
    c1.remove.side_effect = RuntimeError("already gone")  # must be tolerated
    started = [
        {"container": c0, "client_index": 0},
        {"container": c1, "client_index": 1},
        {"container": c2, "client_index": 2},
    ]
    remove_started_containers(started)  # must not raise
    c0.remove.assert_called_once_with(force=True)
    c1.remove.assert_called_once_with(force=True)
    c2.remove.assert_called_once_with(force=True)


def test_stop_background_helper_unknown_status_is_a_failure():
    """If status can't be queried (reload raises a non-NotFound error) the run
    is treated as failed rather than silently passing."""
    c = MagicMock()
    c.reload.side_effect = RuntimeError("docker daemon hiccup")
    c.attrs = {}
    failure = stop_background_helper(_bg(c, index=5))
    assert failure is not None
    assert failure[0] == 5 and failure[2] == "unknown"
    c.stop.assert_not_called()
    c.remove.assert_called_once_with(force=True)


def test_stop_background_helper_exit_code_zero_is_still_a_failure():
    """A helper that exits 0 early still means the listeners were not active
    for the full window -> failure (keyed on status 'exited', not exit code)."""
    c = MagicMock()
    c.status = "exited"
    c.attrs = {"State": {"ExitCode": 0}}
    c.logs.return_value = b""
    failure = stop_background_helper(_bg(c, index=6))
    assert failure is not None
    idx, tool, status, code = failure
    assert idx == 6 and status == "exited" and code == 0
    c.remove.assert_called_once_with(force=True)


def _multi_cfg():
    return {
        "clientconfigs": [
            {
                "run_image": "redislabs/memtier_benchmark:edge",
                "tool": "memtier_benchmark",
                "arguments": '--test-time 1 --command "SET __key__ __data__" '
                "-c 1 -t 1 --hide-histogram",
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


def _runner_args():
    return type("A", (), {"benchmark_local_install": False, "timeout_buffer": 1})()


def _call_run_multiple(docker_client, tmp_path):
    from redis_benchmarks_specification.__runner__.runner import run_multiple_clients

    return run_multiple_clients(
        _multi_cfg(),
        docker_client,
        str(tmp_path),
        "/mnt",
        str(tmp_path),
        "0",
        6379,
        "localhost",
        None,
        False,
        False,
        False,
        None,
        None,
        None,
        "2",
        0,
        1,
        "",
        _runner_args(),
    )


def test_run_multiple_clients_aborts_when_background_helper_exited(tmp_path):
    """If the background bcast-listener exited during the run, the whole run
    must fail rather than report the memtier numbers as valid."""
    import pytest

    fg = MagicMock()
    fg.wait.return_value = {"StatusCode": 0}
    fg.logs.return_value = b"{}"
    bg = MagicMock()
    bg.status = "exited"
    bg.attrs = {"State": {"ExitCode": 1}}
    bg.logs.return_value = b""
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [fg, bg]

    with pytest.raises(RuntimeError, match="Background helper"):
        _call_run_multiple(docker_client, tmp_path)
    bg.remove.assert_called_with(force=True)  # helper still cleaned up


def test_run_multiple_clients_cleans_up_on_midlaunch_failure(tmp_path):
    """If a later container fails to start, the already-started one must be
    force-removed (no leak)."""
    import pytest

    started = MagicMock()
    docker_client = MagicMock()
    docker_client.containers.run.side_effect = [started, Exception("pull failed")]

    with pytest.raises(RuntimeError, match="Failed to start"):
        _call_run_multiple(docker_client, tmp_path)
    started.remove.assert_called_once_with(force=True)
