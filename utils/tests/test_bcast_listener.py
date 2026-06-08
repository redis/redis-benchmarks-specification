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
