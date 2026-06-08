#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from redis_benchmarks_specification.__runner__.runner import (
    prepare_bcast_listener_parameters,
)


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
