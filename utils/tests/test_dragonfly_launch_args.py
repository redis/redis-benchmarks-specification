#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Pure unit tests (no docker / no network) for the server_name-aware standalone launch
arg generator and the --build_timeout CLI flag. Validates that:
  * the default (redis) launch path is unchanged,
  * the Dragonfly path emits only valid gflags and never a redis-style flag,
  * redis-only config parameters are dropped / translated for Dragonfly.
"""
import argparse

import pytest

from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
    generate_standalone_redis_server_args,
    generate_standalone_dragonfly_server_args,
    spin_docker_cluster_redis,
    spin_up_redis_replicas,
)
from redis_benchmarks_specification.__cli__.args import spec_cli_args
from redis_benchmarks_specification.__common__.runner import execute_init_commands
import redis


class _RaisingConn:
    """Minimal fake redis conn whose execute_command always raises ResponseError."""

    def execute_command(self, *args, **kwargs):
        raise redis.exceptions.ResponseError("ERR unknown command")


# Flags that Dragonfly's gflags parser would abort on if we ever emitted them.
REDIS_ONLY_FLAGS = {
    "--protected-mode",
    "--logfile",
    "--save",
    "--appendonly",
    "--maxmemory-policy",
    "--rdbcompression",
    "--io-threads",
    "--io-threads-do-reads",
}


def test_redis_default_path_unchanged():
    """Default server_name keeps the exact historical redis-server args."""
    cmd = generate_standalone_redis_server_args(
        "/mnt/redis/redis-server", 6379, "/mnt/redis/"
    )
    assert cmd[0] == "/mnt/redis/redis-server"
    assert "--protected-mode" in cmd and "no" in cmd
    assert "--port" in cmd and "6379" in cmd
    # redis writes a logfile in the data dir
    assert "--logfile" in cmd
    assert "--dir" in cmd


def test_redis_explicit_server_name_matches_default():
    base = generate_standalone_redis_server_args(
        "/mnt/redis/redis-server", 6379, "/mnt/redis/"
    )
    explicit = generate_standalone_redis_server_args(
        "/mnt/redis/redis-server", 6379, "/mnt/redis/", server_name="redis"
    )
    assert base == explicit


def test_dragonfly_dispatch_via_server_name():
    """server_name='dragonfly' routes to the Dragonfly generator."""
    via_dispatch = generate_standalone_redis_server_args(
        "/mnt/redis/dragonfly-server",
        6379,
        "/mnt/redis/",
        server_name="dragonfly",
    )
    direct = generate_standalone_dragonfly_server_args(
        "/mnt/redis/dragonfly-server", 6379, "/mnt/redis/"
    )
    assert via_dispatch == direct


def test_dragonfly_emits_only_valid_gflags():
    cmd = generate_standalone_dragonfly_server_args(
        "/mnt/redis/dragonfly-server", 6379, "/mnt/redis/"
    )
    assert cmd[0] == "/mnt/redis/dragonfly-server"
    assert "--port" in cmd and "6379" in cmd
    assert "--logtostderr" in cmd
    assert "--proactor_threads=1" in cmd
    assert "--dbfilename=" in cmd
    # No redis-only flag must ever appear (Dragonfly would abort).
    for tok in cmd:
        assert tok not in REDIS_ONLY_FLAGS, f"redis-only flag leaked: {tok}"
    # No redis logfile / protected-mode either.
    assert "--protected-mode" not in cmd
    assert "--logfile" not in cmd


def test_dragonfly_drops_redis_only_and_translates_maxmemory():
    cfg = {
        "save": '""',
        "maxmemory-policy": "noeviction",
        "rdbcompression": "no",
        "maxmemory": "1gb",
    }
    cmd = generate_standalone_dragonfly_server_args(
        "/mnt/redis/dragonfly-server", 6379, "/mnt/redis/", configuration_parameters=cfg
    )
    joined = " ".join(cmd)
    # redis-only params dropped
    assert "save" not in joined
    assert "maxmemory-policy" not in joined
    assert "rdbcompression" not in joined
    # maxmemory translated to a gflag (and NOT confused with maxmemory-policy)
    assert "--maxmemory=1gb" in cmd


def test_dragonfly_password_and_dir():
    cmd = generate_standalone_dragonfly_server_args(
        "/mnt/redis/dragonfly-server",
        6379,
        "/mnt/redis/",
        password="secret",
    )
    assert "--requirepass" in cmd
    i = cmd.index("--requirepass")
    assert cmd[i + 1] == "secret"
    assert "--dir" in cmd
    assert "/mnt/redis/" in cmd


def test_dragonfly_proactor_threads_override():
    cmd = generate_standalone_dragonfly_server_args(
        "/mnt/redis/dragonfly-server",
        6379,
        "/mnt/redis/",
        redis_arguments="--proactor_threads=4",
    )
    assert "--proactor_threads=4" in cmd
    assert "--proactor_threads=1" not in cmd


def test_dragonfly_ignores_redis_style_topology_arguments():
    """redis-style topology redis_arguments (e.g. --io-threads) must not leak through."""
    cmd = generate_standalone_dragonfly_server_args(
        "/mnt/redis/dragonfly-server",
        6379,
        "/mnt/redis/",
        redis_arguments="--io-threads 4 --io-threads-do-reads yes",
    )
    joined = " ".join(cmd)
    assert "io-threads" not in joined


def test_build_timeout_cli_flag():
    """--build_timeout parses as an int and defaults to 0 (pipeline default 600s)."""
    parser = argparse.ArgumentParser()
    spec_cli_args(parser)
    default_args = parser.parse_args([])
    assert default_args.build_timeout == 0
    set_args = parser.parse_args(["--build_timeout", "1800"])
    assert set_args.build_timeout == 1800


def test_dragonfly_disables_version_check_phone_home():
    cmd = generate_standalone_dragonfly_server_args(
        "/mnt/redis/dragonfly-server", 6379, "/mnt/redis/"
    )
    assert "--version_check=false" in cmd


def test_dragonfly_proactor_override_space_form():
    """The space form --proactor_threads N must be honored (not just the = form)."""
    cmd = generate_standalone_dragonfly_server_args(
        "/mnt/redis/dragonfly-server",
        6379,
        "/mnt/redis/",
        redis_arguments="--proactor_threads 8",
    )
    assert "--proactor_threads=8" in cmd
    assert "--proactor_threads=1" not in cmd


def test_dragonfly_proactor_defaults_to_one_when_absent():
    cmd = generate_standalone_dragonfly_server_args(
        "/mnt/redis/dragonfly-server",
        6379,
        "/mnt/redis/",
        redis_arguments="--io-threads 4",
    )
    assert "--proactor_threads=1" in cmd


def test_cluster_topology_rejects_dragonfly():
    """Cluster topology must fail loudly for Dragonfly (standalone-only in P1)."""
    with pytest.raises(NotImplementedError):
        spin_docker_cluster_redis(
            1, 1, 0, None, None, [], 6379, "img", "/tmp/", server_name="dragonfly"
        )


def test_replica_topology_rejects_dragonfly():
    """Replica topology must fail loudly for Dragonfly (standalone-only in P1)."""
    with pytest.raises(NotImplementedError):
        spin_up_redis_replicas(
            1,
            6379,
            0,
            None,
            [],
            "img",
            "",
            "/mnt/redis/",
            1,
            None,
            "",
            None,
            server_name="dragonfly",
        )


_INIT_CFG = {"dbconfig": {"init_commands": ["CONFIG SET some-redis-only-param yes"]}}


def test_init_commands_reraise_for_redis():
    """A failing init command still fails the run for redis (historical behavior)."""
    with pytest.raises(redis.exceptions.ResponseError):
        execute_init_commands(_INIT_CFG, _RaisingConn(), server_name="redis")


def test_init_commands_tolerated_for_dragonfly():
    """A failing/unsupported init command is tolerated for non-redis servers."""
    # Should NOT raise.
    execute_init_commands(_INIT_CFG, _RaisingConn(), server_name="dragonfly")
