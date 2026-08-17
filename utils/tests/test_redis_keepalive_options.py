#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Pure unit test (no docker / no network) for the TCP keepalive tuning applied to
long/indefinitely-blocking redis connections (e.g. XREADGROUP ... BLOCK 0). Without
explicit socket_keepalive_options, `socket_keepalive=True` alone only enables the OS
default keepalive timers (2h on Linux before the first probe) -- far longer than many
cloud NAT/security-group/LB idle-connection reap windows, which lets an intermediate
hop silently kill a genuinely-idle blocking-read connection.
"""
import ast
import inspect
import socket

from redis_benchmarks_specification.__common__.env import (
    redis_long_blocking_read_keepalive_options,
)
from redis_benchmarks_specification.__builder__ import builder as builder_module
from redis_benchmarks_specification.__self_contained_coordinator__ import (
    self_contained_coordinator as coordinator_module,
)


def test_keepalive_options_present_on_linux():
    opts = redis_long_blocking_read_keepalive_options()
    if not all(
        hasattr(socket, attr)
        for attr in ("TCP_KEEPIDLE", "TCP_KEEPINTVL", "TCP_KEEPCNT")
    ):
        assert opts == {}
        return
    assert opts[socket.TCP_KEEPIDLE] > 0
    assert opts[socket.TCP_KEEPINTVL] > 0
    assert opts[socket.TCP_KEEPCNT] > 0
    # idle-before-first-probe must be well under typical cloud idle-reap windows
    # (AWS NAT gateway default ~350s) so probes refresh any middlebox connection
    # tracking state before it expires.
    assert opts[socket.TCP_KEEPIDLE] < 120


def test_keepalive_options_returns_dict_type():
    # redis-py's socket_keepalive_options must be a plain dict (or {}), never None.
    assert isinstance(redis_long_blocking_read_keepalive_options(), dict)


def _redis_strictredis_kwargs_in(func):
    """Parse `func`'s source and return the keyword-argument names passed to every
    top-level `redis.StrictRedis(...)` call inside it -- an execution-free regression
    guard. `main()` itself isn't practically unit-testable (argparse, an infinite
    consumer loop, Docker), so this catches "the kwarg got dropped from the call
    site" at the source level instead of only in production."""
    tree = ast.parse(inspect.getsource(func))
    return [
        {kw.arg for kw in node.keywords if kw.arg is not None}
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "StrictRedis"
    ]


def test_builder_main_wires_keepalive_options_into_blocking_connection():
    calls = _redis_strictredis_kwargs_in(builder_module.main)
    assert calls, "no redis.StrictRedis(...) call found in builder.main()"
    assert any("socket_keepalive_options" in kwargs for kwargs in calls), (
        "builder.main()'s event-stream connection must pass "
        "socket_keepalive_options=redis_long_blocking_read_keepalive_options() -- "
        "without it, a genuinely idle BLOCK 0 read can be silently killed by an "
        "intermediate NAT/LB hop"
    )


def test_coordinator_main_wires_keepalive_options_into_blocking_connection():
    calls = _redis_strictredis_kwargs_in(coordinator_module.main)
    assert (
        calls
    ), "no redis.StrictRedis(...) call found in self_contained_coordinator.main()"
    assert any("socket_keepalive_options" in kwargs for kwargs in calls), (
        "self_contained_coordinator.main()'s benchmark-stream connection must pass "
        "socket_keepalive_options=redis_long_blocking_read_keepalive_options()"
    )
