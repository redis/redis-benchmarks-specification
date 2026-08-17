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
import sys
import types

import redis_benchmarks_specification.__common__.env as env_module
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


def test_keepalive_options_empty_on_platforms_missing_the_socket_constants(
    monkeypatch,
):
    """The Linux-only fallback branch (e.g. macOS local dev) isn't reachable on this
    CI's Linux runners without forcing it -- delete one of the three required attrs
    from a throwaway `socket`-like object and confirm {} comes back rather than a
    KeyError/AttributeError from a partially-built options dict."""
    fake_socket = types.SimpleNamespace(TCP_KEEPIDLE=1, TCP_KEEPINTVL=2)
    # TCP_KEEPCNT deliberately absent, mirroring a non-Linux platform.
    monkeypatch.setitem(sys.modules, "socket", fake_socket)
    assert env_module.redis_long_blocking_read_keepalive_options() == {}


def test_keepalive_options_populated_when_all_socket_constants_present(monkeypatch):
    """The populated-dict branch is otherwise only exercised incidentally (this CI's
    runners happen to be Linux) -- force it explicitly, mirroring the test above, so
    it's deterministic regardless of the host OS running the suite."""
    fake_socket = types.SimpleNamespace(
        TCP_KEEPIDLE="idle", TCP_KEEPINTVL="intvl", TCP_KEEPCNT="cnt"
    )
    monkeypatch.setitem(sys.modules, "socket", fake_socket)
    assert env_module.redis_long_blocking_read_keepalive_options() == {
        "idle": 30,
        "intvl": 10,
        "cnt": 6,
    }


def _redis_strictredis_kwargs_by_target_in(func):
    """Parse `func`'s source and return {assigned-variable-name: kwarg-names} for every
    top-level `<var> = redis.StrictRedis(...)` assignment inside it -- an execution-free
    regression guard. `main()` itself isn't practically unit-testable (argparse, an
    infinite consumer loop, Docker), so this catches "the kwarg got dropped from the
    call site" at the source level instead of only in production.

    Keyed by assignment target rather than collapsed into "any call passes it": both
    builder.main() and self_contained_coordinator.main() construct MORE than one
    redis.StrictRedis client (a blocking event-stream/benchmark-stream consumer, plus
    e.g. a datasink connection) -- an `any(...)` check across every call in the function
    would still pass if the kwarg landed on the wrong one, missing exactly the drop this
    guard exists to catch on the connection it's actually meant to protect."""
    tree = ast.parse(inspect.getsource(func))
    by_target = {}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and node.value.func.attr == "StrictRedis"
        ):
            by_target[node.targets[0].id] = {
                kw.arg for kw in node.value.keywords if kw.arg is not None
            }
    return by_target


def _assert_blocking_connection_has_keepalive(by_target, var_name, where):
    assert (
        by_target
    ), "no `<var> = redis.StrictRedis(...)` assignment found in {}".format(where)
    assert var_name in by_target, (
        "{} no longer assigns its blocking connection to `{}` (found: {}) -- update "
        "this test to the new variable name, and verify the kwarg is still present "
        "before doing so".format(where, var_name, sorted(by_target))
    )
    assert "socket_keepalive_options" in by_target[var_name], (
        "{}'s `{}` (the long-blocking-read connection) must pass "
        "socket_keepalive_options=redis_long_blocking_read_keepalive_options() -- "
        "without it, a genuinely idle BLOCK 0 read can be silently killed by an "
        "intermediate NAT/LB hop. Other redis.StrictRedis(...) connections in {} "
        "having it is not sufficient.".format(where, var_name, where)
    )


def test_builder_main_wires_keepalive_options_into_blocking_connection():
    by_target = _redis_strictredis_kwargs_by_target_in(builder_module.main)
    _assert_blocking_connection_has_keepalive(by_target, "conn", "builder.main()")


def test_coordinator_main_wires_keepalive_options_into_blocking_connection():
    by_target = _redis_strictredis_kwargs_by_target_in(coordinator_module.main)
    _assert_blocking_connection_has_keepalive(
        by_target, "gh_event_conn", "self_contained_coordinator.main()"
    )
