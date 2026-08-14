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
import socket

from redis_benchmarks_specification.__common__.env import (
    redis_long_blocking_read_keepalive_options,
)


def test_keepalive_options_present_on_linux():
    opts = redis_long_blocking_read_keepalive_options()
    if not all(
        hasattr(socket, attr) for attr in ("TCP_KEEPIDLE", "TCP_KEEPINTVL", "TCP_KEEPCNT")
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
