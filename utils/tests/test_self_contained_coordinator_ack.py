# Regression tests for redis/redis-benchmarks-specification#519:
# a consumer must always XACK the message it was actually delivered,
# even when process_self_contained_coordinator_stream() raises or
# returns a stream_id that doesn't match the delivered one. Pure
# mock-based unit tests -- no live redis/docker required.
from unittest.mock import MagicMock, patch

import redis.exceptions

from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    self_contained_coordinator_blocking_read,
    _poll_for_work_with_retry,
    _initial_stream_id,
    MAX_CONSECUTIVE_CONNECTION_ERRORS,
)


def _make_delivered_message(msg_id=b"1787194724560-0"):
    # Shape of redis-py's xreadgroup() response:
    # [[stream_name, [(msg_id, {field: value, ...})]]]
    return [[b"oss:api:gh/redis/redis/builds:amd64", [(msg_id, {b"f": b"v"})]]]


def test_blocking_read_acks_captured_id_when_processor_raises():
    msg_id = b"1787194724560-0"
    conn = MagicMock()
    conn.xreadgroup.return_value = _make_delivered_message(msg_id)
    conn.xack.return_value = 1

    with patch(
        "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator.process_self_contained_coordinator_stream",
        side_effect=RuntimeError("boom"),
    ):
        overall_result, stream_id, num_streams, num_suites = (
            self_contained_coordinator_blocking_read(
                conn,
                None,
                None,
                "/tmp",
                ">",
                None,
                {},
                {},
                "x86-aws-m8a.metal-24xl",
                False,
                [],
                arch="amd64",
            )
        )

    # The message must be acked exactly once, on the id it was ACTUALLY
    # delivered under -- never left pending because the processor blew up.
    conn.xack.assert_called_once()
    acked_id = conn.xack.call_args[0][2]
    assert acked_id == msg_id
    assert overall_result is False


def test_blocking_read_acks_delivered_id_not_processor_sentinel():
    msg_id = b"1787194724560-0"
    conn = MagicMock()
    conn.xreadgroup.return_value = _make_delivered_message(msg_id)
    conn.xack.return_value = 1

    # Processor "succeeds" but returns an unrelated sentinel stream_id
    # (e.g. "n/a", or an early-return id) instead of the real one.
    with patch(
        "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator.process_self_contained_coordinator_stream",
        return_value=("n/a", True, 1),
    ):
        self_contained_coordinator_blocking_read(
            conn,
            None,
            None,
            "/tmp",
            ">",
            None,
            {},
            {},
            "x86-aws-m8a.metal-24xl",
            False,
            [],
            arch="amd64",
        )

    conn.xack.assert_called_once()
    acked_id = conn.xack.call_args[0][2]
    assert acked_id == msg_id, (
        "must ack the id the broker actually delivered, not whatever "
        "sentinel the processor happens to return"
    )


def test_blocking_read_acks_delivered_id_on_normal_success():
    msg_id = b"1787194724560-0"
    conn = MagicMock()
    conn.xreadgroup.return_value = _make_delivered_message(msg_id)
    conn.xack.return_value = 1

    # Processor succeeds and (correctly) echoes the same id back -- pins the
    # ack-id contract for the common case too, not just the two failure
    # modes above.
    with patch(
        "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator.process_self_contained_coordinator_stream",
        return_value=(msg_id.decode(), True, 1),
    ):
        overall_result, stream_id, num_streams, num_suites = (
            self_contained_coordinator_blocking_read(
                conn,
                None,
                None,
                "/tmp",
                ">",
                None,
                {},
                {},
                "x86-aws-m8a.metal-24xl",
                False,
                [],
                arch="amd64",
            )
        )

    conn.xack.assert_called_once()
    acked_id = conn.xack.call_args[0][2]
    assert acked_id == msg_id
    assert overall_result is True
    assert num_streams == 1


# Regression tests for the crash-loop half of #519: a ~8126s-periodic
# redis.exceptions.TimeoutError from the blocking XREADGROUP call, observed
# live on x86-aws-m8a.metal-24xl, crashed the whole coordinator process on
# every occurrence because main()'s while loop had no try/except around it.


def test_poll_for_work_with_retry_survives_timeout_error():
    with patch(
        "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator.self_contained_coordinator_blocking_read",
        side_effect=redis.exceptions.TimeoutError("Timeout reading from socket"),
    ), patch("time.sleep") as mock_sleep:
        stream_id, errors = _poll_for_work_with_retry(
            0, stream_id=">", some_other_kwarg="x"
        )

    # Must not raise (that's the whole point -- this used to kill the
    # process), must preserve the stream_id for the next attempt, and must
    # count the failure so a persistent outage backs off instead of
    # busy-looping.
    assert stream_id == ">"
    assert errors == 1
    mock_sleep.assert_called_once_with(2)


def test_poll_for_work_with_retry_survives_connection_error_and_backs_off():
    with patch(
        "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator.self_contained_coordinator_blocking_read",
        side_effect=redis.exceptions.ConnectionError("Connection reset by peer"),
    ), patch("time.sleep") as mock_sleep:
        stream_id, errors = _poll_for_work_with_retry(3, stream_id="12345-0")

    assert stream_id == "12345-0"
    assert errors == 4
    # backoff is min(errors * 2, 30) computed on the POST-increment count
    mock_sleep.assert_called_once_with(8)


def test_poll_for_work_with_retry_resets_error_count_on_success():
    with patch(
        "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator.self_contained_coordinator_blocking_read",
        return_value=(True, ">", 1, 1),
    ):
        stream_id, errors = _poll_for_work_with_retry(5, stream_id="9999-0")

    assert stream_id == ">"
    assert errors == 0


def test_poll_for_work_with_retry_does_not_swallow_other_exceptions():
    # Only the two transient-connection exception types are caught -- a
    # genuine bug (e.g. TypeError from a bad call site) must still surface
    # loudly rather than be silently retried forever.
    with patch(
        "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator.self_contained_coordinator_blocking_read",
        side_effect=TypeError("unrelated bug"),
    ):
        try:
            _poll_for_work_with_retry(0, stream_id=">")
            assert False, "expected TypeError to propagate"
        except TypeError:
            pass


def test_poll_for_work_with_retry_gives_up_after_persistent_outage():
    # A transient blip should retry silently, but a PERSISTENT connection
    # failure (revoked credentials, endpoint genuinely gone) must eventually
    # re-raise rather than retry forever -- the heartbeat thread keeps
    # reporting "waiting" the whole time this loop is stuck, so an infinite
    # silent retry would be a quieter version of the exact blind spot #519
    # is about. Re-raising restores the old crash+respawn alerting signal.
    with patch(
        "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator.self_contained_coordinator_blocking_read",
        side_effect=redis.exceptions.TimeoutError("Timeout reading from socket"),
    ), patch("time.sleep"):
        try:
            _poll_for_work_with_retry(MAX_CONSECUTIVE_CONNECTION_ERRORS, stream_id=">")
            assert False, "expected the persistent outage to re-raise"
        except redis.exceptions.TimeoutError:
            pass


def test_poll_for_work_with_retry_stays_silent_below_the_giveup_threshold():
    with patch(
        "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator.self_contained_coordinator_blocking_read",
        side_effect=redis.exceptions.TimeoutError("Timeout reading from socket"),
    ), patch("time.sleep") as mock_sleep:
        stream_id, errors = _poll_for_work_with_retry(
            MAX_CONSECUTIVE_CONNECTION_ERRORS - 1, stream_id=">"
        )

    assert stream_id == ">"
    assert errors == MAX_CONSECUTIVE_CONNECTION_ERRORS
    mock_sleep.assert_called_once()


# Regression tests for the other half of #519: --skip-clear-pending-on-startup
# preserves this consumer's PEL across a restart, but a fresh process
# previously always started at ">" (new work only) regardless, so that
# preserved backlog was never actually retried -- just an inert liability
# that grows across every crash-restart cycle.


def test_initial_stream_id_drains_own_history_when_skip_clear_pending():
    args = MagicMock(skip_clear_pending_on_startup=True, consumer_start_id=">")
    assert _initial_stream_id(args) == "0"


def test_initial_stream_id_uses_consumer_start_id_by_default():
    args = MagicMock(skip_clear_pending_on_startup=False, consumer_start_id=">")
    assert _initial_stream_id(args) == ">"


def test_initial_stream_id_respects_explicit_consumer_start_id_override():
    # An operator who explicitly overrides --consumer-start-id (e.g. to
    # replay from a specific point) should still get that value when
    # --skip-clear-pending-on-startup is NOT set -- only the skip-clear path
    # forces "0".
    args = MagicMock(skip_clear_pending_on_startup=False, consumer_start_id="12345-0")
    assert _initial_stream_id(args) == "12345-0"
