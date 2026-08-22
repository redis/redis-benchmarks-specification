# Regression tests for redis/redis-benchmarks-specification#519:
# a consumer must always XACK the message it was actually delivered,
# even when process_self_contained_coordinator_stream() raises or
# returns a stream_id that doesn't match the delivered one. Pure
# mock-based unit tests -- no live redis/docker required.
from unittest.mock import MagicMock, patch

from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    self_contained_coordinator_blocking_read,
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
