from unittest.mock import Mock, patch

import pytest
import redis

from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
    measure_replica_full_sync,
)

MODULE = "redis_benchmarks_specification.__self_contained_coordinator__.docker"


def replica(*observations):
    conn = Mock()
    conn.dbsize.return_value = 0
    conn.info.side_effect = [{"role": "master"}, *observations]
    return conn


def test_full_sync_includes_command_and_observation_latency():
    conn = replica(
        {"role": "slave", "master_link_status": "up", "master_sync_in_progress": 0}
    )
    with patch(MODULE + ".time.monotonic", side_effect=[10, 10.2, 10.7]):
        assert measure_replica_full_sync(conn, 6379) == pytest.approx(0.7)
    conn.execute_command.assert_called_once_with("REPLICAOF", "localhost", 6379)


def test_full_sync_late_response_is_failure_not_timeout_datapoint():
    conn = replica(
        {"role": "slave", "master_link_status": "up", "master_sync_in_progress": 0}
    )
    with patch(MODULE + ".time.monotonic", side_effect=[10, 10.2, 12]):
        with pytest.raises(TimeoutError):
            measure_replica_full_sync(conn, 6379, timeout=1)


def test_full_sync_missing_state_does_not_prove_completion():
    conn = replica({"role": "slave", "master_link_status": "up"})
    with patch(MODULE + ".time.monotonic", side_effect=[0, 0, 0.5, 1]), patch(
        MODULE + ".time.sleep"
    ):
        with pytest.raises(TimeoutError):
            measure_replica_full_sync(conn, 6379, timeout=1)


@pytest.mark.parametrize(
    "info, count", [({"role": "slave"}, 0), ({"role": "master"}, 1), ({}, 0)]
)
def test_full_sync_rejects_preexisting_replica_or_data(info, count):
    conn = Mock()
    conn.info.return_value = info
    conn.dbsize.return_value = count
    with pytest.raises(ValueError):
        measure_replica_full_sync(conn, 6379)
    conn.execute_command.assert_not_called()


def test_full_sync_connection_failure_cannot_create_duration():
    conn = replica()
    conn.execute_command.side_effect = redis.TimeoutError("stalled")
    with pytest.raises(redis.TimeoutError):
        measure_replica_full_sync(conn, 6379)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_full_sync_rejects_invalid_deadline(timeout):
    with pytest.raises(ValueError):
        measure_replica_full_sync(Mock(), 6379, timeout=timeout)


@pytest.mark.parametrize(
    "error", [redis.TimeoutError("busy"), redis.BusyLoadingError("loading")]
)
def test_full_sync_retries_busy_loading_without_resetting_deadline(error):
    conn = replica(
        error,
        {"role": "slave", "master_link_status": "up", "master_sync_in_progress": 0},
    )
    with patch(MODULE + ".time.monotonic", side_effect=[0, 0, 0.4, 0.5, 0.8]), patch(
        MODULE + ".time.sleep"
    ):
        assert measure_replica_full_sync(conn, 6379, timeout=1) == 0.8


def test_replica_readiness_timeout_is_bounded_and_preserves_cleanup_tracking():
    from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
        spin_up_redis_replicas,
    )

    normal, timing, primary = Mock(), Mock(), Mock()
    timing.ping.side_effect = redis.TimeoutError("unresponsive process")
    containers = []
    container = Mock()

    def started(*args, **kwargs):
        args[4].append(container)

    with patch(
        MODULE + ".redis.StrictRedis", side_effect=[normal, timing, primary]
    ) as clients, patch(
        MODULE + ".start_redis_container", side_effect=started
    ) as start:
        with pytest.raises(redis.TimeoutError):
            spin_up_redis_replicas(
                1, 6399, 0, Mock(), containers, "redis:8.6", "", "", 1, {}, "", None
            )
    assert containers == [container]
    assert "--replicaof no one" in start.call_args.args[0]
    assert clients.call_args_list[1].kwargs["socket_timeout"] == 1
    normal.ping.assert_not_called()
    timing.close.assert_called_once()
    primary.close.assert_called_once()
