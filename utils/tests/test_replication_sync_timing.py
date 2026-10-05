from unittest.mock import Mock, patch

import pytest
import redis

from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
    declared_sync_keyspacelen,
    measure_replica_full_sync,
    retry_primary_probe,
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


@pytest.mark.parametrize("poll_interval", [0, -1, float("inf"), float("nan")])
def test_full_sync_rejects_invalid_poll_interval(poll_interval):
    conn = Mock()
    with pytest.raises(ValueError):
        measure_replica_full_sync(conn, 6379, poll_interval=poll_interval)
    conn.execute_command.assert_not_called()


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


@pytest.mark.parametrize(
    "primary_keys, replica_keys, sync_delta",
    [(19, 20, 1), (20, 19, 1), (20, 20, 0), (20, 20, 2)],
)
def test_full_sync_rejects_dataset_or_sync_count_mismatch(
    primary_keys, replica_keys, sync_delta
):
    from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
        spin_up_redis_replicas,
    )

    normal, timing, primary = Mock(), Mock(), Mock()
    primary.dbsize.return_value = primary_keys
    timing.dbsize.return_value = replica_keys
    primary.info.side_effect = [{"sync_full": 0}, {"sync_full": sync_delta}]
    with patch(
        MODULE + ".redis.StrictRedis", side_effect=[normal, timing, primary]
    ), patch(MODULE + ".start_redis_container"), patch(
        MODULE + ".measure_replica_full_sync", return_value=0.25
    ) as measure:
        with pytest.raises(ValueError):
            spin_up_redis_replicas(
                1,
                6399,
                0,
                Mock(),
                [],
                "redis:8.6",
                "",
                "",
                1,
                {},
                "",
                None,
                expected_keyspacelen=20,
            )
    if primary_keys != 20:
        measure.assert_not_called()
    timing.close.assert_called_once()
    primary.close.assert_called_once()


def test_full_sync_returns_duration_only_after_dataset_and_sync_count_validation():
    from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
        spin_up_redis_replicas,
    )

    normal, timing, primary = Mock(), Mock(), Mock()
    primary.dbsize.return_value = timing.dbsize.return_value = 20
    primary.info.side_effect = [{"sync_full": 0}, {"sync_full": 1}]
    with patch(
        MODULE + ".redis.StrictRedis", side_effect=[normal, timing, primary]
    ), patch(MODULE + ".start_redis_container"), patch(
        MODULE + ".measure_replica_full_sync", return_value=0.25
    ):
        conns, _, times = spin_up_redis_replicas(
            1,
            6399,
            0,
            Mock(),
            [],
            "redis:8.6",
            "",
            "",
            1,
            {},
            "",
            None,
            expected_keyspacelen=20,
        )
    assert conns == [normal]
    assert times == [0.25]
    timing.close.assert_called_once()
    primary.close.assert_called_once()


@pytest.mark.parametrize(
    "dbconfig, preload_done, expected",
    [
        ({"check": {"keyspacelen": 20000000}}, False, None),
        ({"check": {"keyspacelen": 20000000}}, True, 20000000),
        ({"check": {}}, True, None),
        ({"check": None}, True, None),
        ({}, True, None),
        ({}, False, None),
    ],
)
def test_declared_sync_keyspacelen_only_when_preloaded_before_replica(
    dbconfig, preload_done, expected
):
    assert declared_sync_keyspacelen(dbconfig, preload_done) == expected


def test_coordinator_passes_helper_result_to_spin_up_replicas():
    import inspect

    from redis_benchmarks_specification.__self_contained_coordinator__ import (
        self_contained_coordinator as coordinator,
    )

    source = inspect.getsource(coordinator)
    assert "expected_keyspacelen=declared_sync_keyspacelen(" in source
    assert "preload_already_done," in source


def test_primary_probe_retries_transient_timeout_then_succeeds():
    probe = Mock(side_effect=[redis.TimeoutError("slow"), 7])
    with patch(MODULE + ".time.sleep"):
        assert retry_primary_probe(probe) == 7
    assert probe.call_count == 2


def test_primary_probe_is_bounded_and_reraises():
    probe = Mock(side_effect=redis.TimeoutError("stalled"))
    with patch(MODULE + ".time.sleep"):
        with pytest.raises(redis.TimeoutError):
            retry_primary_probe(probe, attempts=3)
    assert probe.call_count == 3


def test_primary_probe_does_not_retry_other_errors():
    probe = Mock(side_effect=redis.ResponseError("denied"))
    with pytest.raises(redis.ResponseError):
        retry_primary_probe(probe)
    assert probe.call_count == 1


def test_valid_sample_survives_one_slow_primary_info():
    from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
        spin_up_redis_replicas,
    )

    normal, timing, primary = Mock(), Mock(), Mock()
    primary.dbsize.return_value = timing.dbsize.return_value = 20
    primary.info.side_effect = [
        {"sync_full": 0},
        redis.TimeoutError("busy after transfer"),
        {"sync_full": 1},
    ]
    with patch(
        MODULE + ".redis.StrictRedis", side_effect=[normal, timing, primary]
    ), patch(MODULE + ".start_redis_container"), patch(
        MODULE + ".measure_replica_full_sync", return_value=0.25
    ), patch(
        MODULE + ".time.sleep"
    ):
        _, _, times = spin_up_redis_replicas(
            1,
            6399,
            0,
            Mock(),
            [],
            "redis:8.6",
            "",
            "",
            1,
            {},
            "",
            None,
            expected_keyspacelen=20,
        )
    assert times == [0.25]


def test_no_declared_keyspacelen_skips_dbsize_checks():
    from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
        spin_up_redis_replicas,
    )

    normal, timing, primary = Mock(), Mock(), Mock()
    primary.info.side_effect = [{"sync_full": 0}, {"sync_full": 1}]
    with patch(
        MODULE + ".redis.StrictRedis", side_effect=[normal, timing, primary]
    ), patch(MODULE + ".start_redis_container"), patch(
        MODULE + ".measure_replica_full_sync", return_value=0.25
    ):
        spin_up_redis_replicas(
            1, 6399, 0, Mock(), [], "redis:8.6", "", "", 1, {}, "", None
        )
    primary.dbsize.assert_not_called()
    timing.dbsize.assert_not_called()
