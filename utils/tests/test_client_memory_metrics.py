from unittest.mock import Mock

import pytest

from redis_benchmarks_specification.__common__.client_memory_metrics import (
    inject_client_memory_metrics,
    snapshot_client_memory_metrics,
)
from redis_benchmarks_specification.__common__.runner import (
    collect_redis_metrics,
    CPU_STATS_SECTION_FILTER,
)


def snapshot(cpu=20, calls=500, evicted=0, run_id="server-a"):
    return dict(cpu=cpu, calls=calls, evicted=evicted, run_id=run_id)


def test_phase_deltas_exclude_preload_and_preserve_raw_result_fields():
    result = {"ALL STATS": {"Totals": {"Ops/sec": 100}}}
    inject_client_memory_metrics(
        {"client-memory-metrics": {"max-evicted-clients": 0}},
        result,
        snapshot(cpu=20, calls=50),
        snapshot(cpu=22, calls=150),
    )
    assert result["ALL STATS"]["Totals"] == {
        "Ops/sec": 100,
        "ClientPhaseUserCpuSeconds": 2,
        "ClientPhaseGetCalls": 100,
        "ClientPhaseUserCpuSecondsPerGet": 0.02,
        "ClientPhaseEvictedClients": 0,
    }


@pytest.mark.parametrize(
    "after",
    [
        snapshot(run_id="new-server"),
        snapshot(cpu=19),
        snapshot(calls=499),
        snapshot(),
        snapshot(evicted=-1),
    ],
)
def test_invalid_window_rejected_before_injection(after):
    result = {"ALL STATS": {"Totals": {}}}
    with pytest.raises(ValueError):
        inject_client_memory_metrics(
            {"client-memory-metrics": {}}, result, snapshot(), after
        )
    assert result["ALL STATS"]["Totals"] == {}


@pytest.mark.parametrize(
    "options,evictions",
    [
        ({"min-evicted-clients": 1}, 0),
        ({"max-evicted-clients": 0}, 1),
    ],
)
def test_eviction_acceptance_guard(options, evictions):
    with pytest.raises(ValueError, match="eviction count"):
        inject_client_memory_metrics(
            {"client-memory-metrics": options},
            {"ALL STATS": {"Totals": {}}},
            snapshot(),
            snapshot(calls=600, evicted=evictions),
        )


def test_eviction_positive_delta_exported():
    result = {"ALL STATS": {"Totals": {}}}
    inject_client_memory_metrics(
        {"client-memory-metrics": {"min-evicted-clients": 1}},
        result,
        snapshot(evicted=100),
        snapshot(calls=600, evicted=117),
    )
    assert result["ALL STATS"]["Totals"]["ClientPhaseEvictedClients"] == 17


def test_disabled_metrics_do_not_query_or_change_results():
    conn = Mock()
    assert snapshot_client_memory_metrics({}, [conn]) is None
    results = {}
    inject_client_memory_metrics({}, results, None, None)
    assert results == {}
    conn.info.assert_not_called()


def test_snapshot_uses_one_server_all_sections_and_accepts_no_prior_get():
    conn = Mock()
    conn.info.return_value = {
        "run_id": "a",
        "used_cpu_user_main_thread": 4.0,
        "evicted_clients": 0,
    }
    assert snapshot_client_memory_metrics({"client-memory-metrics": {}}, [conn]) == {
        "run_id": "a",
        "cpu": 4.0,
        "calls": 0,
        "evicted": 0,
    }
    conn.info.assert_called_once_with("all")


@pytest.mark.parametrize("cpu", [float("nan"), float("inf"), -1, True])
def test_invalid_counter_rejected(cpu):
    conn = Mock()
    conn.info.return_value = {
        "run_id": "a",
        "used_cpu_user_main_thread": cpu,
        "evicted_clients": 0,
    }
    with pytest.raises(ValueError):
        snapshot_client_memory_metrics({"client-memory-metrics": {}}, [conn])


def test_info_failure_is_fatal_not_a_redis_error_swallowed_by_exporter():
    import redis

    conn = Mock()
    conn.info.side_effect = redis.exceptions.ConnectionError("gone")
    with pytest.raises(RuntimeError):
        snapshot_client_memory_metrics({"client-memory-metrics": {}}, [conn])


def test_server_counter_collection_includes_evicted_clients():
    conn = Mock()
    conn.info.return_value = {"evicted_clients": 17, "instantaneous_ops_per_sec": 9}
    _, _, metrics = collect_redis_metrics([conn], ["stats"], CPU_STATS_SECTION_FILTER)
    assert metrics == {"stats_evicted_clients": 17}


def test_required_cap_is_verified_before_a_counter_snapshot():
    conn = Mock()
    conn.config_get.return_value = {"maxmemory-clients": "0"}
    with pytest.raises(RuntimeError, match="Cannot snapshot"):
        snapshot_client_memory_metrics(
            {"client-memory-metrics": {"maxmemory-clients": 1048576}}, [conn]
        )
    conn.info.assert_not_called()


def test_eviction_workload_rejects_reused_standalone_server_before_preload():
    from redis_benchmarks_specification.__common__.client_memory_metrics import (
        validate_client_memory_runner,
    )

    validate_client_memory_runner({})
    validate_client_memory_runner({"client-memory-metrics": {}})
    with pytest.raises(ValueError, match="fresh server"):
        validate_client_memory_runner(
            {"client-memory-metrics": {"coordinator-only": True}}
        )


@pytest.mark.parametrize(
    "options",
    [
        {"min-evicted-clients": -1},
        {"max-evicted-clients": True},
        {"maxmemory-clients": "1mb"},
        {"min-evicted-clients": 2, "max-evicted-clients": 1},
    ],
)
def test_bad_options_rejected_without_server_access(options):
    conn = Mock()
    with pytest.raises(ValueError):
        snapshot_client_memory_metrics({"client-memory-metrics": options}, [conn])
    conn.info.assert_not_called()
