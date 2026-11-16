from copy import deepcopy
from unittest.mock import Mock, patch

import pytest
import redis

from redis_benchmarks_specification.__common__.postrun_assertions import (
    validate_postrun_lua,
)
from redis_benchmarks_specification.__common__.runner import exporter_datasink_common


def config():
    return {
        "dbconfig": {"postrun_lua": "return 1"},
        "clientconfig": {"tool": "memtier_benchmark"},
        "redis-topologies": ["oss-standalone"],
    }


def results():
    return {
        "ALL STATS": {
            "Runtime": {
                "Start time": 1700000000000,
                "Finish time": 1700000001000,
                "Total duration": 1000,
                "Time unit": "MILLISECONDS",
                "Interrupted": "false",
            },
            "Totals": {"Max Latency": 12.5, "Errors": 0},
        }
    }


def test_no_opt_in_does_not_require_memtier_or_a_server():
    validate_postrun_lua({}, [], {})


def test_assertions_receive_actual_runtime_and_must_all_pass():
    spec = config()
    spec["dbconfig"]["postrun_lua"] = ["return 1", "return 2 - 1"]
    conn = Mock()
    conn.execute_command.return_value = 1
    validate_postrun_lua(spec, [conn], results())
    assert conn.execute_command.call_args_list[0].args == (
        "EVAL",
        "return 1",
        0,
        1700000000000,
        1700000001000,
    )
    assert conn.execute_command.call_count == 2


@pytest.mark.parametrize(
    "value",
    [None, "1700000000000", True, -1, 0, float("nan"), float("inf"), 10**1000, 1.5],
)
def test_invalid_runtime_is_rejected_before_server_calls(value):
    data = results()
    data["ALL STATS"]["Runtime"]["Start time"] = value
    conn = Mock()
    with pytest.raises(ValueError):
        validate_postrun_lua(config(), [conn], data)
    conn.execute_command.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("Finish time", 1700000000000),
        ("Total duration", 999),
        ("Time unit", "SECONDS"),
        ("Interrupted", "true"),
        ("Unanswered requests", 1),
    ],
)
def test_incomplete_or_inconsistent_run_is_rejected(field, value):
    data = results()
    data["ALL STATS"]["Runtime"][field] = value
    with pytest.raises(ValueError):
        validate_postrun_lua(config(), [Mock()], data)


def test_missing_runtime_and_client_errors_are_rejected():
    with pytest.raises(ValueError):
        validate_postrun_lua(config(), [Mock()], {"ALL STATS": {}})
    data = results()
    data["ALL STATS"]["Totals"]["Errors"] = 1
    with pytest.raises(ValueError):
        validate_postrun_lua(config(), [Mock()], data)


@pytest.mark.parametrize("response", [0, None, b"1", "1", True])
def test_missing_or_invalid_receipt_is_rejected(response):
    conn = Mock()
    conn.execute_command.return_value = response
    with pytest.raises(ValueError):
        validate_postrun_lua(config(), [conn], results())


@pytest.mark.parametrize(
    "error",
    [redis.ResponseError("assertion failed"), redis.ConnectionError("disconnected")],
)
def test_server_errors_cannot_be_tolerated_as_metric_collection_failures(error):
    conn = Mock()
    conn.execute_command.side_effect = error
    with pytest.raises(ValueError):
        validate_postrun_lua(config(), [conn], results())


def test_ambiguous_client_or_server_is_rejected():
    spec = config()
    spec["clientconfigs"] = [{"tool": "memtier_benchmark"}]
    with pytest.raises(ValueError):
        validate_postrun_lua(spec, [Mock()], results())
    with pytest.raises(ValueError):
        validate_postrun_lua(config(), [Mock(), Mock()], results())


def test_declared_metric_must_exist_and_be_finite():
    spec = config()
    spec["exporter"] = {
        "redistimeseries": {"metrics": ['$."ALL STATS".Totals."Max Latency"']}
    }
    conn = Mock()
    conn.execute_command.return_value = 1
    validate_postrun_lua(spec, [conn], results())
    for value in [None, "12.5", float("nan"), float("inf")]:
        data = results()
        data["ALL STATS"]["Totals"]["Max Latency"] = value
        with pytest.raises(ValueError):
            validate_postrun_lua(spec, [conn], data)
    data = results()
    del data["ALL STATS"]["Totals"]["Max Latency"]
    with pytest.raises(ValueError):
        validate_postrun_lua(spec, [conn], data)


def test_export_is_blocked_before_any_client_or_server_metric_is_published():
    conn = Mock()
    conn.execute_command.side_effect = redis.ConnectionError("lost server")
    args = [
        config(),
        1,
        "build",
        0,
        0,
        Mock(),
        True,
        "main",
        None,
        {},
        [conn],
        results(),
        "platform",
        "setup",
        "oss-standalone",
        "test",
        "redis",
        "redis",
        "ci",
        "oss-standalone",
    ]
    with patch(
        "redis_benchmarks_specification.__common__.runner.timeseries_test_sucess_flow"
    ) as client_export:
        with patch(
            "redis_benchmarks_specification.__common__.runner.export_redis_metrics"
        ) as server_export:
            with pytest.raises(ValueError):
                exporter_datasink_common(*args)
    client_export.assert_not_called()
    server_export.assert_not_called()


def test_successful_assertion_precedes_export():
    conn = Mock()
    events = []
    conn.execute_command.side_effect = lambda *args: events.append("assert") or 1
    args = [
        config(),
        1,
        "build",
        0,
        0,
        Mock(),
        True,
        "main",
        None,
        {},
        [conn],
        deepcopy(results()),
        "platform",
        "setup",
        "oss-standalone",
        "test",
        "redis",
        "redis",
        "ci",
        "oss-standalone",
    ]
    with patch(
        "redis_benchmarks_specification.__common__.runner.timeseries_test_sucess_flow",
        side_effect=lambda *a, **k: events.append("export"),
    ):
        exporter_datasink_common(
            *args,
            collect_memory_metrics=False,
            collect_commandstats=False,
            collect_cpu_stats=False
        )
    assert events == ["assert", "export"]
