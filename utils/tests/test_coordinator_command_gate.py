"""The coordinator skips a suite whose tested-commands the server does not implement,
before any preload, like the client runner does."""

import inspect
from unittest import mock

import pytest

from redis_benchmarks_specification.__runner__.runner import (
    check_test_command_support,
)
from redis_benchmarks_specification.__self_contained_coordinator__ import (
    self_contained_coordinator as coordinator,
)
from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    UnsupportedTestedCommands,
    ensure_tested_commands_supported,
)

JSON_SUITE = {"tested-commands": ["json.set", "json.get"]}


def _conn_with_commands(*names):
    conn = mock.Mock()
    conn.execute_command.return_value = {name: {} for name in names}
    return conn


def test_subcommand_is_checked_by_its_container():
    supported = {"CLIENT", "SET"}
    assert check_test_command_support(
        {"tested-commands": ["client|list"]}, supported
    ) == (True, [])
    assert check_test_command_support(
        {"tested-commands": ["config|get"]}, supported
    ) == (False, ["config|get"])


def test_missing_module_command_raises_skip():
    conn = _conn_with_commands("set", "get", "hset")
    with pytest.raises(UnsupportedTestedCommands, match="json.set"):
        ensure_tested_commands_supported(JSON_SUITE, [conn], "json-suite")


def test_supported_commands_pass():
    conn = _conn_with_commands("set", "json.set", "json.get")
    ensure_tested_commands_supported(JSON_SUITE, [conn], "json-suite")


def test_suite_without_tested_commands_passes():
    conn = _conn_with_commands("set")
    ensure_tested_commands_supported({}, [conn], "plain-suite")


def test_fails_open_when_command_is_unavailable():
    conn = mock.Mock()
    conn.execute_command.side_effect = Exception("unknown command 'COMMAND'")
    ensure_tested_commands_supported(JSON_SUITE, [conn], "json-suite")


def test_gate_runs_before_preload_and_is_a_clean_skip():
    src = inspect.getsource(coordinator.process_self_contained_coordinator_stream)
    gate = src.index("ensure_tested_commands_supported(")
    # Both preload call sites (preload_before_replica and the regular one) and the
    # init commands come after the gate.
    preloads = [
        i for i in range(len(src)) if src.startswith("data_prepopulation_step(", i)
    ]
    assert len(preloads) == 2 and gate < min(preloads)
    assert gate < src.index("execute_init_commands(")
    # The skip is caught before the generic handler that fails the test.
    skip_handler = src.index("except UnsupportedTestedCommands")
    assert gate < skip_handler < src.index("except Exception as e:", skip_handler)
