from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from redis_benchmarks_specification.__self_contained_coordinator__.rdb_checks import (
    RdbChecks,
)


def fixture(operation="load"):
    conn = Mock()
    conn.dbsize.side_effect = [3, 0, 3] if operation == "load" else [3, 3]
    conn.get.return_value = b"abcd"
    conn.save.return_value = True
    command = "debug" if operation == "load" else "save"
    conn.info.side_effect = [
        {"cmdstat_" + command: {"calls": 1, "failed_calls": 0, "rejected_calls": 0}},
        {"rdb_last_load_keys_loaded": 3, "rdb_last_load_keys_expired": 0},
    ]
    check = RdbChecks(
        {
            "operation": operation,
            "keyspacelen": 3,
            "value_size": 4,
            "sample_keys": ["k"],
        },
        [conn],
        "oss-standalone",
        0,
    )
    return conn, check


@pytest.mark.parametrize("operation", ["save", "load"])
def test_rdb_success(operation):
    conn, check = fixture(operation)
    check.prepare()
    check.verify()
    conn.config_resetstat.assert_called_once_with()
    if operation == "save":
        conn.save.assert_not_called()
    else:
        conn.save.assert_called_once_with()
        conn.execute_command.assert_called_once_with("FLUSHALL", "SYNC")


@pytest.mark.parametrize("counts", [[2], [3, 1]])
def test_rdb_rejects_wrong_preload_or_nonempty_load(counts):
    conn, check = fixture()
    conn.dbsize.side_effect = counts
    with pytest.raises(ValueError):
        check.prepare()
    conn.config_resetstat.assert_not_called()


@pytest.mark.parametrize("value", [None, b"abc"])
def test_rdb_rejects_wrong_value_size(value):
    conn, check = fixture()
    conn.get.return_value = value
    with pytest.raises(ValueError, match="size mismatch"):
        check.prepare()


def test_rdb_rejects_corrupted_reload_with_same_length():
    conn, check = fixture()
    conn.get.side_effect = [b"abcd", b"abce"]
    check.prepare()
    with pytest.raises(ValueError, match="values changed"):
        check.verify()


@pytest.mark.parametrize(
    "stats",
    [
        {},
        {"calls": 0},
        {"calls": 2},
        {"calls": 1, "failed_calls": 1, "rejected_calls": 0},
        {"calls": 1, "failed_calls": 0, "rejected_calls": 1},
    ],
)
def test_rdb_rejects_missing_repeated_or_failed_command(stats):
    conn, check = fixture()
    conn.info.side_effect = [{"cmdstat_debug": stats}]
    check.prepare()
    with pytest.raises(ValueError, match="exactly one"):
        check.verify()


@pytest.mark.parametrize(
    "stats",
    [
        {},
        {"rdb_last_load_keys_loaded": 2},
        {"rdb_last_load_keys_loaded": 3, "rdb_last_load_keys_expired": 1},
    ],
)
def test_rdb_rejects_missing_or_invalid_load_counters(stats):
    conn, check = fixture()
    commandstats = conn.info()
    conn.info.side_effect = [commandstats, stats]
    check.prepare()
    with pytest.raises(ValueError, match="load counters"):
        check.verify()


@pytest.mark.parametrize(
    "topology,connections,override",
    [
        ("oss-cluster", [Mock()], 0),
        ("oss-standalone", [Mock(), Mock()], 0),
        ("oss-standalone", [Mock()], 30),
    ],
)
def test_rdb_rejects_unsupported_execution(topology, connections, override):
    with pytest.raises(ValueError):
        RdbChecks({}, connections, topology, override)


def test_sweep_covers_value_sizes_and_routes_through_checks():
    paths = list(
        Path("redis_benchmarks_specification/test-suites").glob(
            "memtier_benchmark-rdb-*.yml"
        )
    )
    assert len(paths) == 24
    cells = set()
    for path in paths:
        spec = yaml.safe_load(path.read_text())
        db = spec["dbconfig"]
        check = db["rdb_checks"]
        size = check["value_size"]
        count = check["keyspacelen"]
        random = "--random-data" in db["preload_tool"]["arguments"]
        payload = "random" if random else "repeated"
        expected_name = f"memtier_benchmark-rdb-{check['operation']}-{size}B-{payload}"
        assert spec["name"] == path.stem == expected_name
        command = (
            "SAVE" if check["operation"] == "save" else "DEBUG RELOAD NOSAVE NOFLUSH"
        )
        assert spec["clientconfig"]["arguments"].startswith(f'--command "{command}" ')
        cells.add((size, check["operation"], random))
        assert db["check"]["keyspacelen"] == count
        assert count % 40 == 0
        assert all(len(k) == 16 for k in check["sample_keys"])
        assert "init_commands" not in db
        assert "--pipeline 1 --hide-histogram" in spec["clientconfig"]["arguments"]
        assert "-n 1 -c 1 -t 1" in spec["clientconfig"]["arguments"]
        assert spec["exporter"]["redistimeseries"]["metrics"] == [
            '$."ALL STATS".Totals.Latency'
        ]
    assert cells == {
        (s, op, r)
        for s in [16, 64, 256, 1024, 4096, 16384]
        for op in ["save", "load"]
        for r in [False, True]
    }
