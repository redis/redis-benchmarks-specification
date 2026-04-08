"""
Tests for topdown_profiler module.

Unit tests for availability detection, label building, and collection logic.
These tests mock subprocess calls so they run on any platform (including CI
which is ubuntu without topdown-profiler or Intel PMU access).
"""

import platform
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from redis_benchmarks_specification.__self_contained_coordinator__.topdown_profiler import (
    build_topdown_labels,
    check_perf_event_paranoid,
    check_topdown_available,
    collect_topdown,
    extract_topdown_labels_from_benchmark,
    TopdownCollector,
)


# ---------------------------------------------------------------------------
# check_topdown_available
# ---------------------------------------------------------------------------


@patch("platform.machine", return_value="aarch64")
@patch("shutil.which", return_value="/usr/bin/topdown")
def test_check_topdown_available_arm_rejected(mock_which, mock_machine):
    """ARM architecture should be rejected — TMA is Intel-only."""
    assert check_topdown_available() is False


@patch("platform.machine", return_value="x86_64")
@patch("shutil.which", return_value=None)
def test_check_topdown_available_not_in_path(mock_which, mock_machine):
    """If topdown command is not in PATH, should return False."""
    assert check_topdown_available() is False


@patch("platform.machine", return_value="x86_64")
@patch("shutil.which", return_value="/usr/local/bin/topdown")
@patch("subprocess.run")
def test_check_topdown_available_version_fails(mock_run, mock_which, mock_machine):
    """If topdown version returns non-zero, should return False."""
    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error")
    assert check_topdown_available() is False


@patch("platform.machine", return_value="x86_64")
@patch("shutil.which", return_value="/usr/local/bin/topdown")
@patch("subprocess.run")
def test_check_topdown_available_version_timeout(mock_run, mock_which, mock_machine):
    """If topdown version times out, should return False."""
    import subprocess

    mock_run.side_effect = subprocess.TimeoutExpired(cmd="topdown", timeout=10)
    assert check_topdown_available() is False


@patch("platform.machine", return_value="x86_64")
@patch("shutil.which", return_value="/usr/local/bin/topdown")
@patch("subprocess.run")
def test_check_topdown_available_success(mock_run, mock_which, mock_machine):
    """Happy path: x86_64, in PATH, version succeeds."""
    mock_run.return_value = MagicMock(returncode=0, stdout="topdown-profiler 0.1.0")
    assert check_topdown_available() is True


# ---------------------------------------------------------------------------
# check_perf_event_paranoid
# ---------------------------------------------------------------------------


@patch("os.geteuid", return_value=0)
def test_check_perf_event_paranoid_root(mock_euid):
    """Root always has PMU access."""
    assert check_perf_event_paranoid() is True


@patch("os.geteuid", return_value=1000)
@patch("builtins.open", mock.mock_open(read_data="1\n"))
def test_check_perf_event_paranoid_ok(mock_euid):
    """perf_event_paranoid=1 should be allowed."""
    assert check_perf_event_paranoid() is True


@patch("os.geteuid", return_value=1000)
@patch("builtins.open", mock.mock_open(read_data="-1\n"))
def test_check_perf_event_paranoid_minus_one(mock_euid):
    """perf_event_paranoid=-1 should be allowed."""
    assert check_perf_event_paranoid() is True


@patch("os.geteuid", return_value=1000)
@patch("builtins.open", mock.mock_open(read_data="4\n"))
def test_check_perf_event_paranoid_too_high(mock_euid):
    """perf_event_paranoid=4 (Ubuntu default) should return False."""
    assert check_perf_event_paranoid() is False


@patch("os.geteuid", return_value=1000)
@patch("builtins.open", side_effect=FileNotFoundError)
def test_check_perf_event_paranoid_file_missing(mock_open, mock_euid):
    """Missing /proc file should return False gracefully."""
    assert check_perf_event_paranoid() is False


# ---------------------------------------------------------------------------
# build_topdown_labels
# ---------------------------------------------------------------------------


def test_build_topdown_labels_empty():
    """Empty dict should produce empty list."""
    assert build_topdown_labels({}) == []


def test_build_topdown_labels_basic():
    """Basic labels should produce --label key=value pairs."""
    result = build_topdown_labels({"git_branch": "unstable", "test_name": "set-get"})
    assert result == [
        "--label",
        "git_branch=unstable",
        "--label",
        "test_name=set-get",
    ]


def test_build_topdown_labels_none_value_skipped():
    """None values should be skipped."""
    result = build_topdown_labels({"git_branch": "unstable", "optional": None})
    assert result == ["--label", "git_branch=unstable"]


def test_build_topdown_labels_sanitizes_quotes():
    """Quotes in values should be stripped."""
    result = build_topdown_labels({"variant": 'release"debug'})
    assert result == ["--label", "variant=releasedebug"]


def test_build_topdown_labels_integer_value():
    """Integer values should be converted to string."""
    result = build_topdown_labels({"level": 3})
    assert result == ["--label", "level=3"]


# ---------------------------------------------------------------------------
# extract_topdown_labels_from_benchmark
# ---------------------------------------------------------------------------


def test_extract_topdown_labels_merges_all_tiers():
    """Labels from all three tiers should be merged, later tiers taking precedence."""
    startup = {"platform": "aws", "arch": "x86_64"}
    build = {"git_branch": "unstable", "git_hash": "abc123"}
    test = {"test_name": "set-get-100", "topology": "oss-standalone"}

    result = extract_topdown_labels_from_benchmark(startup, build, test)

    assert result == {
        "platform": "aws",
        "arch": "x86_64",
        "git_branch": "unstable",
        "git_hash": "abc123",
        "test_name": "set-get-100",
        "topology": "oss-standalone",
    }


def test_extract_topdown_labels_later_tier_overrides():
    """Test-level labels should override startup-level if same key."""
    startup = {"platform": "aws"}
    build = {"platform": "gcp"}  # override
    test = {}

    result = extract_topdown_labels_from_benchmark(startup, build, test)
    assert result["platform"] == "gcp"


# ---------------------------------------------------------------------------
# collect_topdown
# ---------------------------------------------------------------------------


@patch("subprocess.run")
def test_collect_topdown_success(mock_run):
    """Successful collection should return the run ID."""
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout=(
            "Found 1 PID(s) for 'redis-server': [12345]\n"
            "Collecting level 2 data for 30s...\n"
            "Done. Run ID: 7f3a2b1c-abcd-1234-ef56-789012345678\n"
            "  Samples: 2340 | Duration: 30.2s\n"
        ),
        stderr="",
    )

    run_id = collect_topdown(
        process_name="redis-server",
        duration_seconds=30,
        level=2,
        labels={"git_branch": "unstable"},
    )

    assert run_id == "7f3a2b1c-abcd-1234-ef56-789012345678"

    # Verify the command was constructed correctly
    cmd = mock_run.call_args[0][0]
    assert cmd[0] == "topdown"
    assert cmd[1] == "collect"
    assert "--process" in cmd
    assert "redis-server" in cmd
    assert "--level" in cmd
    assert "2" in cmd
    assert "--duration" in cmd
    assert "30s" in cmd
    assert "--label" in cmd
    assert "git_branch=unstable" in cmd


@patch("subprocess.run")
def test_collect_topdown_failure(mock_run):
    """Failed collection should return None."""
    mock_run.return_value = MagicMock(
        returncode=1,
        stdout="",
        stderr="Error: process not found",
    )

    run_id = collect_topdown(
        process_name="redis-server",
        duration_seconds=30,
        level=2,
        labels={},
    )

    assert run_id is None


@patch("subprocess.run")
def test_collect_topdown_timeout(mock_run):
    """Timeout should return None."""
    import subprocess

    mock_run.side_effect = subprocess.TimeoutExpired(cmd="topdown", timeout=90)

    run_id = collect_topdown(
        process_name="redis-server",
        duration_seconds=30,
        level=2,
        labels={},
    )

    assert run_id is None


@patch("subprocess.run")
def test_collect_topdown_custom_db_path(mock_run):
    """Custom db_path should be passed as TOPDOWN_DB_PATH env var."""
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout="Done. Run ID: abc123\n",
        stderr="",
    )

    collect_topdown(
        process_name="redis-server",
        duration_seconds=10,
        level=3,
        labels={},
        db_path="/tmp/test.db",
    )

    # Check env was passed
    call_kwargs = mock_run.call_args[1]
    assert call_kwargs["env"]["TOPDOWN_DB_PATH"] == "/tmp/test.db"


@patch("subprocess.run")
def test_collect_topdown_multiple_labels(mock_run):
    """Multiple labels should all appear in the command."""
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout="Done. Run ID: abc123\n",
        stderr="",
    )

    labels = {
        "git_branch": "unstable",
        "test_name": "set-get-100",
        "topology": "oss-standalone",
        "build_variant": "release",
    }
    collect_topdown(
        process_name="redis-server",
        duration_seconds=30,
        level=2,
        labels=labels,
    )

    cmd = mock_run.call_args[0][0]
    label_args = [cmd[i + 1] for i, v in enumerate(cmd) if v == "--label"]
    assert "git_branch=unstable" in label_args
    assert "test_name=set-get-100" in label_args
    assert "topology=oss-standalone" in label_args
    assert "build_variant=release" in label_args


# ---------------------------------------------------------------------------
# TopdownCollector
# ---------------------------------------------------------------------------


@patch(
    "redis_benchmarks_specification.__self_contained_coordinator__"
    ".topdown_profiler.collect_topdown"
)
def test_topdown_collector_success(mock_collect):
    """TopdownCollector should start, collect, and return run ID."""
    mock_collect.return_value = "run-id-123"

    collector = TopdownCollector(
        process_name="redis-server",
        duration_seconds=10,
        level=2,
        labels={"test_name": "set-get"},
    )
    collector.start()
    run_id = collector.wait_for_completion(timeout=5)

    assert run_id == "run-id-123"
    mock_collect.assert_called_once_with(
        process_name="redis-server",
        duration_seconds=10,
        level=2,
        labels={"test_name": "set-get"},
        db_path=None,
    )


@patch(
    "redis_benchmarks_specification.__self_contained_coordinator__"
    ".topdown_profiler.collect_topdown"
)
def test_topdown_collector_failure(mock_collect):
    """TopdownCollector should return None on collection failure."""
    mock_collect.return_value = None

    collector = TopdownCollector(
        process_name="redis-server",
        duration_seconds=10,
        level=2,
    )
    collector.start()
    run_id = collector.wait_for_completion(timeout=5)

    assert run_id is None


def test_topdown_collector_not_started():
    """Calling wait without start should return None."""
    collector = TopdownCollector(
        process_name="redis-server",
        duration_seconds=10,
        level=2,
    )
    run_id = collector.wait_for_completion(timeout=1)
    assert run_id is None


@patch(
    "redis_benchmarks_specification.__self_contained_coordinator__"
    ".topdown_profiler.collect_topdown"
)
def test_topdown_collector_exception(mock_collect):
    """TopdownCollector should handle exceptions gracefully."""
    mock_collect.side_effect = RuntimeError("unexpected error")

    collector = TopdownCollector(
        process_name="redis-server",
        duration_seconds=10,
        level=2,
    )
    collector.start()
    run_id = collector.wait_for_completion(timeout=5)

    assert run_id is None
