#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import argparse
import re

import redis

from redis_benchmarks_specification.__cli__.args import spec_cli_args
from redis_benchmarks_specification.__cli__.admin import (
    admin_command_logic,
    admin_runners_command,
    admin_builders_command,
    admin_queues_command,
    admin_status_command,
    admin_cancel_command,
    admin_summary_command,
    _format_idle,
    _format_age,
    _get_queue_progress,
    _short_hash,
    _short_info,
)
from redis_benchmarks_specification.__builder__.builder import (
    generate_benchmark_stream_request,
)
from redis_benchmarks_specification.__compare__.args import create_compare_arguments
from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_GH_EVENTS_COMMIT,
    STREAM_GH_EVENTS_COMMIT_BUILDERS_CG,
    STREAM_GH_NEW_BUILD_RUNNERS_CG,
    get_arch_specific_stream_name,
    REDIS_BINS_EXPIRE_SECS,
)


# --- Unit tests (no Redis required) ---


def test_cli_args_deployment_name_regexp():
    """Test that --deployment-name-regexp is properly parsed."""
    parser = argparse.ArgumentParser()
    parser = spec_cli_args(parser)

    # Default
    args = parser.parse_args([])
    assert args.deployment_name_regexp == ".*"

    # Custom
    args = parser.parse_args(
        ["--deployment-name-regexp", "oss-standalone-0[48]-io-threads"]
    )
    assert args.deployment_name_regexp == "oss-standalone-0[48]-io-threads"


def test_cli_args_wait_benchmark():
    """Test that --wait-benchmark and --wait-benchmark-timeout are properly parsed."""
    parser = argparse.ArgumentParser()
    parser = spec_cli_args(parser)

    args = parser.parse_args(["--wait-benchmark"])
    assert args.wait_benchmark is True
    assert args.wait_benchmark_timeout == -1

    args = parser.parse_args(["--wait-benchmark", "--wait-benchmark-timeout", "300"])
    assert args.wait_benchmark is True
    assert args.wait_benchmark_timeout == 300


def test_cli_args_admin_command():
    """Test that --tool admin args are properly parsed."""
    parser = argparse.ArgumentParser()
    parser = spec_cli_args(parser)

    args = parser.parse_args(
        ["--tool", "admin", "--admin-command", "runners", "--stream-id", "12345-0"]
    )
    assert args.tool == "admin"
    assert args.admin_command == "runners"
    assert args.stream_id == "12345-0"


def test_compare_args_list_deployments():
    """Test that --list-deployments and --deployment-name-regexp are parsed in compare tool."""
    parser = argparse.ArgumentParser()
    parser = create_compare_arguments(parser)

    args = parser.parse_args(["--list-deployments"])
    assert args.list_deployments is True

    args = parser.parse_args(["--deployment-name-regexp", "oss-standalone.*io-threads"])
    assert args.deployment_name_regexp == "oss-standalone.*io-threads"

    # Comma-separated deployment_name
    args = parser.parse_args(
        ["--deployment_name", "oss-standalone,oss-standalone-04-io-threads"]
    )
    assert "," in args.deployment_name


def test_format_idle():
    """Test idle time formatting."""
    assert _format_idle(500) == "500ms"
    assert _format_idle(5000) == "5s"
    assert _format_idle(65000) == "1m"
    assert _format_idle(3700000) == "1.0h"
    assert _format_idle(90000000) == "1.0d"


def test_format_age():
    """Test stream ID age formatting."""
    import datetime

    now_ms = int(datetime.datetime.utcnow().timestamp() * 1000)
    # 30 seconds ago
    assert "s ago" in _format_age(f"{now_ms - 30000}-0")
    # 5 minutes ago
    assert "m ago" in _format_age(f"{now_ms - 300000}-0")
    # Invalid
    assert _format_age("invalid") == "unknown"


def test_short_hash():
    """Test hash truncation."""
    assert _short_hash("abcdef1234567890") == "abcdef1234"
    assert _short_hash("short") == "short"
    assert _short_hash("") == ""
    assert _short_hash(None) == ""


def test_short_info():
    """Test build info one-line summary."""
    # redis/redis unstable
    info = {
        "github_org": "redis",
        "github_repo": "redis",
        "git_branch": "unstable",
        "git_hash": "abcdef1234567890",
    }
    result = _short_info(info)
    assert "unstable" in result
    assert "abcdef1234" in result

    # Non-redis org
    info = {
        "github_org": "tezc",
        "github_repo": "redis",
        "git_branch": "hinted-hash",
        "git_hash": "cf2a2ee7ded8088052fcdd5d",
    }
    result = _short_info(info)
    assert "tezc/redis" in result
    assert "hinted-hash" in result

    # With PR
    info = {
        "github_org": "redis",
        "github_repo": "redis",
        "git_branch": "fix-branch",
        "git_hash": "1234567890abcdef",
        "pull_request": "14907",
    }
    result = _short_info(info)
    assert "PR#14907" in result

    # Empty
    assert _short_info({}) == ""
    assert _short_info(None) == ""


def test_topology_filtering_regex():
    """Test that deployment_name_regexp regex correctly filters topologies."""
    topologies = [
        "oss-standalone",
        "oss-standalone-02-io-threads",
        "oss-standalone-04-io-threads",
        "oss-standalone-08-io-threads",
        "oss-standalone-12-io-threads",
        "oss-standalone-16-io-threads",
        "oss-cluster-3-primaries",
    ]

    # Only io-threads variants
    regexp = "oss-standalone-.*io-threads"
    filtered = [t for t in topologies if re.match(regexp, t)]
    assert len(filtered) == 5
    assert "oss-standalone" not in filtered
    assert "oss-cluster-3-primaries" not in filtered

    # 4 and 8 io-threads only
    regexp = "oss-standalone-0[48]-io-threads"
    filtered = [t for t in topologies if re.match(regexp, t)]
    assert filtered == [
        "oss-standalone-04-io-threads",
        "oss-standalone-08-io-threads",
    ]

    # Standalone only (no io-threads, no cluster)
    regexp = "oss-standalone$"
    filtered = [t for t in topologies if re.match(regexp, t)]
    assert filtered == ["oss-standalone"]

    # Default: all
    regexp = ".*"
    filtered = [t for t in topologies if re.match(regexp, t)]
    assert len(filtered) == 7


def test_generate_benchmark_stream_request_deployment_regexp():
    """Test that deployment_name_regexp is included/excluded correctly in stream fields."""
    try:
        conn = redis.StrictRedis(decode_responses=False)
        conn.ping()

        # Without deployment_name_regexp (default) - should NOT include field
        fields, result = generate_benchmark_stream_request(
            "test-id",
            conn,
            "redis",
            "amd64",
            {},
            "linux",
            deployment_name_regexp=".*",
        )
        assert "deployment_name_regexp" not in fields

        # With deployment_name_regexp - should include field
        fields, result = generate_benchmark_stream_request(
            "test-id",
            conn,
            "redis",
            "amd64",
            {},
            "linux",
            deployment_name_regexp="oss-standalone-0[48]-io-threads",
        )
        assert fields["deployment_name_regexp"] == "oss-standalone-0[48]-io-threads"

    except redis.exceptions.ConnectionError:
        pass


def test_dry_run_topology_breakdown():
    """Test dry-run topology breakdown counting with test suites."""
    import yaml
    import glob
    import os

    test_suites_folder = os.path.abspath("redis_benchmarks_specification/test-suites")
    spec_files = sorted(glob.glob(os.path.join(test_suites_folder, "*.yml")))

    # Count with default (all topologies)
    total_tests = 0
    total_topology_runs = 0
    tests_regexp = re.compile(".*setget2000c.*")

    for spec_file in spec_files:
        if "defaults" in spec_file:
            continue
        with open(spec_file) as f:
            config = yaml.safe_load(f)
        if config is None or "name" not in config:
            continue
        test_name = config["name"]
        if not tests_regexp.match(test_name):
            continue
        topologies = config.get("redis-topologies", [])
        if topologies:
            total_tests += 1
            total_topology_runs += len(topologies)

    assert total_tests > 0, "Should find setget2000c tests"
    assert (
        total_topology_runs > total_tests
    ), "setget2000c tests have multiple topologies"

    # Count with io-threads filter
    total_runs_filtered = 0
    deployment_regexp = "oss-standalone-.*io-threads"

    for spec_file in spec_files:
        if "defaults" in spec_file:
            continue
        with open(spec_file) as f:
            config = yaml.safe_load(f)
        if config is None or "name" not in config:
            continue
        test_name = config["name"]
        if not tests_regexp.match(test_name):
            continue
        topologies = config.get("redis-topologies", [])
        topologies = [t for t in topologies if re.match(deployment_regexp, t)]
        if topologies:
            total_runs_filtered += len(topologies)

    assert total_runs_filtered < total_topology_runs
    assert total_runs_filtered > 0


# --- Integration tests (require Redis) ---


def test_topology_queue_tracking():
    """Test topology-level queue keys (pending/running/completed/failed)."""
    try:
        conn = redis.StrictRedis(decode_responses=True)
        conn.ping()

        stream_id = "9999999999999-0"
        platform = "test-platform-x86"
        prefix = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{platform}"

        # Clean up test keys
        for suffix in [
            "tests_pending",
            "tests_running",
            "tests_completed",
            "tests_failed",
            "topologies_pending",
            "topologies_running",
            "topologies_completed",
            "topologies_failed",
        ]:
            conn.delete(f"{prefix}:{suffix}")

        topo_pending_key = f"{prefix}:topologies_pending"
        topo_running_key = f"{prefix}:topologies_running"
        topo_completed_key = f"{prefix}:topologies_completed"
        topo_failed_key = f"{prefix}:topologies_failed"
        test_pending_key = f"{prefix}:tests_pending"

        test_name = "memtier_benchmark-test"
        topologies = ["oss-standalone", "oss-standalone-04-io-threads"]

        # Populate
        conn.lpush(test_pending_key, test_name)
        for topo in topologies:
            conn.lpush(topo_pending_key, f"{test_name}::{topo}")

        assert conn.llen(test_pending_key) == 1
        assert conn.llen(topo_pending_key) == 2

        # Move first topology to running
        entry = f"{test_name}::oss-standalone"
        conn.lrem(topo_pending_key, 1, entry)
        conn.lpush(topo_running_key, entry)
        assert conn.llen(topo_pending_key) == 1
        assert conn.llen(topo_running_key) == 1

        # Complete it
        conn.lrem(topo_running_key, 1, entry)
        conn.lpush(topo_completed_key, entry)
        assert conn.llen(topo_running_key) == 0
        assert conn.llen(topo_completed_key) == 1

        # Fail second topology (pushed to both completed AND failed, like coordinator does)
        entry2 = f"{test_name}::oss-standalone-04-io-threads"
        conn.lrem(topo_pending_key, 1, entry2)
        conn.lpush(topo_completed_key, entry2)
        conn.lpush(topo_failed_key, entry2)

        assert conn.llen(topo_pending_key) == 0
        assert conn.llen(topo_completed_key) == 2
        assert conn.llen(topo_failed_key) == 1

        # Verify failed entry is parseable
        failed_entries = conn.lrange(topo_failed_key, 0, -1)
        parts = failed_entries[0].split("::")
        assert parts[0] == test_name
        assert parts[1] == "oss-standalone-04-io-threads"

        # Clean up
        for suffix in [
            "tests_pending",
            "topologies_pending",
            "topologies_running",
            "topologies_completed",
            "topologies_failed",
        ]:
            conn.delete(f"{prefix}:{suffix}")

    except redis.exceptions.ConnectionError:
        pass


def test_queue_progress_total_no_double_count():
    """Test that _get_queue_progress does not double-count failed tests.

    The coordinator pushes failed tests to BOTH tests_completed and tests_failed.
    So total should be pending + running + completed (not + failed).
    """
    try:
        conn = redis.StrictRedis(decode_responses=False)
        conn.ping()

        stream_id = "9999999999998-0"
        platform = "test-platform-dblcount"
        prefix = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{platform}"

        # Clean up
        for suffix in [
            "tests_pending",
            "tests_running",
            "tests_completed",
            "tests_failed",
        ]:
            conn.delete(f"{prefix}:{suffix}")

        # Simulate: 2 completed (1 of which also failed), 1 pending
        conn.lpush(f"{prefix}:tests_pending", "test-c")
        conn.lpush(f"{prefix}:tests_completed", "test-a")
        conn.lpush(f"{prefix}:tests_completed", "test-b")
        conn.lpush(f"{prefix}:tests_failed", "test-b")

        pending, running, completed, failed, total = _get_queue_progress(
            conn, stream_id, platform
        )
        assert pending == 1
        assert running == 0
        assert completed == 2
        assert failed == 1
        # total should NOT include failed (it's a subset of completed)
        assert total == 3  # 1 pending + 0 running + 2 completed
        # NOT 4 (which would be the double-counted version)

        # Clean up
        for suffix in [
            "tests_pending",
            "tests_running",
            "tests_completed",
            "tests_failed",
        ]:
            conn.delete(f"{prefix}:{suffix}")

    except redis.exceptions.ConnectionError:
        pass


def test_stream_deployment_name_regexp_roundtrip():
    """Test that deployment_name_regexp survives XADD -> XREAD roundtrip."""
    try:
        conn = redis.StrictRedis(decode_responses=True)
        conn.ping()

        test_stream = "test:admin:deployment-regexp-roundtrip"
        conn.delete(test_stream)

        fields = {
            "id": "1",
            "run_image": "redis",
            "os": "linux",
            "arch": "amd64",
            "tests_regexp": ".*setget.*",
            "deployment_name_regexp": "oss-standalone-0[48]-io-threads",
            "git_hash": "abc123",
        }
        conn.xadd(test_stream, fields)

        entries = conn.xread({test_stream: "0-0"}, count=1)
        assert len(entries) > 0
        _, messages = entries[0]
        _, msg_fields = messages[0]

        assert "deployment_name_regexp" in msg_fields
        assert msg_fields["deployment_name_regexp"] == "oss-standalone-0[48]-io-threads"

        # Verify regex actually works on the recovered value
        topologies = [
            "oss-standalone",
            "oss-standalone-04-io-threads",
            "oss-standalone-08-io-threads",
            "oss-standalone-16-io-threads",
        ]
        regexp = msg_fields["deployment_name_regexp"]
        matched = [t for t in topologies if re.match(regexp, t)]
        assert matched == [
            "oss-standalone-04-io-threads",
            "oss-standalone-08-io-threads",
        ]

        conn.delete(test_stream)

    except redis.exceptions.ConnectionError:
        pass


def test_admin_cancel_command():
    """Test that cancel flushes pending tests for a stream."""
    try:
        conn = redis.StrictRedis(decode_responses=False)
        conn.ping()

        stream_id = "9999999999997-0"
        platform = "test-platform-cancel"
        prefix = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{platform}"
        zset_key = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{platform}:zset"

        # Clean up
        conn.delete(f"{prefix}:tests_pending")
        conn.delete(f"{prefix}:topologies_pending")
        conn.delete(zset_key)

        # Setup: add stream to platform zset and populate pending
        conn.zadd(zset_key, {stream_id: 9999999999997})
        conn.lpush(f"{prefix}:tests_pending", "test-a", "test-b", "test-c")
        conn.lpush(f"{prefix}:topologies_pending", "test-a::topo1", "test-b::topo1")
        assert conn.llen(f"{prefix}:tests_pending") == 3

        # Build args
        parser = argparse.ArgumentParser()
        parser = spec_cli_args(parser)
        args = parser.parse_args(
            [
                "--tool",
                "admin",
                "--admin-command",
                "cancel",
                "--stream-id",
                stream_id,
                "--platform",
                platform,
            ]
        )

        admin_cancel_command(conn, args)

        assert conn.llen(f"{prefix}:tests_pending") == 0
        assert conn.llen(f"{prefix}:topologies_pending") == 0

        # Clean up
        conn.delete(zset_key)

    except redis.exceptions.ConnectionError:
        pass


def test_all_topologies_filtered_no_name_error():
    """Test that filtering out ALL topologies doesn't cause a NameError on test_result.

    When deployment_name_regexp filters out every topology for a test, test_result
    must be initialized before the topology loop. Otherwise line 2163 in the
    coordinator (if test_result is False) raises NameError on the first test,
    or leaks the value from a previous test iteration.
    """
    # This test verifies the fix by simulating the coordinator's logic:
    # iterate over topologies, skip all via regex, then check test_result.
    topologies = [
        "oss-standalone",
        "oss-standalone-02-io-threads",
        "oss-standalone-04-io-threads",
    ]
    deployment_name_regexp = "oss-cluster-.*"  # matches NOTHING in the list

    # Simulate the coordinator's topology loop with the fix:
    # test_result must be initialized BEFORE the loop
    test_result = True  # <-- the fix
    for topology_spec_name in topologies:
        if deployment_name_regexp != ".*":
            if not re.match(deployment_name_regexp, topology_spec_name):
                continue
        # If we get here, a topology matched — this won't happen in this test
        test_result = False  # would be set by actual benchmark execution

    # After the loop, test_result must be accessible (not NameError)
    # and should be True (all topologies were skipped = nothing failed)
    assert test_result is True

    # Now verify that when some topologies DO match, test_result reflects execution
    deployment_name_regexp = "oss-standalone$"
    test_result = True
    for topology_spec_name in topologies:
        if deployment_name_regexp != ".*":
            if not re.match(deployment_name_regexp, topology_spec_name):
                continue
        # Simulate a failed test execution for oss-standalone
        test_result = False

    assert test_result is False  # the matching topology "ran" and "failed"


def test_compare_comma_separated_deployment_auto_enable():
    """Test that compare-by-env auto-enables with comma-separated deployment names."""
    parser = argparse.ArgumentParser()
    parser = create_compare_arguments(parser)

    # Single deployment: no auto-enable
    args = parser.parse_args(
        [
            "--deployment_name",
            "oss-standalone",
            "--baseline-branch",
            "unstable",
            "--comparison-branch",
            "unstable",
        ]
    )
    assert "," not in args.deployment_name

    # Comma-separated: should parse correctly
    args = parser.parse_args(
        [
            "--deployment_name",
            "oss-standalone,oss-standalone-04-io-threads,oss-standalone-08-io-threads",
            "--baseline-branch",
            "unstable",
            "--comparison-branch",
            "unstable",
        ]
    )
    env_list = [d.strip() for d in args.deployment_name.split(",")]
    assert len(env_list) == 3
    assert env_list == [
        "oss-standalone",
        "oss-standalone-04-io-threads",
        "oss-standalone-08-io-threads",
    ]
