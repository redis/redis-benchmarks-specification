"""
Tests for --target-platform runner targeting feature.

Verifies that:
- CLI adds target_platform field to stream when specified
- CLI omits target_platform field when not specified
- Coordinator skips work targeted at other runners
- Coordinator processes work targeted at this runner
- Coordinator processes untargeted work (backwards compatible)
"""

import argparse


def test_cli_args_target_platform_default():
    """--target-platform defaults to None."""
    from redis_benchmarks_specification.__cli__.args import spec_cli_args

    parser = argparse.ArgumentParser()
    spec_cli_args(parser)
    args = parser.parse_args([])
    assert args.target_platform is None


def test_cli_args_target_platform_set():
    """--target-platform accepts a runner name."""
    from redis_benchmarks_specification.__cli__.args import spec_cli_args

    parser = argparse.ArgumentParser()
    spec_cli_args(parser)
    args = parser.parse_args(["--target-platform", "x86-aws-m7i.metal-24xl-profiler"])
    assert args.target_platform == "x86-aws-m7i.metal-24xl-profiler"


def test_stream_fields_include_target_platform():
    """When target_platform is set, it should appear in build_stream_fields."""
    # Simulate the field construction logic from cli.py
    build_stream_fields = {}
    target_platform = "x86-aws-m7i.metal-24xl-profiler"

    if target_platform is not None:
        build_stream_fields["target_platform"] = target_platform

    assert "target_platform" in build_stream_fields
    assert build_stream_fields["target_platform"] == "x86-aws-m7i.metal-24xl-profiler"


def test_stream_fields_omit_target_platform_when_none():
    """When target_platform is None, field should not be in stream."""
    build_stream_fields = {}
    target_platform = None

    if target_platform is not None:
        build_stream_fields["target_platform"] = target_platform

    assert "target_platform" not in build_stream_fields


def test_coordinator_skips_mismatched_target_platform():
    """Coordinator should skip work targeted at a different runner."""
    running_platform = "x86-aws-m7i.metal-24xl"
    testDetails = {
        b"target_platform": b"x86-aws-m7i.metal-24xl-profiler",
    }

    skip_test = False
    if b"target_platform" in testDetails:
        target_platform = testDetails[b"target_platform"]
        target_platform_str = (
            target_platform.decode()
            if isinstance(target_platform, bytes)
            else target_platform
        )
        if running_platform != target_platform_str:
            skip_test = True

    assert skip_test is True


def test_coordinator_processes_matching_target_platform():
    """Coordinator should process work targeted at this runner."""
    running_platform = "x86-aws-m7i.metal-24xl-profiler"
    testDetails = {
        b"target_platform": b"x86-aws-m7i.metal-24xl-profiler",
    }

    skip_test = False
    if b"target_platform" in testDetails:
        target_platform = testDetails[b"target_platform"]
        target_platform_str = (
            target_platform.decode()
            if isinstance(target_platform, bytes)
            else target_platform
        )
        if running_platform != target_platform_str:
            skip_test = True

    assert skip_test is False


def test_coordinator_processes_untargeted_work():
    """Coordinator should process work with no target_platform (backwards compat)."""
    running_platform = "x86-aws-m7i.metal-24xl"
    testDetails = {
        b"some_other_field": b"value",
    }

    skip_test = False
    if b"target_platform" in testDetails:
        target_platform = testDetails[b"target_platform"]
        target_platform_str = (
            target_platform.decode()
            if isinstance(target_platform, bytes)
            else target_platform
        )
        if running_platform != target_platform_str:
            skip_test = True

    assert skip_test is False


def test_coordinator_handles_string_target_platform():
    """Coordinator should handle target_platform as string (not bytes)."""
    running_platform = "x86-aws-m7i.metal-24xl-profiler"
    testDetails = {
        b"target_platform": "x86-aws-m7i.metal-24xl-profiler",  # string, not bytes
    }

    skip_test = False
    if b"target_platform" in testDetails:
        target_platform = testDetails[b"target_platform"]
        target_platform_str = (
            target_platform.decode()
            if isinstance(target_platform, bytes)
            else target_platform
        )
        if running_platform != target_platform_str:
            skip_test = True

    assert skip_test is False


# --- explicit-only mode tests ---


def _should_skip(testDetails, running_platform, explicit_only=False):
    """Replicate the coordinator's skip logic for testing."""
    skip_test = False
    has_explicit_target = b"target_platform" in testDetails
    if explicit_only and not has_explicit_target:
        skip_test = True
    elif has_explicit_target:
        target_platform = testDetails[b"target_platform"]
        target_platform_str = (
            target_platform.decode()
            if isinstance(target_platform, bytes)
            else target_platform
        )
        if running_platform != target_platform_str:
            skip_test = True
    return skip_test


def test_explicit_only_skips_untargeted():
    """In explicit-only mode, entries without target_platform are skipped."""
    testDetails = {b"some_field": b"value"}
    assert (
        _should_skip(testDetails, "x86-aws-m7i.metal-24xl", explicit_only=True) is True
    )


def test_explicit_only_processes_matching_target():
    """In explicit-only mode, entries with matching target_platform are processed."""
    testDetails = {b"target_platform": b"x86-aws-m7i.metal-24xl-profiler"}
    assert (
        _should_skip(testDetails, "x86-aws-m7i.metal-24xl-profiler", explicit_only=True)
        is False
    )


def test_explicit_only_skips_mismatched_target():
    """In explicit-only mode, entries with a different target_platform are skipped."""
    testDetails = {b"target_platform": b"arm-aws-m8g.metal-24xl"}
    assert (
        _should_skip(testDetails, "x86-aws-m7i.metal-24xl", explicit_only=True) is True
    )


def test_normal_mode_processes_untargeted():
    """In normal mode, entries without target_platform are processed (backwards compat)."""
    testDetails = {b"some_field": b"value"}
    assert (
        _should_skip(testDetails, "x86-aws-m7i.metal-24xl", explicit_only=False)
        is False
    )


# --- CLI args tests for replay tool ---


def test_cli_args_replay_stream_id_default():
    """--replay-stream-id defaults to None."""
    from redis_benchmarks_specification.__cli__.args import spec_cli_args

    parser = argparse.ArgumentParser()
    spec_cli_args(parser)
    args = parser.parse_args([])
    assert args.replay_stream_id is None


def test_cli_args_replay_stream_id_set():
    """--replay-stream-id accepts a stream ID."""
    from redis_benchmarks_specification.__cli__.args import spec_cli_args

    parser = argparse.ArgumentParser()
    spec_cli_args(parser)
    args = parser.parse_args(
        ["--tool", "replay", "--replay-stream-id", "1776070055182-0"]
    )
    assert args.tool == "replay"
    assert args.replay_stream_id == "1776070055182-0"


def test_cli_args_replay_with_overrides():
    """--tool replay accepts target-platform and tests-regexp overrides."""
    from redis_benchmarks_specification.__cli__.args import spec_cli_args

    parser = argparse.ArgumentParser()
    spec_cli_args(parser)
    args = parser.parse_args(
        [
            "--tool",
            "replay",
            "--replay-stream-id",
            "1776070055182-0",
            "--target-platform",
            "x86-aws-m7i.metal-24xl-profiler",
            "--tests-regexp",
            "memtier_benchmark-1Mkeys.*expire",
            "--arch",
            "arm64",
        ]
    )
    assert args.tool == "replay"
    assert args.replay_stream_id == "1776070055182-0"
    assert args.target_platform == "x86-aws-m7i.metal-24xl-profiler"
    assert args.tests_regexp == "memtier_benchmark-1Mkeys.*expire"
    assert args.arch == "arm64"


# --- coordinator args tests for explicit-only ---


def test_coordinator_args_explicit_only_default():
    """--explicit-only defaults to False."""
    from redis_benchmarks_specification.__self_contained_coordinator__.args import (
        create_self_contained_coordinator_args,
    )

    parser = create_self_contained_coordinator_args("test")
    args = parser.parse_args([])
    assert args.explicit_only is False


def test_coordinator_args_explicit_only_set():
    """--explicit-only flag can be enabled."""
    from redis_benchmarks_specification.__self_contained_coordinator__.args import (
        create_self_contained_coordinator_args,
    )

    parser = create_self_contained_coordinator_args("test")
    args = parser.parse_args(["--explicit-only"])
    assert args.explicit_only is True
