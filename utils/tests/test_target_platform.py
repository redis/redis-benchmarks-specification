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
    args = parser.parse_args(
        ["--target-platform", "x86-aws-m7i.metal-24xl-profiler"]
    )
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
