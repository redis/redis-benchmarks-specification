from unittest.mock import MagicMock, patch

import pytest

from redis_benchmarks_specification.__self_contained_coordinator__ import (
    idle_shutdown,
)
from redis_benchmarks_specification.__self_contained_coordinator__.args import (
    create_self_contained_coordinator_args,
)
from redis_benchmarks_specification.__self_contained_coordinator__.idle_shutdown import (
    DEFAULT_IDLE_SHUTDOWN_MINUTES,
    IdleTracker,
    idle_shutdown_due,
    shutdown_host,
    start_idle_shutdown_watchdog,
)
from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    self_contained_coordinator_blocking_read,
)

COORDINATOR = "redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator"


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_idle_time_grows_with_the_clock():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    clock.now += 90
    assert tracker.idle_seconds() == 90


def test_busy_runner_is_never_idle_however_long_the_job_runs():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    tracker.start_work()
    clock.now += 6 * 3600
    assert tracker.idle_seconds() == 0
    assert not idle_shutdown_due(tracker, 15)


def test_finished_job_that_started_a_test_restarts_the_idle_clock():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    tracker.start_work()
    tracker.note_test_started()
    clock.now += 3600
    tracker.end_work()
    clock.now += 60
    assert tracker.idle_seconds() == 60


def test_entry_that_ran_nothing_does_not_restart_the_idle_clock():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    clock.now += 600
    tracker.start_work()
    tracker.end_work()
    clock.now += 300
    assert tracker.idle_seconds() == 900


def test_a_test_started_in_an_earlier_job_does_not_restart_the_clock_again():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    tracker.start_work()
    tracker.note_test_started()
    tracker.end_work()
    clock.now += 600
    tracker.start_work()
    tracker.end_work()
    clock.now += 300
    assert tracker.idle_seconds() == 900


def test_shutdown_is_due_only_once_the_limit_is_reached():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    clock.now += 15 * 60 - 1
    assert not idle_shutdown_due(tracker, 15)
    clock.now += 1
    assert idle_shutdown_due(tracker, 15)


def test_fractional_minutes_are_supported():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    clock.now += 30
    assert idle_shutdown_due(tracker, 0.5)


def _tracker_idle_for(seconds):
    tracker = MagicMock()
    tracker.idle_seconds.return_value = seconds
    return tracker


def _run_watchdog(tracker, shutdown, minutes=15):
    thread = start_idle_shutdown_watchdog(
        tracker, minutes, shutdown=shutdown, check_interval=0.01
    )
    thread.join(timeout=2)
    return thread


def test_watchdog_shuts_down_once_idle_for_the_limit():
    shutdown = MagicMock()
    thread = _run_watchdog(_tracker_idle_for(15 * 60), shutdown)
    assert not thread.is_alive()
    shutdown.assert_called_once_with()


def test_watchdog_leaves_a_runner_that_is_not_idle_long_enough_alone():
    shutdown = MagicMock()
    thread = _run_watchdog(_tracker_idle_for(15 * 60 - 1), shutdown)
    assert thread.is_alive()
    shutdown.assert_not_called()


def test_watchdog_does_not_shut_down_a_busy_runner():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    shutdown = MagicMock()
    thread = start_idle_shutdown_watchdog(
        tracker, 15, shutdown=shutdown, check_interval=0.01
    )
    tracker.start_work()
    clock.now += 10 * 3600
    thread.join(timeout=0.3)
    assert thread.is_alive()
    shutdown.assert_not_called()


def test_a_fresh_runner_gets_the_full_window_to_receive_work():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    clock.now += 3600  # the process was up for an hour before the watchdog started
    start_idle_shutdown_watchdog(tracker, 15, shutdown=MagicMock(), check_interval=3600)
    assert tracker.idle_seconds() == 0


def test_watchdog_gives_up_after_a_failed_shutdown():
    shutdown = MagicMock(side_effect=RuntimeError("no permission"))
    thread = _run_watchdog(_tracker_idle_for(20 * 60), shutdown)
    assert not thread.is_alive()
    shutdown.assert_called_once_with()


def test_failed_shutdown_is_reported_to_the_caller():
    error = RuntimeError("no permission")
    on_failure = MagicMock()
    thread = start_idle_shutdown_watchdog(
        _tracker_idle_for(20 * 60),
        15,
        shutdown=MagicMock(side_effect=error),
        check_interval=0.01,
        on_failure=on_failure,
    )
    thread.join(timeout=2)
    on_failure.assert_called_once_with(error)


def test_shutdown_host_runs_the_command_directly_as_root():
    with patch.object(idle_shutdown.os, "geteuid", return_value=0), patch.object(
        idle_shutdown.subprocess, "run"
    ) as run:
        shutdown_host()
    run.assert_called_once_with(["shutdown", "-h", "now"], check=True)


def test_shutdown_host_uses_non_interactive_sudo_when_not_root():
    with patch.object(idle_shutdown.os, "geteuid", return_value=1000), patch.object(
        idle_shutdown.subprocess, "run"
    ) as run:
        shutdown_host()
    run.assert_called_once_with(["sudo", "-n", "shutdown", "-h", "now"], check=True)


def test_flag_is_off_by_default():
    args = create_self_contained_coordinator_args("test").parse_args([])
    assert args.idle_shutdown is None


def test_flag_without_a_value_uses_the_default_of_15_minutes():
    args = create_self_contained_coordinator_args("test").parse_args(
        ["--idle-shutdown"]
    )
    assert DEFAULT_IDLE_SHUTDOWN_MINUTES == 15
    assert args.idle_shutdown == 15


def test_flag_value_overrides_the_default():
    args = create_self_contained_coordinator_args("test").parse_args(
        ["--idle-shutdown", "45"]
    )
    assert args.idle_shutdown == 45


@pytest.mark.parametrize("bad", ["0", "-5", "abc"])
def test_flag_rejects_non_positive_and_non_numeric_values(bad):
    with pytest.raises(SystemExit):
        create_self_contained_coordinator_args("test").parse_args(
            ["--idle-shutdown", bad]
        )


def _blocking_read(conn):
    return self_contained_coordinator_blocking_read(
        conn,
        None,
        None,
        "/tmp",
        ">",
        None,
        {},
        {},
        "x86-aws-m7i.metal-24xl",
        False,
        [],
        arch="amd64",
    )


def _delivered_message():
    return [
        [b"oss:api:gh/redis/redis/builds:amd64", [(b"1787194724560-0", {b"f": b"v"})]]
    ]


def _conn():
    conn = MagicMock()
    conn.xreadgroup.return_value = _delivered_message()
    conn.xack.return_value = 1
    return conn


def _processor(tracker, clock, elapsed, started_a_test, result):
    def run(*_args, **_kwargs):
        clock.now += elapsed
        if started_a_test:
            tracker.note_test_started()
        return result

    return run


def test_skipped_entry_does_not_count_as_activity():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    clock.now += 600
    with patch(f"{COORDINATOR}.IDLE_TRACKER", tracker), patch(
        f"{COORDINATOR}.process_self_contained_coordinator_stream",
        side_effect=_processor(tracker, clock, 1, False, ("1787194724560-0", False, 0)),
    ):
        _blocking_read(_conn())
    assert tracker.idle_seconds() == 601


def test_processed_entry_counts_as_activity():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    clock.now += 600
    with patch(f"{COORDINATOR}.IDLE_TRACKER", tracker), patch(
        f"{COORDINATOR}.process_self_contained_coordinator_stream",
        side_effect=_processor(tracker, clock, 60, True, ("1787194724560-0", True, 4)),
    ):
        _blocking_read(_conn())
    assert tracker.idle_seconds() == 0


def test_job_whose_tests_all_fail_still_counts_as_activity():
    # Per-test failures are caught inside the processor, which then returns
    # normally with zero completed runs. The runner spent the time working, so
    # the idle clock must restart when it finishes, not jump past the limit.
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    clock.now += 600
    with patch(f"{COORDINATOR}.IDLE_TRACKER", tracker), patch(
        f"{COORDINATOR}.process_self_contained_coordinator_stream",
        side_effect=_processor(
            tracker, clock, 2 * 3600, True, ("1787194724560-0", False, 0)
        ),
    ):
        _blocking_read(_conn())
    assert tracker.idle_seconds() == 0
    clock.now += 60
    assert not idle_shutdown_due(tracker, 15)


def test_job_that_raises_after_starting_a_test_counts_and_is_never_left_busy():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)

    def run(*_args, **_kwargs):
        tracker.note_test_started()
        clock.now += 3600
        raise RuntimeError("boom")

    with patch(f"{COORDINATOR}.IDLE_TRACKER", tracker), patch(
        f"{COORDINATOR}.process_self_contained_coordinator_stream", side_effect=run
    ):
        _blocking_read(_conn())
    clock.now += 60
    assert tracker.idle_seconds() == 60


def test_job_that_raises_before_starting_any_test_does_not_count():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    clock.now += 600
    with patch(f"{COORDINATOR}.IDLE_TRACKER", tracker), patch(
        f"{COORDINATOR}.process_self_contained_coordinator_stream",
        side_effect=RuntimeError("bad spec"),
    ):
        _blocking_read(_conn())
    assert tracker.idle_seconds() == 600


def test_runner_is_busy_while_the_job_runs():
    clock = FakeClock()
    tracker = IdleTracker(clock=clock)
    seen = []

    def processor(*_args, **_kwargs):
        clock.now += 3600
        seen.append(tracker.idle_seconds())
        return ("1787194724560-0", True, 1)

    with patch(f"{COORDINATOR}.IDLE_TRACKER", tracker), patch(
        f"{COORDINATOR}.process_self_contained_coordinator_stream",
        side_effect=processor,
    ):
        _blocking_read(_conn())
    assert seen == [0]
