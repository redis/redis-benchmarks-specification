import logging
import os
import subprocess
import threading
import time

DEFAULT_IDLE_SHUTDOWN_MINUTES = 15
IDLE_CHECK_INTERVAL_SECS = 30
SHUTDOWN_COMMAND = ["shutdown", "-h", "now"]


class IdleTracker:
    """Tracks how long the runner has gone without running a benchmark.

    The event stream is shared: a runner is also handed entries that are not
    meant for it (other platforms, other priorities) and skips them. Receiving
    such an entry is not activity, otherwise a runner on a busy stream would
    never look idle. Only an entry that started a test resets the clock, and it
    does so when the job ends, whether or not the tests passed.
    """

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._lock = threading.Lock()
        self._busy = False
        self._started_a_test = False
        self._last_activity = clock()

    def mark_activity(self):
        with self._lock:
            self._last_activity = self._clock()

    def start_work(self):
        with self._lock:
            self._busy = True
            self._started_a_test = False

    def note_work(self):
        """Called when a test starts running for this runner."""
        with self._lock:
            self._started_a_test = True

    def end_work(self):
        with self._lock:
            self._busy = False
            if self._started_a_test:
                self._last_activity = self._clock()

    def idle_seconds(self):
        with self._lock:
            if self._busy:
                return 0.0
            return self._clock() - self._last_activity


IDLE_TRACKER = IdleTracker()


def shutdown_host(command=None):
    cmd = list(command or SHUTDOWN_COMMAND)
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        cmd = ["sudo", "-n"] + cmd
    logging.warning("Idle shutdown: running {}".format(" ".join(cmd)))
    subprocess.run(cmd, check=True)


def idle_shutdown_due(tracker, idle_minutes):
    return tracker.idle_seconds() >= idle_minutes * 60


def start_idle_shutdown_watchdog(
    tracker,
    idle_minutes,
    shutdown=shutdown_host,
    check_interval=IDLE_CHECK_INTERVAL_SECS,
    on_failure=None,
):
    """Shut the host down once the runner has been idle for idle_minutes.

    The idle clock starts now, so a freshly started runner gets the full
    window to receive work. A failed shutdown (for example no permission) is
    not retried: it would fail the same way every check. on_failure(error) lets
    the caller surface that state, so a runner that cannot stop is not silent.
    """
    tracker.mark_activity()

    def _watchdog_loop():
        while True:
            time.sleep(check_interval)
            if not idle_shutdown_due(tracker, idle_minutes):
                continue
            logging.warning(
                "Idle for {:.0f}s (limit {} min): shutting the host down.".format(
                    tracker.idle_seconds(), idle_minutes
                )
            )
            try:
                shutdown()
            except Exception as e:
                logging.error(
                    "Idle shutdown failed: {}. Giving up; the runner stays up.".format(
                        e
                    )
                )
                if on_failure is not None:
                    on_failure(e)
            return

    t = threading.Thread(target=_watchdog_loop, daemon=True, name="idle-shutdown")
    t.start()
    logging.info(
        "Idle shutdown enabled: the host shuts down after {} min without running benchmarks.".format(
            idle_minutes
        )
    )
    return t
