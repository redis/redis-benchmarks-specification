"""
topdown-profiler integration for the self-contained coordinator.

This module provides functions to check if topdown-profiler is available
and to collect Intel Top-Down Microarchitecture Analysis (TMA) data
alongside benchmark runs, with labels matching the parca-agent label system.
"""

import logging
import shutil
import subprocess
from typing import Dict, Optional


def check_topdown_available() -> bool:
    """
    Check if topdown-profiler CLI is available on the system.

    Returns True only if:
      1. `topdown` command exists in PATH
      2. `topdown version` runs successfully
      3. System is x86_64 (Intel TMA requires Intel CPU)
    """
    # Step 1: Check if topdown command exists
    if shutil.which("topdown") is None:
        logging.info("topdown command not found in PATH - topdown integration disabled")
        return False

    # Step 2: Verify it runs
    try:
        result = subprocess.run(
            ["topdown", "version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            logging.info(
                f"topdown version failed (rc={result.returncode}) - topdown integration disabled"
            )
            return False
        version = result.stdout.strip()
        logging.info(f"topdown-profiler available: {version} - integration enabled")
        return True
    except subprocess.TimeoutExpired:
        logging.warning("Timeout running topdown version")
        return False
    except Exception as e:
        logging.warning(f"Failed to check topdown availability: {e}")
        return False


def check_perf_event_paranoid() -> bool:
    """
    Check if perf_event_paranoid is set low enough for TMA collection.

    Returns True if perf_event_paranoid <= 1 (or running as root).
    """
    import os

    if os.geteuid() == 0:
        return True

    try:
        with open("/proc/sys/kernel/perf_event_paranoid", "r") as f:
            value = int(f.read().strip())
            if value <= 1:
                return True
            else:
                logging.warning(
                    f"perf_event_paranoid={value} (need <=1) - topdown collection may fail. "
                    f"Fix with: sudo sysctl kernel.perf_event_paranoid=1"
                )
                return False
    except Exception as e:
        logging.warning(f"Could not read perf_event_paranoid: {e}")
        return False


def build_topdown_labels(labels: Dict[str, str]) -> list:
    """
    Convert a label dictionary to topdown CLI --label arguments.

    Returns a list of ['--label', 'key=value', '--label', 'key=value', ...] args.
    """
    args = []
    for key, value in labels.items():
        if value is not None:
            # topdown-profiler stores labels as JSON, so less sanitization needed
            # than parca-agent, but we still clean up for safety
            clean_value = str(value).replace("'", "").replace('"', "")
            args.extend(["--label", f"{key}={clean_value}"])
    return args


def collect_topdown(
    process_name: str,
    duration_seconds: int,
    level: int,
    labels: Dict[str, str],
    db_path: Optional[str] = None,
    timeout: Optional[int] = None,
) -> Optional[str]:
    """
    Run a TMA collection for a process with labels.

    Args:
        process_name: Process name to profile (e.g., 'redis-server')
        duration_seconds: Collection duration in seconds
        level: TMA analysis level (1-6, default 2)
        labels: Dictionary of label key-value pairs
        db_path: Optional path to SQLite database
        timeout: Subprocess timeout (default: duration + 60s buffer)

    Returns:
        Run ID string if successful, None on failure
    """
    if timeout is None:
        timeout = duration_seconds + 60

    cmd = [
        "topdown",
        "collect",
        "--process",
        process_name,
        "--level",
        str(level),
        "--duration",
        f"{duration_seconds}s",
    ]

    # Add labels
    cmd.extend(build_topdown_labels(labels))

    # Add database path if specified
    env = None
    if db_path:
        import os

        env = {**os.environ, "TOPDOWN_DB_PATH": db_path}

    try:
        logging.info(
            f"Starting topdown collection: process={process_name}, "
            f"level={level}, duration={duration_seconds}s, labels={len(labels)}"
        )
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )

        if result.returncode != 0:
            logging.warning(
                f"topdown collect failed (rc={result.returncode}): {result.stderr}"
            )
            return None

        # Extract run ID from output (format: "Done. Run ID: <uuid>")
        for line in result.stdout.splitlines():
            if "Run ID:" in line:
                run_id = line.split("Run ID:")[-1].strip()
                logging.info(f"topdown collection complete: run_id={run_id}")
                return run_id

        logging.info(f"topdown collection complete (no run ID parsed)")
        return "unknown"

    except subprocess.TimeoutExpired:
        logging.warning(
            f"topdown collection timed out after {timeout}s "
            f"(duration was {duration_seconds}s)"
        )
        return None
    except Exception as e:
        logging.warning(f"topdown collection failed: {e}")
        return None


class TopdownCollector:
    """
    Thread-based topdown collector that runs alongside a benchmark.

    Usage:
        collector = TopdownCollector(
            process_name="redis-server",
            duration_seconds=30,
            level=2,
            labels={...},
        )
        collector.start()
        # ... run benchmark ...
        run_id = collector.wait_for_completion()
    """

    def __init__(
        self,
        process_name: str,
        duration_seconds: int,
        level: int = 2,
        labels: Optional[Dict[str, str]] = None,
        db_path: Optional[str] = None,
    ):
        self.process_name = process_name
        self.duration_seconds = duration_seconds
        self.level = level
        self.labels = labels or {}
        self.db_path = db_path
        self._thread = None
        self._run_id = None
        self._error = None

    def start(self):
        """Start topdown collection in a background thread."""
        import threading

        self._thread = threading.Thread(target=self._collect_worker, daemon=True)
        self._thread.start()
        logging.info(
            f"TopdownCollector started: process={self.process_name}, "
            f"duration={self.duration_seconds}s, level={self.level}"
        )

    def _collect_worker(self):
        """Worker thread that runs topdown collect."""
        try:
            self._run_id = collect_topdown(
                process_name=self.process_name,
                duration_seconds=self.duration_seconds,
                level=self.level,
                labels=self.labels,
                db_path=self.db_path,
            )
        except Exception as e:
            self._error = str(e)
            logging.warning(f"TopdownCollector error: {e}")

    def wait_for_completion(self, timeout: int = 120) -> Optional[str]:
        """
        Wait for collection to complete.

        Returns the run ID if successful, None on failure/timeout.
        """
        if self._thread is None:
            logging.warning("TopdownCollector: start() was not called")
            return None

        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            logging.warning(f"TopdownCollector: timed out after {timeout}s")
            return None

        if self._error:
            logging.warning(f"TopdownCollector: failed with error: {self._error}")
            return None

        return self._run_id


def extract_topdown_labels_from_benchmark(
    startup_labels: Dict[str, str],
    build_labels: Dict[str, str],
    test_labels: Dict[str, str],
) -> Dict[str, str]:
    """
    Merge all label tiers into a single dict for topdown collection.

    Same label hierarchy as parca-agent:
      1. Startup labels (platform, arch, coordinator_version)
      2. Build labels (git_hash, git_branch, build_variant, github_org, github_repo)
      3. Test labels (test_name, topology, client_tool, tested_commands, etc.)
    """
    return {
        **startup_labels,
        **build_labels,
        **test_labels,
    }
