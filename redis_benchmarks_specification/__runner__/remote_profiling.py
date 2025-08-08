"""
Remote profiling utilities for Redis benchmark runner.

This module provides functionality to trigger remote profiling of Redis processes
via HTTP GET endpoints during benchmark execution. Profiles are collected in
pprof binary format for performance analysis.
"""

import datetime
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Any
import requests


def extract_redis_pid(redis_conn) -> Optional[int]:
    """
    Extract Redis process ID from Redis INFO command.

    Args:
        redis_conn: Redis connection object

    Returns:
        Redis process ID as integer, or None if not found
    """
    try:
        redis_info = redis_conn.info()
        pid = redis_info.get("process_id")
        if pid is not None:
            logging.info(f"Extracted Redis PID: {pid}")
            return int(pid)
        else:
            logging.warning("Redis process_id not found in INFO command")
            return None
    except Exception as e:
        logging.error(f"Failed to extract Redis PID: {e}")
        return None


def extract_redis_metadata(redis_conn) -> Dict[str, Any]:
    """
    Extract Redis metadata for profile comments.

    Args:
        redis_conn: Redis connection object

    Returns:
        Dictionary containing Redis metadata
    """
    try:
        redis_info = redis_conn.info()
        metadata = {
            "redis_version": redis_info.get("redis_version", "unknown"),
            "redis_git_sha1": redis_info.get("redis_git_sha1", "unknown"),
            "redis_git_dirty": redis_info.get("redis_git_dirty", "unknown"),
            "redis_build_id": redis_info.get("redis_build_id", "unknown"),
            "process_id": redis_info.get("process_id", "unknown"),
            "tcp_port": redis_info.get("tcp_port", "unknown"),
        }

        # Use build_id if git_sha1 is empty or 0
        if metadata["redis_git_sha1"] in ("", 0, "0"):
            metadata["redis_git_sha1"] = metadata["redis_build_id"]

        logging.info(
            f"Extracted Redis metadata: version={metadata['redis_version']}, sha={metadata['redis_git_sha1']}, pid={metadata['process_id']}"
        )
        return metadata
    except Exception as e:
        logging.error(f"Failed to extract Redis metadata: {e}")
        return {
            "redis_version": "unknown",
            "redis_git_sha1": "unknown",
            "redis_git_dirty": "unknown",
            "redis_build_id": "unknown",
            "process_id": "unknown",
            "tcp_port": "unknown",
        }


def extract_server_info_for_args(redis_conn) -> Dict[str, str]:
    """
    Extract server information from Redis INFO SERVER to auto-detect
    github_org, github_repo, github_version, and github_hash when not explicitly provided.

    Args:
        redis_conn: Redis connection object

    Returns:
        Dictionary containing detected server information:
        - github_org: Detected organization (e.g., 'redis', 'valkey-io')
        - github_repo: Detected repository (e.g., 'redis', 'valkey')
        - github_version: Detected version
        - github_hash: Detected git hash from redis_git_sha1
        - server_name: Server name from INFO
    """
    try:
        server_info = redis_conn.info("server")

        # Extract server name and version info
        server_name = server_info.get("server_name", "").lower()
        redis_version = server_info.get("redis_version", "unknown")

        # Extract git hash info
        redis_git_sha1 = server_info.get("redis_git_sha1", "")
        redis_build_id = server_info.get("redis_build_id", "")
        github_hash = "unknown"

        # Use git_sha1 if available and not empty/zero
        if redis_git_sha1 and redis_git_sha1 not in ("", "0", "00000000"):
            github_hash = redis_git_sha1
        # Fallback to build_id if git_sha1 is not available
        elif redis_build_id and redis_build_id not in ("", "0"):
            github_hash = redis_build_id

        # Default values
        github_org = "redis"
        github_repo = "redis"
        github_version = redis_version

        # Check for Valkey
        if "valkey" in server_name:
            github_org = "valkey-io"
            github_repo = "valkey"
            # Use valkey_version if available, fallback to redis_version
            valkey_version = server_info.get("valkey_version")
            if valkey_version:
                github_version = valkey_version

        logging.info(
            f"Auto-detected server info: org={github_org}, repo={github_repo}, "
            f"version={github_version}, hash={github_hash}, server_name={server_name}"
        )

        return {
            "github_org": github_org,
            "github_repo": github_repo,
            "github_version": github_version,
            "github_hash": github_hash,
            "server_name": server_name,
        }

    except Exception as e:
        logging.error(f"Failed to extract server info: {e}")
        return {
            "github_org": "redis",
            "github_repo": "redis",
            "github_version": "unknown",
            "github_hash": "unknown",
            "server_name": "unknown",
        }


def extract_server_metadata_for_timeseries(redis_conn) -> Dict[str, str]:
    """
    Extract comprehensive server metadata from Redis INFO SERVER for use as
    timeseries metadata tags.

    Args:
        redis_conn: Redis connection object

    Returns:
        Dictionary containing server metadata for timeseries tags:
        - os: Operating system information
        - arch_bits: Architecture bits (32/64)
        - gcc_version: GCC compiler version
        - server_mode: Server mode (standalone/cluster/sentinel)
        - multiplexing_api: Multiplexing API used (epoll/kqueue/etc)
        - atomicvar_api: Atomic variable API
        - redis_build_id: Build ID
        - redis_git_dirty: Git dirty flag
        - process_supervised: Process supervision status
        - availability_zone: Availability zone (if available)
        - And other interesting metadata fields
    """
    try:
        server_info = redis_conn.info("server")

        # Extract interesting metadata fields for timeseries tags
        metadata = {}

        # Core system information
        if "os" in server_info:
            metadata["os"] = str(server_info["os"])
        if "arch_bits" in server_info:
            metadata["arch_bits"] = str(server_info["arch_bits"])
        if "gcc_version" in server_info:
            metadata["gcc_version"] = str(server_info["gcc_version"])

        # Server configuration
        if "server_mode" in server_info:
            metadata["server_mode"] = str(server_info["server_mode"])
        elif "redis_mode" in server_info:  # Fallback for older versions
            metadata["server_mode"] = str(server_info["redis_mode"])

        # Performance-related APIs
        if "multiplexing_api" in server_info:
            metadata["multiplexing_api"] = str(server_info["multiplexing_api"])
        if "atomicvar_api" in server_info:
            metadata["atomicvar_api"] = str(server_info["atomicvar_api"])
        if "monotonic_clock" in server_info:
            metadata["monotonic_clock"] = str(server_info["monotonic_clock"])

        # Build information
        if "redis_build_id" in server_info:
            metadata["redis_build_id"] = str(server_info["redis_build_id"])
        if "redis_git_dirty" in server_info:
            metadata["redis_git_dirty"] = str(server_info["redis_git_dirty"])

        # Process information
        if "process_supervised" in server_info:
            metadata["process_supervised"] = str(server_info["process_supervised"])

        # Cloud/deployment information
        if "availability_zone" in server_info and server_info["availability_zone"]:
            metadata["availability_zone"] = str(server_info["availability_zone"])

        # IO threads (performance relevant)
        if "io_threads_active" in server_info:
            metadata["io_threads_active"] = str(server_info["io_threads_active"])

        # Server name and version info
        if "server_name" in server_info and server_info["server_name"]:
            metadata["server_name"] = str(server_info["server_name"])
        if "redis_version" in server_info:
            metadata["redis_version"] = str(server_info["redis_version"])
        if "valkey_version" in server_info:
            metadata["valkey_version"] = str(server_info["valkey_version"])
        if "valkey_release_stage" in server_info:
            metadata["valkey_release_stage"] = str(server_info["valkey_release_stage"])

        # Configuration file info
        if "config_file" in server_info and server_info["config_file"]:
            metadata["config_file"] = str(server_info["config_file"])
        else:
            metadata["config_file"] = "none"

        logging.info(
            f"Extracted {len(metadata)} server metadata fields for timeseries: {list(metadata.keys())}"
        )

        return metadata

    except Exception as e:
        logging.error(f"Failed to extract server metadata: {e}")
        return {}


def calculate_profile_duration(benchmark_duration_seconds: int) -> int:
    """
    Calculate profiling duration based on benchmark duration.

    Args:
        benchmark_duration_seconds: Expected benchmark duration in seconds

    Returns:
        Profiling duration in seconds (minimum: benchmark duration, maximum: 30)
    """
    # Minimum duration is the benchmark duration, maximum is 30 seconds
    duration = min(max(benchmark_duration_seconds, 10), 30)
    logging.info(
        f"Calculated profile duration: {duration}s (benchmark: {benchmark_duration_seconds}s)"
    )
    return duration


def trigger_remote_profile(
    host: str,
    port: int,
    pid: int,
    duration: int,
    timeout: int = 60,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> Optional[bytes]:
    """
    Trigger remote profiling via HTTP GET request using pprof endpoint.

    Args:
        host: Remote host address
        port: Remote port number
        pid: Redis process ID
        duration: Profiling duration in seconds
        timeout: HTTP request timeout in seconds
        username: Optional username for HTTP basic authentication
        password: Optional password for HTTP basic authentication

    Returns:
        Profile content in pprof binary format, or None if failed
    """
    url = f"http://{host}:{port}/debug/pprof/profile"
    params = {"pid": pid, "seconds": duration}

    # Prepare authentication if provided
    auth = None
    if username is not None and password is not None:
        auth = (username, password)
        logging.info(f"Using HTTP basic authentication with username: {username}")

    try:
        logging.info(
            f"Triggering remote profile: {url} with PID={pid}, duration={duration}s"
        )
        response = requests.get(url, params=params, timeout=timeout, auth=auth)
        response.raise_for_status()

        profile_content = response.content
        logging.info(f"Successfully collected profile: {len(profile_content)} bytes")
        return profile_content

    except requests.exceptions.Timeout:
        logging.error(f"Remote profiling request timed out after {timeout}s")
        return None
    except requests.exceptions.ConnectionError:
        logging.error(f"Failed to connect to remote profiling endpoint: {host}:{port}")
        return None
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error during remote profiling: {e}")
        if e.response.status_code == 401:
            logging.error("Authentication failed - check username and password")
        return None
    except Exception as e:
        logging.error(f"Unexpected error during remote profiling: {e}")
        return None


def save_profile_with_metadata(
    profile_content: bytes,
    benchmark_name: str,
    output_dir: str,
    redis_metadata: Dict[str, Any],
    duration: int,
) -> Optional[str]:
    """
    Save profile content to file in pprof binary format.

    Args:
        profile_content: Profile data in pprof binary format
        benchmark_name: Name of the benchmark
        output_dir: Output directory path
        redis_metadata: Redis metadata dictionary
        duration: Profiling duration in seconds

    Returns:
        Path to saved file, or None if failed
    """
    try:
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Generate filename with .pb.gz extension
        filename = f"{benchmark_name}.pb.gz"
        filepath = os.path.join(output_dir, filename)

        # Generate timestamp
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Write binary profile content directly
        with open(filepath, "wb") as f:
            f.write(profile_content)

        # Create a separate metadata file
        metadata_filename = f"{benchmark_name}.metadata.txt"
        metadata_filepath = os.path.join(output_dir, metadata_filename)

        metadata_content = (
            f"Profile from redis sha = {redis_metadata['redis_git_sha1']} "
            f"and pid {redis_metadata['process_id']} for duration of {duration}s. "
            f"Collection date: {timestamp}\n"
            f"benchmark_name={benchmark_name}\n"
            f"redis_git_sha1={redis_metadata['redis_git_sha1']}\n"
            f"redis_version={redis_metadata['redis_version']}\n"
            f"redis_git_dirty={redis_metadata['redis_git_dirty']}\n"
            f"redis_build_id={redis_metadata['redis_build_id']}\n"
            f"process_id={redis_metadata['process_id']}\n"
            f"tcp_port={redis_metadata['tcp_port']}\n"
            f"duration_seconds={duration}\n"
        )

        with open(metadata_filepath, "w") as f:
            f.write(metadata_content)

        logging.info(f"Saved profile to: {filepath}")
        logging.info(f"Saved metadata to: {metadata_filepath}")
        return filepath

    except Exception as e:
        logging.error(f"Failed to save profile file: {e}")
        return None


class RemoteProfiler:
    """
    Remote profiler class to handle threaded profiling execution.
    """

    def __init__(
        self,
        host: str,
        port: int,
        output_dir: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.output_dir = output_dir
        self.username = username
        self.password = password
        self.profile_thread = None
        self.profile_result = None
        self.profile_error = None

    def start_profiling(
        self, redis_conn, benchmark_name: str, benchmark_duration_seconds: int
    ) -> bool:
        """
        Start profiling in a separate thread.

        Args:
            redis_conn: Redis connection object
            benchmark_name: Name of the benchmark
            benchmark_duration_seconds: Expected benchmark duration

        Returns:
            True if profiling thread started successfully, False otherwise
        """
        try:
            # Extract Redis metadata and PID
            redis_metadata = extract_redis_metadata(redis_conn)
            pid = redis_metadata.get("process_id")

            if pid == "unknown" or pid is None:
                logging.error("Cannot start remote profiling: Redis PID not available")
                return False

            # Calculate profiling duration
            duration = calculate_profile_duration(benchmark_duration_seconds)

            # Start profiling thread
            self.profile_thread = threading.Thread(
                target=self._profile_worker,
                args=(pid, duration, benchmark_name, redis_metadata),
                daemon=True,
            )
            self.profile_thread.start()

            logging.info(
                f"Started remote profiling thread for benchmark: {benchmark_name}"
            )
            return True

        except Exception as e:
            logging.error(f"Failed to start remote profiling: {e}")
            return False

    def _profile_worker(
        self,
        pid: int,
        duration: int,
        benchmark_name: str,
        redis_metadata: Dict[str, Any],
    ):
        """
        Worker function for profiling thread.
        """
        try:
            # Trigger remote profiling
            profile_content = trigger_remote_profile(
                self.host,
                self.port,
                pid,
                duration,
                username=self.username,
                password=self.password,
            )

            if profile_content is not None:
                # Save profile with metadata
                filepath = save_profile_with_metadata(
                    profile_content,
                    benchmark_name,
                    self.output_dir,
                    redis_metadata,
                    duration,
                )
                self.profile_result = filepath
            else:
                self.profile_error = "Failed to collect profile content"

        except Exception as e:
            self.profile_error = f"Profile worker error: {e}"
            logging.error(self.profile_error)

    def wait_for_completion(self, timeout: int = 60) -> bool:
        """
        Wait for profiling thread to complete.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if completed successfully, False if timed out or failed
        """
        if self.profile_thread is None:
            return False

        try:
            self.profile_thread.join(timeout=timeout)

            if self.profile_thread.is_alive():
                logging.warning(
                    f"Remote profiling thread did not complete within {timeout}s"
                )
                return False

            if self.profile_error:
                logging.error(f"Remote profiling failed: {self.profile_error}")
                return False

            if self.profile_result:
                logging.info(
                    f"Remote profiling completed successfully: {self.profile_result}"
                )
                return True
            else:
                logging.warning("Remote profiling completed but no result available")
                return False

        except Exception as e:
            logging.error(f"Error waiting for remote profiling completion: {e}")
            return False
