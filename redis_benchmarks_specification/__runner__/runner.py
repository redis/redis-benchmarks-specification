# Import warning suppression first
from redis_benchmarks_specification.__common__.suppress_warnings import *

import datetime
import json
import logging
import math
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
import re
import tqdm
from urllib.parse import urlparse
import docker
import redis
from docker.models.containers import Container
from pytablewriter import CsvTableWriter, MarkdownTableWriter
from redisbench_admin.profilers.profilers_local import (
    check_compatible_system_and_kernel_and_prepare_profile,
)
from redisbench_admin.run.common import (
    get_start_time_vars,
    merge_default_and_config_metrics,
    prepare_benchmark_parameters,
)

from redis_benchmarks_specification.__common__.runner import (
    export_redis_metrics,
)

from redisbench_admin.profilers.profilers_local import (
    local_profilers_platform_checks,
    profilers_start_if_required,
    profilers_stop_if_required,
)
from redisbench_admin.run.common import (
    dbconfig_keyspacelen_check,
)
from redisbench_admin.run.metrics import extract_results_table
from redisbench_admin.run.run import calculate_client_tool_duration_and_check
from redisbench_admin.utils.benchmark_config import (
    get_final_benchmark_config,
    get_defaults,
)
from redisbench_admin.utils.local import get_local_run_full_filename
from redisbench_admin.utils.results import post_process_benchmark_results

from redis_benchmarks_specification.__common__.env import (
    LOG_DATEFMT,
    LOG_FORMAT,
    LOG_LEVEL,
    REDIS_HEALTH_CHECK_INTERVAL,
    REDIS_SOCKET_TIMEOUT,
    S3_BUCKET_NAME,
)
from redis_benchmarks_specification.__common__.package import (
    get_version_string,
    populate_with_poetry_data,
)
from redis_benchmarks_specification.__common__.runner import (
    extract_testsuites,
    exporter_datasink_common,
    reset_commandstats,
    execute_init_commands,
    export_redis_metrics,
)
from redis_benchmarks_specification.__common__.spec import (
    extract_client_container_image,
    extract_client_cpu_limit,
    extract_client_tool,
    extract_client_configs,
    extract_client_container_images,
    extract_client_tools,
)
from redis_benchmarks_specification.__runner__.args import create_client_runner_args
from redis_benchmarks_specification.__runner__.remote_profiling import RemoteProfiler


# Global flag to track if user wants to exit
_exit_requested = False


def signal_handler(signum, frame):
    """Handle Ctrl+C signal to exit gracefully"""
    global _exit_requested
    if not _exit_requested:
        _exit_requested = True
        logging.info("Ctrl+C detected. Exiting after current test completes...")
        print("\nCtrl+C detected. Exiting after current test completes...")
    else:
        logging.info("Ctrl+C detected again. Force exiting...")
        print("\nForce exiting...")
        sys.exit(1)


def parse_redis_uri(uri):
    """
    Parse Redis URI and extract connection parameters.

    Args:
        uri (str): Redis URI in format redis://user:password@host:port/dbnum
                   or rediss://user:password@host:port/dbnum for TLS

    Returns:
        dict: Dictionary containing parsed connection parameters
    """
    if not uri:
        return {}

    try:
        parsed = urlparse(uri)

        # Extract connection parameters
        params = {}

        # Host (required)
        if parsed.hostname:
            params["host"] = parsed.hostname

        # Port (optional, defaults to 6379)
        if parsed.port:
            params["port"] = parsed.port

        # Username and password
        if parsed.username:
            params["username"] = parsed.username
        if parsed.password:
            params["password"] = parsed.password

        # Database number
        if parsed.path and len(parsed.path) > 1:  # path starts with '/'
            try:
                params["db"] = int(parsed.path[1:])  # Remove leading '/'
            except ValueError:
                logging.warning(f"Invalid database number in URI: {parsed.path[1:]}")

        # TLS detection
        if parsed.scheme == "rediss":
            params["tls_enabled"] = True
        elif parsed.scheme == "redis":
            params["tls_enabled"] = False
        else:
            logging.warning(
                f"Unknown scheme in URI: {parsed.scheme}. Assuming non-TLS."
            )
            params["tls_enabled"] = False

        logging.info(
            f"Parsed Redis URI: host={params.get('host', 'N/A')}, "
            f"port={params.get('port', 'N/A')}, "
            f"username={params.get('username', 'N/A')}, "
            f"db={params.get('db', 'N/A')}, "
            f"tls={params.get('tls_enabled', False)}"
        )

        return params

    except Exception as e:
        logging.error(f"Failed to parse Redis URI '{uri}': {e}")
        return {}


def validate_benchmark_metrics(
    results_dict, test_name, benchmark_config=None, default_metrics=None
):
    """
    Validate benchmark metrics to ensure they contain reasonable values.
    Fails the test if critical metrics indicate something is wrong.

    Args:
        results_dict: Dictionary containing benchmark results
        test_name: Name of the test being validated
        benchmark_config: Benchmark configuration (optional, contains tested-commands)
        default_metrics: Default metrics configuration (unused, for compatibility)

    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        # Get tested commands from config if available
        tested_commands = []
        if benchmark_config and "tested-commands" in benchmark_config:
            tested_commands = [
                cmd.lower() for cmd in benchmark_config["tested-commands"]
            ]

        # Define validation rules
        throughput_patterns = [
            "ops/sec",
            "qps",
            "totals.ops/sec",
            "all_stats.totals.ops/sec",
        ]

        latency_patterns = ["p50", "p95", "p99", "p999", "percentile"]

        validation_errors = []

        def check_nested_dict(data, path=""):
            """Recursively check nested dictionary for metrics"""
            if isinstance(data, dict):
                for key, value in data.items():
                    current_path = f"{path}.{key}" if path else key
                    check_nested_dict(value, current_path)
            elif isinstance(data, (int, float)):
                metric_path_lower = path.lower()

                # Skip Waits metrics as they can legitimately be 0
                if "waits" in metric_path_lower:
                    return

                # Skip general latency metrics that can legitimately be 0
                # Only validate specific percentile latencies (p50, p95, etc.)
                if any(
                    pattern in metric_path_lower
                    for pattern in [
                        "average latency",
                        "totals.latency",
                        "all_stats.totals.latency",
                    ]
                ):
                    return

                # Skip operation-specific metrics for operations not being tested
                # For example, skip Gets.Ops/sec if only SET commands are tested
                if tested_commands:
                    skip_metric = False
                    operation_types = [
                        "gets",
                        "sets",
                        "hgets",
                        "hsets",
                        "lpush",
                        "rpush",
                        "sadd",
                    ]
                    for op_type in operation_types:
                        if (
                            op_type in metric_path_lower
                            and op_type not in tested_commands
                        ):
                            skip_metric = True
                            break
                    if skip_metric:
                        return

                # Check throughput metrics
                for pattern in throughput_patterns:
                    if pattern in metric_path_lower:
                        if data <= 10:  # Below 10 QPS threshold
                            validation_errors.append(
                                f"Throughput metric '{path}' has invalid value: {data} "
                                f"(below 10 QPS threshold)"
                            )
                        break

                # Check latency metrics
                for pattern in latency_patterns:
                    if pattern in metric_path_lower:
                        if data <= 0.0:  # Invalid latency
                            validation_errors.append(
                                f"Latency metric '{path}' has invalid value: {data} "
                                f"(should be > 0.0)"
                            )
                        break

        # Validate the results dictionary
        check_nested_dict(results_dict)

        if validation_errors:
            error_msg = f"Test {test_name} failed metric validation:\n" + "\n".join(
                validation_errors
            )
            logging.error(error_msg)
            return False, error_msg

        logging.info(f"Test {test_name} passed metric validation")
        return True, None

    except Exception as e:
        logging.warning(f"Error during metric validation for test {test_name}: {e}")
        # Don't fail the test if validation itself fails
        return True, None


def run_local_command_with_timeout(command_str, timeout_seconds, description="command"):
    """
    Run a local command with timeout support.

    Args:
        command_str: The command string to execute
        timeout_seconds: Timeout in seconds
        description: Description for logging

    Returns:
        tuple: (success, stdout, stderr)
    """
    try:
        logging.info(
            f"Running {description} with {timeout_seconds}s timeout: {command_str}"
        )

        # Use shell=True to support complex command strings with pipes, etc.
        process = subprocess.Popen(
            command_str,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            stdout, stderr = process.communicate(timeout=timeout_seconds)
            return_code = process.returncode

            if return_code == 0:
                logging.info(f"{description} completed successfully")
                return True, stdout, stderr
            else:
                logging.error(f"{description} failed with return code {return_code}")
                logging.error(f"stderr: {stderr}")
                return False, stdout, stderr

        except subprocess.TimeoutExpired:
            logging.error(f"{description} timed out after {timeout_seconds} seconds")
            process.kill()
            try:
                stdout, stderr = process.communicate(
                    timeout=5
                )  # Give 5 seconds to cleanup
            except subprocess.TimeoutExpired:
                stdout, stderr = "", "Process killed due to timeout"
            return False, stdout, f"Timeout after {timeout_seconds} seconds. {stderr}"

    except Exception as e:
        logging.error(f"Error running {description}: {e}")
        return False, "", str(e)


def calculate_process_timeout(command_str, buffer_timeout):
    """
    Calculate timeout for a process based on test-time parameter and buffer.

    Args:
        command_str: The command string to analyze
        buffer_timeout: Buffer time to add to test-time

    Returns:
        int: Timeout in seconds
    """
    default_timeout = 300  # 5 minutes default
    run_count = 1
    if "run-count" in command_str:
        # Try to extract test time and add buffer
        # Handle both --test-time (memtier) and -test-time (pubsub-sub-bench)
        run_count_match = re.search(r"--?run-count[=\s]+(\d+)", command_str)
        if run_count_match:
            run_count = int(run_count_match.group(1))
            logging.info(f"Detected run count of: {run_count}")
        run_count_match = re.search(r"-?x[=\s]+(\d+)", command_str)
        if run_count_match:
            run_count = int(run_count_match.group(1))
            logging.info(f"Detected run count (from -x) of: {run_count}")

    if "test-time" in command_str:
        # Try to extract test time and add buffer
        # Handle both --test-time (memtier) and -test-time (pubsub-sub-bench)
        test_time_match = re.search(r"--?test-time[=\s]+(\d+)", command_str)
        if test_time_match:
            test_time = int(test_time_match.group(1))
            timeout = (test_time + buffer_timeout) * run_count
            logging.info(
                f"Set process timeout to {timeout}s (test-time: {test_time}s + {buffer_timeout}s buffer) x {run_count} runs)"
            )
            return timeout

    logging.info(f"Using default process timeout: {default_timeout}s")
    return default_timeout


def parse_size(size):
    units = {
        "B": 1,
        "KB": 2**10,
        "MB": 2**20,
        "GB": 2**30,
        "TB": 2**40,
        "": 1,
        "KIB": 10**3,
        "MIB": 10**6,
        "GIB": 10**9,
        "TIB": 10**12,
        "K": 2**10,
        "M": 2**20,
        "G": 2**30,
        "T": 2**40,
        "": 1,
        "KI": 10**3,
        "MI": 10**6,
        "GI": 10**9,
        "TI": 10**12,
    }
    m = re.match(r"^([\d\.]+)\s*([a-zA-Z]{0,3})$", str(size).strip())
    number, unit = float(m.group(1)), m.group(2).upper()
    return int(number * units[unit])


def extract_expected_benchmark_duration(
    benchmark_command_str, override_memtier_test_time
):
    """
    Extract expected benchmark duration from command string or override.

    Args:
        benchmark_command_str: The benchmark command string
        override_memtier_test_time: Override test time value

    Returns:
        Expected duration in seconds, or 30 as default
    """
    if override_memtier_test_time > 0:
        return override_memtier_test_time

    # Try to extract test-time from command string
    if "test-time" in benchmark_command_str:
        # Handle both --test-time (memtier) and -test-time (pubsub-sub-bench)
        test_time_match = re.search(r"--?test-time[=\s]+(\d+)", benchmark_command_str)
        if test_time_match:
            return int(test_time_match.group(1))

    # Default duration if not found
    return 30


def detect_object_encoding(redis_conn, dbconfig):
    """
    Detect object encoding by scanning 1% of the dataset.

    Args:
        redis_conn: Redis connection
        dbconfig: Database configuration containing keyspace info

    Returns:
        Dict with encoding information
    """
    try:
        # Get total key count
        total_keys = redis_conn.dbsize()
        logging.debug(f"Object encoding detection: DBSIZE reports {total_keys} keys")

        if total_keys == 0:
            logging.warning("No keys found in database for encoding detection")
            return {
                "encoding": "unknown",
                "confidence": 0.0,
                "sample_size": 0,
                "total_keys": 0,
                "encoding_distribution": {},
            }

        # Determine scanning strategy based on dataset size
        if total_keys <= 1000:
            # For small datasets, scan all keys for complete accuracy
            sample_size = total_keys
            scan_all_keys = True
            logging.info(
                f"Scanning all {total_keys} keys (small dataset - complete analysis)"
            )
        else:
            # For large datasets, sample 1% (minimum 10, maximum 1000)
            sample_size = max(10, min(1000, int(total_keys * 0.01)))
            scan_all_keys = False
            logging.info(
                f"Sampling {sample_size} keys out of {total_keys} total keys ({(sample_size/total_keys)*100:.2f}%)"
            )

        # Use SCAN to get keys
        encoding_counts = {}
        scanned_keys = []
        cursor = 0

        if scan_all_keys:
            # Scan all keys in the database
            while True:
                cursor, keys = redis_conn.scan(cursor=cursor, count=100)
                scanned_keys.extend(keys)

                # Break if we've completed a full scan
                if cursor == 0:
                    break
        else:
            # Sample keys until we reach our target sample size
            while len(scanned_keys) < sample_size:
                cursor, keys = redis_conn.scan(
                    cursor=cursor, count=min(100, sample_size - len(scanned_keys))
                )
                scanned_keys.extend(keys)

                # Break if we've completed a full scan
                if cursor == 0:
                    break

            # Limit to our target sample size
            scanned_keys = scanned_keys[:sample_size]

        logging.debug(
            f"SCAN completed: found {len(scanned_keys)} keys, cursor ended at {cursor}"
        )

        # If SCAN didn't find any keys but we know there are keys, try KEYS command as fallback
        if len(scanned_keys) == 0 and total_keys > 0:
            logging.warning(
                f"SCAN found no keys but DBSIZE reports {total_keys} keys. Trying KEYS fallback."
            )
            try:
                # Use KEYS * as fallback (only for small datasets to avoid blocking)
                if total_keys <= 1000:
                    all_keys = redis_conn.keys("*")
                    scanned_keys = (
                        all_keys[:sample_size] if not scan_all_keys else all_keys
                    )
                    logging.info(f"KEYS fallback found {len(scanned_keys)} keys")
                else:
                    logging.error(
                        f"Cannot use KEYS fallback for large dataset ({total_keys} keys)"
                    )
            except Exception as e:
                logging.error(f"KEYS fallback failed: {e}")

        # Final check: if we still have no keys, return early
        if len(scanned_keys) == 0:
            logging.error(
                f"No keys found for encoding detection despite DBSIZE={total_keys}"
            )
            return {
                "encoding": "unknown",
                "confidence": 0.0,
                "sample_size": 0,
                "total_keys": total_keys,
                "encoding_distribution": {},
                "is_complete_scan": scan_all_keys,
                "error": "No keys found by SCAN or KEYS commands",
            }

        # Get encoding for each sampled key
        successful_encodings = 0
        for i, key in enumerate(scanned_keys):
            try:
                # Use the redis-py object_encoding method instead of raw command
                encoding = redis_conn.object("ENCODING", key)
                if isinstance(encoding, bytes):
                    encoding = encoding.decode("utf-8")
                elif encoding is None:
                    # Key might have expired or been deleted
                    logging.debug(
                        f"Key '{key}' returned None encoding (key may have expired)"
                    )
                    continue

                encoding_counts[encoding] = encoding_counts.get(encoding, 0) + 1
                successful_encodings += 1

                # Log first few keys for debugging
                if i < 3:
                    logging.debug(f"Key '{key}' has encoding '{encoding}'")

            except Exception as e:
                logging.warning(f"Failed to get encoding for key {key}: {e}")
                continue

        logging.debug(
            f"Successfully got encoding for {successful_encodings}/{len(scanned_keys)} keys"
        )

        if not encoding_counts:
            logging.warning(
                f"No object encodings detected! Scanned {len(scanned_keys)} keys, successful encodings: {successful_encodings}"
            )
            return {
                "encoding": "unknown",
                "confidence": 0.0,
                "sample_size": 0,
                "total_keys": total_keys,
                "encoding_distribution": {},
                "is_complete_scan": scan_all_keys,
            }

        # Determine dominant encoding
        total_sampled = sum(encoding_counts.values())
        dominant_encoding = max(encoding_counts.items(), key=lambda x: x[1])
        confidence = dominant_encoding[1] / total_sampled

        # Calculate encoding distribution percentages
        encoding_distribution = {
            enc: (count / total_sampled) * 100 for enc, count in encoding_counts.items()
        }

        result = {
            "encoding": dominant_encoding[0],
            "confidence": confidence,
            "sample_size": total_sampled,
            "total_keys": total_keys,
            "encoding_distribution": encoding_distribution,
            "is_complete_scan": scan_all_keys,
        }

        scan_type = "complete scan" if scan_all_keys else "sample"
        logging.info(
            f"Object encoding analysis ({scan_type}): {dominant_encoding[0]} ({confidence*100:.1f}% confidence)"
        )
        if len(encoding_counts) > 1:
            logging.info(f"Encoding distribution: {encoding_distribution}")

        return result

    except Exception as e:
        logging.error(f"Failed to detect object encoding: {e}")
        return {
            "encoding": "error",
            "confidence": 0.0,
            "sample_size": 0,
            "total_keys": 0,
            "encoding_distribution": {},
            "error": str(e),
        }


def run_multiple_clients(
    benchmark_config,
    docker_client,
    temporary_dir_client,
    client_mnt_point,
    benchmark_tool_workdir,
    client_cpuset_cpus,
    port,
    host,
    password,
    oss_cluster_api_enabled,
    tls_enabled,
    tls_skip_verify,
    test_tls_cert,
    test_tls_key,
    test_tls_cacert,
    resp_version,
    override_memtier_test_time,
    override_test_runs,
    unix_socket,
    args,
):
    """
    Run multiple client configurations simultaneously and aggregate results.
    Returns aggregated stdout and list of individual results.
    """
    client_configs = extract_client_configs(benchmark_config)
    client_images = extract_client_container_images(benchmark_config)
    client_tools = extract_client_tools(benchmark_config)

    if not client_configs:
        raise ValueError("No client configurations found")

    containers = []
    results = []

    # Start all containers simultaneously (detached)
    for client_index, (client_config, client_tool, client_image) in enumerate(
        zip(client_configs, client_tools, client_images)
    ):
        try:
            local_benchmark_output_filename = f"benchmark_output_{client_index}.json"

            # Prepare benchmark command for this client
            if "memtier_benchmark" in client_tool:
                # Set benchmark path based on local install option
                if args.benchmark_local_install:
                    full_benchmark_path = getattr(
                        args, "memtier_bin_path", "memtier_benchmark"
                    )
                else:
                    full_benchmark_path = f"/usr/local/bin/{client_tool}"

                (
                    _,
                    benchmark_command_str,
                    arbitrary_command,
                ) = prepare_memtier_benchmark_parameters(
                    client_config,
                    full_benchmark_path,
                    port,
                    host,
                    password,
                    local_benchmark_output_filename,
                    oss_cluster_api_enabled,
                    tls_enabled,
                    tls_skip_verify,
                    test_tls_cert,
                    test_tls_key,
                    test_tls_cacert,
                    resp_version,
                    override_memtier_test_time,
                    override_test_runs,
                    unix_socket,
                )
            elif "pubsub-sub-bench" in client_tool:
                (
                    _,
                    benchmark_command_str,
                    arbitrary_command,
                ) = prepare_pubsub_sub_bench_parameters(
                    client_config,
                    client_tool,
                    port,
                    host,
                    password,
                    local_benchmark_output_filename,
                    oss_cluster_api_enabled,
                    tls_enabled,
                    tls_skip_verify,
                    test_tls_cert,
                    test_tls_key,
                    test_tls_cacert,
                    resp_version,
                    override_memtier_test_time,
                    unix_socket,
                    None,  # username
                )
            elif "vector-db-benchmark" in client_tool:
                (
                    _,
                    benchmark_command_str,
                    arbitrary_command,
                    client_env_vars,
                ) = prepare_vector_db_benchmark_parameters(
                    client_config,
                    client_tool,
                    port,
                    host,
                    password,
                    local_benchmark_output_filename,
                    oss_cluster_api_enabled,
                    tls_enabled,
                    tls_skip_verify,
                    test_tls_cert,
                    test_tls_key,
                    test_tls_cacert,
                    resp_version,
                    override_memtier_test_time,
                    unix_socket,
                    None,  # username
                )
            else:
                # Handle other benchmark tools
                (
                    benchmark_command,
                    benchmark_command_str,
                ) = prepare_benchmark_parameters(
                    {**benchmark_config, "clientconfig": client_config},
                    client_tool,
                    port,
                    host,
                    local_benchmark_output_filename,
                    False,
                    benchmark_tool_workdir,
                    False,
                )

            # Calculate container timeout
            container_timeout = 300  # 5 minutes default
            # Use new timeout_buffer argument, fallback to container_timeout_buffer for backward compatibility
            buffer_timeout = getattr(
                args, "timeout_buffer", getattr(args, "container_timeout_buffer", 60)
            )
            if "test-time" in benchmark_command_str:
                # Try to extract test time and add buffer
                import re

                # Handle both --test-time (memtier) and -test-time (pubsub-sub-bench)
                test_time_match = re.search(
                    r"--?test-time[=\s]+(\d+)", benchmark_command_str
                )
                if test_time_match:
                    test_time = int(test_time_match.group(1))
                    container_timeout = test_time + buffer_timeout
                    logging.info(
                        f"Client {client_index}: Set container timeout to {container_timeout}s (test-time: {test_time}s + {buffer_timeout}s buffer)"
                    )

            logging.info(
                f"Starting client {client_index} with docker image {client_image} (cpuset={client_cpuset_cpus}) with args: {benchmark_command_str}"
            )

            # Start container (detached)
            # Set working directory based on tool
            working_dir = benchmark_tool_workdir
            if "vector-db-benchmark" in client_tool:
                working_dir = "/app"  # vector-db-benchmark needs to run from /app

            # Prepare container arguments
            volumes = {
                temporary_dir_client: {
                    "bind": client_mnt_point,
                    "mode": "rw",
                },
            }

            # For vector-db-benchmark, also mount the results directory
            if "vector-db-benchmark" in client_tool:
                volumes[temporary_dir_client] = {
                    "bind": "/app/results",
                    "mode": "rw",
                }

            container_kwargs = {
                "image": client_image,
                "volumes": volumes,
                "auto_remove": False,
                "privileged": True,
                "working_dir": working_dir,
                "command": benchmark_command_str,
                "network_mode": "host",
                "detach": True,
                "cpuset_cpus": client_cpuset_cpus,
            }

            # Only add user for non-vector-db-benchmark tools to avoid permission issues
            if "vector-db-benchmark" not in client_tool:
                container_kwargs["user"] = f"{os.getuid()}:{os.getgid()}"

            # Add environment variables for vector-db-benchmark
            if "vector-db-benchmark" in client_tool:
                try:
                    container_kwargs["environment"] = client_env_vars
                except NameError:
                    # client_env_vars not defined, skip environment variables
                    pass

            container = docker_client.containers.run(**container_kwargs)

            containers.append(
                {
                    "container": container,
                    "client_index": client_index,
                    "client_tool": client_tool,
                    "client_image": client_image,
                    "benchmark_command_str": benchmark_command_str,
                    "timeout": container_timeout,
                }
            )

        except Exception as e:
            error_msg = f"Error starting client {client_index}: {e}"
            logging.error(error_msg)
            logging.error(f"Image: {client_image}, Tool: {client_tool}")
            logging.error(f"Command: {benchmark_command_str}")
            # Fail fast on container startup errors
            raise RuntimeError(f"Failed to start client {client_index}: {e}")

    # Wait for all containers to complete
    logging.info(f"Waiting for {len(containers)} containers to complete...")

    for container_info in containers:
        container = container_info["container"]
        client_index = container_info["client_index"]
        client_tool = container_info["client_tool"]
        client_image = container_info["client_image"]
        benchmark_command_str = container_info["benchmark_command_str"]

        try:
            # Wait for container to complete
            exit_code = container.wait(timeout=container_info["timeout"])
            client_stdout = container.logs().decode("utf-8")

            # Check if container succeeded
            if exit_code.get("StatusCode", 1) != 0:
                logging.error(
                    f"Client {client_index} failed with exit code: {exit_code}"
                )
                logging.error(f"Client {client_index} stdout/stderr:")
                logging.error(client_stdout)
                # Fail fast on container execution errors
                raise RuntimeError(
                    f"Client {client_index} ({client_tool}) failed with exit code {exit_code}"
                )

            logging.info(
                f"Client {client_index} completed successfully with exit code: {exit_code}"
            )

            results.append(
                {
                    "client_index": client_index,
                    "stdout": client_stdout,
                    "config": client_configs[client_index],
                    "tool": client_tool,
                    "image": client_image,
                }
            )

        except Exception as e:
            # Get logs even if wait failed
            try:
                client_stdout = container.logs().decode("utf-8")
                logging.error(f"Client {client_index} logs:")
                logging.error(client_stdout)
            except:
                logging.error(f"Could not retrieve logs for client {client_index}")

            raise RuntimeError(f"Client {client_index} ({client_tool}) failed: {e}")

        finally:
            # Clean up container
            try:
                container.remove(force=True)
            except Exception as cleanup_error:
                logging.warning(f"Client {client_index} cleanup error: {cleanup_error}")

    logging.info(f"Successfully completed {len(containers)} client configurations")

    # Aggregate results by reading JSON output files
    aggregated_stdout = ""
    successful_results = [r for r in results if "error" not in r]

    if successful_results:
        # Try to read and aggregate JSON output files

        aggregated_json = {}
        memtier_json = None
        pubsub_json = None
        vector_json = None

        for result in successful_results:
            client_index = result["client_index"]
            tool = result["tool"]

            # Look for JSON output file
            json_filename = f"benchmark_output_{client_index}.json"
            json_filepath = os.path.join(temporary_dir_client, json_filename)

            if os.path.exists(json_filepath):
                try:
                    with open(json_filepath, "r") as f:
                        client_json = json.load(f)

                    if "memtier_benchmark" in tool:
                        # Store memtier JSON
                        memtier_json = client_json
                        logging.info(
                            f"Successfully read memtier JSON output from client {client_index}"
                        )
                    elif "pubsub-sub-bench" in tool:
                        # Store pubsub JSON
                        pubsub_json = client_json
                        logging.info(
                            f"Successfully read pubsub-sub-bench JSON output from client {client_index}"
                        )
                    elif "vector-db-benchmark" in tool:
                        # For vector-db-benchmark, look for summary JSON file
                        summary_files = [
                            f
                            for f in os.listdir(temporary_dir_client)
                            if f.endswith("-summary.json")
                        ]
                        if summary_files:
                            summary_filepath = os.path.join(
                                temporary_dir_client, summary_files[0]
                            )
                            try:
                                with open(summary_filepath, "r") as f:
                                    vector_json = json.load(f)
                                logging.info(
                                    f"Successfully read vector-db-benchmark JSON output from {summary_files[0]}"
                                )
                            except Exception as e:
                                logging.warning(
                                    f"Failed to read vector-db-benchmark JSON from {summary_files[0]}: {e}"
                                )
                        else:
                            logging.warning(
                                f"No vector-db-benchmark summary JSON file found for client {client_index}"
                            )

                    logging.info(
                        f"Successfully read JSON output from client {client_index} ({tool})"
                    )

                except Exception as e:
                    logging.warning(
                        f"Failed to read JSON from client {client_index}: {e}"
                    )
                    # Fall back to stdout
                    pass
            else:
                logging.warning(
                    f"JSON output file not found for client {client_index}: {json_filepath}"
                )

        # Merge JSON outputs from all tools
        if memtier_json and pubsub_json and vector_json:
            # Use memtier as base and add other metrics
            aggregated_json = memtier_json.copy()
            aggregated_json.update(pubsub_json)
            aggregated_json.update(vector_json)
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info(
                "Using merged JSON results from memtier, pubsub-sub-bench, and vector-db-benchmark clients"
            )
        elif memtier_json and pubsub_json:
            # Use memtier as base and add pubsub metrics
            aggregated_json = memtier_json.copy()
            aggregated_json.update(pubsub_json)
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info(
                "Using merged JSON results from memtier and pubsub-sub-bench clients"
            )
        elif memtier_json and vector_json:
            # Use memtier as base and add vector metrics
            aggregated_json = memtier_json.copy()
            aggregated_json.update(vector_json)
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info(
                "Using merged JSON results from memtier and vector-db-benchmark clients"
            )
        elif pubsub_json and vector_json:
            # Use pubsub as base and add vector metrics
            aggregated_json = pubsub_json.copy()
            aggregated_json.update(vector_json)
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info(
                "Using merged JSON results from pubsub-sub-bench and vector-db-benchmark clients"
            )
        elif memtier_json:
            # Only memtier available
            aggregated_json = memtier_json
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info("Using JSON results from memtier client only")
        elif pubsub_json:
            # Only pubsub available
            aggregated_json = pubsub_json
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info("Using JSON results from pubsub-sub-bench client only")
        elif vector_json:
            # Only vector-db-benchmark available
            aggregated_json = vector_json
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info("Using JSON results from vector-db-benchmark client only")
        else:
            # Fall back to concatenated stdout
            aggregated_stdout = "\n".join([r["stdout"] for r in successful_results])
            logging.warning(
                "No JSON results found, falling back to concatenated stdout"
            )

    return aggregated_stdout, results


def main():
    # Register signal handler for graceful exit on Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)

    _, _, project_version = populate_with_poetry_data()
    project_name_suffix = "redis-benchmarks-spec-client-runner"
    project_name = f"{project_name_suffix} (solely client)"
    parser = create_client_runner_args(
        get_version_string(project_name, project_version)
    )
    args = parser.parse_args()

    run_client_runner_logic(args, project_name, project_name_suffix, project_version)


def run_client_runner_logic(args, project_name, project_name_suffix, project_version):
    if args.logname is not None:
        print(f"Writting log to {args.logname}")
        logging.basicConfig(
            filename=args.logname,
            filemode="a",
            format=LOG_FORMAT,
            datefmt=LOG_DATEFMT,
            level=LOG_LEVEL,
        )
    else:
        # logging settings
        logging.basicConfig(
            format=LOG_FORMAT,
            level=LOG_LEVEL,
            datefmt=LOG_DATEFMT,
        )
    logging.info(get_version_string(project_name, project_version))
    testsuite_spec_files = extract_testsuites(args)
    datasink_conn = None
    if args.datasink_push_results_redistimeseries:
        logging.info(
            "Checking redistimeseries datasink connection is available at: {}:{} to push the timeseries data".format(
                args.datasink_redistimeseries_host, args.datasink_redistimeseries_port
            )
        )
        try:
            datasink_conn = redis.StrictRedis(
                host=args.datasink_redistimeseries_host,
                port=args.datasink_redistimeseries_port,
                decode_responses=True,
                password=args.datasink_redistimeseries_pass,
                username=args.datasink_redistimeseries_user,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
                socket_keepalive=True,
            )
            datasink_conn.ping()
            datasink_conn.client_setname(project_name_suffix)
        except redis.exceptions.ConnectionError as e:
            logging.error(
                "Unable to connect to redis available at: {}:{}".format(
                    args.datasink_redistimeseries_host,
                    args.datasink_redistimeseries_port,
                )
            )
            logging.error(f"Error message {e.__str__()}")
            exit(1)
    running_platform = args.platform_name
    tls_enabled = args.tls
    tls_skip_verify = args.tls_skip_verify
    tls_cert = args.cert
    tls_key = args.key
    tls_cacert = args.cacert
    resp_version = args.resp
    client_aggregated_results_folder = args.client_aggregated_results_folder
    preserve_temporary_client_dirs = args.preserve_temporary_client_dirs

    docker_client = docker.from_env()
    home = str(Path.home())
    profilers_list = []
    profilers_enabled = args.enable_profilers
    if profilers_enabled:
        profilers_list = args.profilers.split(",")
        res = check_compatible_system_and_kernel_and_prepare_profile(args)
        if res is False:
            logging.error(
                "Requested for the following profilers to be enabled but something went wrong: {}.".format(
                    " ".join(profilers_list)
                )
            )
            exit(1)
    override_memtier_test_time = args.override_memtier_test_time
    if override_memtier_test_time > 0:
        logging.info(
            "Overriding memtier benchmark --test-time to {} seconds".format(
                override_memtier_test_time
            )
        )
    logging.info("Running the benchmark specs.")
    process_self_contained_coordinator_stream(
        args,
        args.datasink_push_results_redistimeseries,
        docker_client,
        home,
        None,
        datasink_conn,
        testsuite_spec_files,
        {},
        running_platform,
        profilers_enabled,
        profilers_list,
        tls_enabled,
        tls_skip_verify,
        tls_cert,
        tls_key,
        tls_cacert,
        client_aggregated_results_folder,
        preserve_temporary_client_dirs,
        resp_version,
        override_memtier_test_time,
    )


def prepare_memtier_benchmark_parameters(
    clientconfig,
    full_benchmark_path,
    port,
    server,
    password,
    local_benchmark_output_filename,
    oss_cluster_api_enabled=False,
    tls_enabled=False,
    tls_skip_verify=False,
    tls_cert=None,
    tls_key=None,
    tls_cacert=None,
    resp_version=None,
    override_memtier_test_time=0,
    override_test_runs=1,
    unix_socket="",
):
    arbitrary_command = False
    benchmark_command = [
        full_benchmark_path,
        "--json-out-file",
        local_benchmark_output_filename,
    ]
    if unix_socket != "":
        benchmark_command.extend(["--unix-socket", unix_socket])
        logging.info(f"Using UNIX SOCKET to connect {unix_socket}")
    else:
        benchmark_command.extend(
            [
                "--port",
                f"{port}",
                "--server",
                f"{server}",
            ]
        )
    if password is not None:
        benchmark_command.extend(["--authenticate", password])
    if tls_enabled:
        benchmark_command.append("--tls")
        if tls_cert is not None and tls_cert != "":
            benchmark_command.extend(["--cert", tls_cert])
        if tls_key is not None and tls_key != "":
            benchmark_command.extend(["--key", tls_key])
        if tls_cacert is not None and tls_cacert != "":
            benchmark_command.extend(["--cacert", tls_cacert])
        if tls_skip_verify:
            benchmark_command.append("--tls-skip-verify")

    if resp_version:
        tool = clientconfig["tool"]
        if tool == "memtier_benchmark":
            if resp_version == "3":
                benchmark_command.extend(["--protocol", "resp{}".format(resp_version)])
        elif tool == "redis-benchmark":
            if resp_version == "3":
                benchmark_command.append("-3")

    if oss_cluster_api_enabled is True:
        benchmark_command.append("--cluster-mode")
    logging.info(f"Preparing the benchmark parameters. {benchmark_command}.")
    benchmark_command_str = " ".join(benchmark_command)
    if "arguments" in clientconfig:
        benchmark_command_str = benchmark_command_str + " " + clientconfig["arguments"]

    if "--command" in benchmark_command_str:
        arbitrary_command = True

    if override_test_runs > 1:
        benchmark_command_str = re.sub(
            "--run-count\\s\\d+",
            "--run-count={}".format(override_test_runs),
            benchmark_command_str,
        )
        benchmark_command_str = re.sub(
            "--run-count=\\d+",
            "--run-count={}".format(override_test_runs),
            benchmark_command_str,
        )
        benchmark_command_str = re.sub(
            '--run-count="\\d+"',
            "--run-count={}".format(override_test_runs),
            benchmark_command_str,
        )
        # short
        benchmark_command_str = re.sub(
            "-x\\s\\d+",
            "-x={}".format(override_test_runs),
            benchmark_command_str,
        )
        benchmark_command_str = re.sub(
            "-x=\\d+",
            "-x={}".format(override_test_runs),
            benchmark_command_str,
        )
        benchmark_command_str = re.sub(
            '-x="\\d+"',
            "-x={}".format(override_test_runs),
            benchmark_command_str,
        )
        if (
            len(
                re.findall(
                    "--run-count={}".format(override_test_runs),
                    benchmark_command_str,
                )
            )
            == 0
            and len(
                re.findall(
                    "-x={}".format(override_test_runs),
                    benchmark_command_str,
                )
            )
            == 0
        ):
            logging.info("adding --run-count option to benchmark run. ")
            benchmark_command_str = (
                benchmark_command_str
                + " "
                + "--run-count={}".format(override_test_runs)
            )

    if override_memtier_test_time > 0:
        benchmark_command_str = re.sub(
            "--test-time\\s\\d+",
            "--test-time={}".format(override_memtier_test_time),
            benchmark_command_str,
        )
        benchmark_command_str = re.sub(
            "--test-time=\\d+",
            "--test-time={}".format(override_memtier_test_time),
            benchmark_command_str,
        )
        benchmark_command_str = re.sub(
            '--test-time="\\d+"',
            "--test-time={}".format(override_memtier_test_time),
            benchmark_command_str,
        )

    return None, benchmark_command_str, arbitrary_command


def prepare_vector_db_benchmark_parameters(
    clientconfig,
    full_benchmark_path,
    port,
    server,
    password,
    local_benchmark_output_filename,
    oss_cluster_api_enabled=False,
    tls_enabled=False,
    tls_skip_verify=False,
    tls_cert=None,
    tls_key=None,
    tls_cacert=None,
    resp_version=None,
    override_test_time=0,
    unix_socket="",
    username=None,
):
    """
    Prepare vector-db-benchmark command parameters
    """
    arbitrary_command = False

    benchmark_command = [
        "/app/run.py",
        "--host",
        f"{server}",
    ]

    # Add port as environment variable (vector-db-benchmark uses env vars)
    env_vars = {}
    if port is not None:
        env_vars["REDIS_PORT"] = str(port)
    if password is not None:
        env_vars["REDIS_AUTH"] = password
    if username is not None:
        env_vars["REDIS_USER"] = username

    # Add engines parameter
    engines = clientconfig.get("engines", "vectorsets-fp32-default")
    benchmark_command.extend(["--engines", engines])

    # Add datasets parameter
    datasets = clientconfig.get("datasets", "random-100")
    benchmark_command.extend(["--datasets", datasets])

    # Add other optional parameters
    if "parallels" in clientconfig:
        benchmark_command.extend(["--parallels", str(clientconfig["parallels"])])

    if "queries" in clientconfig:
        benchmark_command.extend(["--queries", str(clientconfig["queries"])])

    if "timeout" in clientconfig:
        benchmark_command.extend(["--timeout", str(clientconfig["timeout"])])

    # Add custom arguments if specified
    if "arguments" in clientconfig:
        benchmark_command_str = (
            " ".join(benchmark_command) + " " + clientconfig["arguments"]
        )
    else:
        benchmark_command_str = " ".join(benchmark_command)

    return benchmark_command, benchmark_command_str, arbitrary_command, env_vars


def prepare_pubsub_sub_bench_parameters(
    clientconfig,
    full_benchmark_path,
    port,
    server,
    password,
    local_benchmark_output_filename,
    oss_cluster_api_enabled=False,
    tls_enabled=False,
    tls_skip_verify=False,
    tls_cert=None,
    tls_key=None,
    tls_cacert=None,
    resp_version=None,
    override_test_time=0,
    unix_socket="",
    username=None,
):
    """
    Prepare pubsub-sub-bench command parameters
    """
    arbitrary_command = False

    benchmark_command = [
        # full_benchmark_path,
        "-json-out-file",
        local_benchmark_output_filename,
    ]

    # Connection parameters
    if unix_socket != "":
        # pubsub-sub-bench doesn't support unix sockets directly
        # Fall back to host/port
        logging.warning(
            "pubsub-sub-bench doesn't support unix sockets, using host/port"
        )
        benchmark_command.extend(["-host", server, "-port", str(port)])
    else:
        benchmark_command.extend(["-host", server, "-port", str(port)])

    # Authentication
    if username and password:
        # ACL style authentication
        benchmark_command.extend(["-user", username, "-a", password])
    elif password:
        # Password-only authentication
        benchmark_command.extend(["-a", password])

    # TLS support (if the tool supports it in future versions)
    if tls_enabled:
        logging.warning("pubsub-sub-bench TLS support not implemented yet")

    # RESP version
    if resp_version:
        if resp_version == "3":
            benchmark_command.extend(["-resp", "3"])
        elif resp_version == "2":
            benchmark_command.extend(["-resp", "2"])

    # Cluster mode
    if oss_cluster_api_enabled:
        benchmark_command.append("-oss-cluster-api-distribute-subscribers")

    logging.info(f"Preparing pubsub-sub-bench parameters: {benchmark_command}")
    benchmark_command_str = " ".join(benchmark_command)

    # Append user-defined arguments from YAML
    user_arguments = ""
    if "arguments" in clientconfig:
        user_arguments = clientconfig["arguments"]

    # Test time override - handle after user arguments to avoid conflicts
    if override_test_time and override_test_time > 0:
        # Remove any existing -test-time from user arguments
        import re

        user_arguments = re.sub(r"-test-time\s+\d+", "", user_arguments)
        # Add our override test time
        benchmark_command_str = (
            benchmark_command_str + " -test-time " + str(override_test_time)
        )
        logging.info(f"Applied test-time override: {override_test_time}s")

    # Add cleaned user arguments
    if user_arguments.strip():
        benchmark_command_str = benchmark_command_str + " " + user_arguments.strip()

    return benchmark_command, benchmark_command_str, arbitrary_command


def process_self_contained_coordinator_stream(
    args,
    datasink_push_results_redistimeseries,
    docker_client,
    home,
    newTestInfo,
    datasink_conn,
    testsuite_spec_files,
    topologies_map,
    running_platform,
    profilers_enabled=False,
    profilers_list=[],
    tls_enabled=False,
    tls_skip_verify=False,
    tls_cert=None,
    tls_key=None,
    tls_cacert=None,
    client_aggregated_results_folder="",
    preserve_temporary_client_dirs=False,
    resp_version=None,
    override_memtier_test_time=0,
    used_memory_check_fail=False,
):
    def delete_temporary_files(
        temporary_dir_client, full_result_path, benchmark_tool_global
    ):
        if preserve_temporary_client_dirs is True:
            logging.info(f"Preserving temporary client dir {temporary_dir_client}")
        else:
            if benchmark_tool_global and "redis-benchmark" in benchmark_tool_global:
                if full_result_path is not None:
                    os.remove(full_result_path)
                    logging.info("Removing temporary JSON file")
            shutil.rmtree(temporary_dir_client, ignore_errors=True)
            logging.info(f"Removing temporary client dir {temporary_dir_client}")

    overall_result = True
    results_matrix = []
    total_test_suite_runs = 0
    dry_run_count = 0
    dry_run_tests = []  # Track test names for dry run output
    memory_results = []  # Track memory results for memory comparison mode
    loaded_datasets = (
        set()
    )  # Track datasets that have been loaded (for memory comparison mode)
    dry_run = args.dry_run
    memory_comparison_only = args.memory_comparison_only
    dry_run_include_preload = args.dry_run_include_preload
    defaults_filename = args.defaults_filename
    override_test_runs = args.override_test_runs
    get_defaults_result = get_defaults(defaults_filename)
    # Handle variable number of return values from get_defaults
    if len(get_defaults_result) >= 3:
        default_metrics = get_defaults_result[2]
    else:
        default_metrics = []
        logging.warning(
            "get_defaults returned fewer values than expected, using empty default_metrics"
        )

    # For memory comparison mode, analyze datasets before starting
    if memory_comparison_only:
        unique_datasets = set()
        total_tests_with_datasets = 0

        logging.info("Analyzing datasets for memory comparison mode...")
        for test_file in testsuite_spec_files:
            if defaults_filename in test_file:
                continue
            try:
                with open(test_file, "r") as stream:
                    benchmark_config = yaml.safe_load(stream)

                if "dbconfig" in benchmark_config:
                    # Skip load tests (keyspacelen = 0) in memory comparison mode
                    keyspacelen = (
                        benchmark_config["dbconfig"]
                        .get("check", {})
                        .get("keyspacelen", None)
                    )
                    if keyspacelen is not None and keyspacelen == 0:
                        logging.debug(f"Skipping load test {test_file} (keyspacelen=0)")
                        continue

                    dataset_name = benchmark_config["dbconfig"].get("dataset_name")
                    if dataset_name:
                        unique_datasets.add(dataset_name)
                        total_tests_with_datasets += 1

            except Exception as e:
                logging.warning(f"Error analyzing {test_file}: {e}")

        logging.info(f"Memory comparison mode analysis:")
        logging.info(f"  Total tests with datasets: {total_tests_with_datasets}")
        logging.info(f"  Unique datasets to load: {len(unique_datasets)}")
        logging.info(
            f"  Dataset ingestion savings: {total_tests_with_datasets - len(unique_datasets)} skipped loads"
        )
        logging.info(
            f"  Load tests skipped: Tests with keyspacelen=0 are automatically excluded"
        )

        if len(unique_datasets) > 0:
            logging.info(f"  Unique datasets: {', '.join(sorted(unique_datasets))}")

    for test_file in tqdm.tqdm(testsuite_spec_files):
        # Check if user requested exit via Ctrl+C
        if _exit_requested:
            logging.info("Exit requested by user. Stopping test execution.")
            break

        if defaults_filename in test_file:
            continue
        client_containers = []

        with open(test_file, "r") as stream:
            _, benchmark_config, test_name = get_final_benchmark_config(
                None, None, stream, ""
            )

            # Use override topology if provided, otherwise use all topologies from config
            if hasattr(args, "override_topology") and args.override_topology:
                benchmark_topologies = [args.override_topology]
                logging.info(f"Using override topology: {args.override_topology}")
            else:
                benchmark_topologies = benchmark_config["redis-topologies"]
                logging.info(
                    f"Running for a total of {len(benchmark_topologies)} topologies: {benchmark_topologies}"
                )

            # Check if user requested exit via Ctrl+C
            if _exit_requested:
                logging.info(f"Exit requested by user. Skipping test {test_name}.")
                break

            # Filter by command regex if specified
            if hasattr(args, "commands_regex") and args.commands_regex != ".*":
                if "tested-commands" in benchmark_config:
                    tested_commands = benchmark_config["tested-commands"]
                    command_regex_compiled = re.compile(
                        args.commands_regex, re.IGNORECASE
                    )
                    command_match = False
                    for command in tested_commands:
                        if re.search(command_regex_compiled, command):
                            command_match = True
                            logging.info(
                                f"Including test {test_name} (matches command: {command})"
                            )
                            break
                    if not command_match:
                        logging.info(
                            f"Skipping test {test_name} (commands: {tested_commands} do not match regex: {args.commands_regex})"
                        )
                        continue
                else:
                    logging.warning(
                        f"Test {test_name} does not contain 'tested-commands' property. Cannot filter by commands."
                    )

            if tls_enabled:
                test_name = test_name + "-tls"
                logging.info(
                    "Given that TLS is enabled, appending -tls to the testname: {}.".format(
                        test_name
                    )
                )

            for topology_spec_name in benchmark_topologies:
                test_result = False
                benchmark_tool_global = ""
                full_result_path = None
                try:
                    current_cpu_pos = args.cpuset_start_pos
                    temporary_dir_client = tempfile.mkdtemp(dir=home)

                    # These will be updated after auto-detection
                    tf_github_org = args.github_org
                    tf_github_repo = args.github_repo
                    tf_triggering_env = args.platform_name
                    setup_type = args.setup_type
                    priority_upper_limit = args.tests_priority_upper_limit
                    priority_lower_limit = args.tests_priority_lower_limit
                    git_hash = "NA"
                    git_version = args.github_version
                    build_variant_name = "NA"
                    git_branch = None

                    # Parse URI if provided, otherwise use individual arguments
                    if hasattr(args, "uri") and args.uri:
                        uri_params = parse_redis_uri(args.uri)
                        port = uri_params.get("port", args.db_server_port)
                        host = uri_params.get("host", args.db_server_host)
                        password = uri_params.get("password", args.db_server_password)
                        # Override TLS setting from URI if specified
                        if "tls_enabled" in uri_params:
                            tls_enabled = uri_params["tls_enabled"]
                            if tls_enabled:
                                test_name = test_name + "-tls"
                                logging.info(
                                    "TLS enabled via URI. Appending -tls to testname."
                                )
                        # Note: username and db are handled by redis-py automatically when using URI
                        logging.info(
                            f"Using connection parameters from URI: host={host}, port={port}, tls={tls_enabled}"
                        )
                    else:
                        port = args.db_server_port
                        host = args.db_server_host
                        password = args.db_server_password
                        logging.info(
                            f"Using individual connection arguments: host={host}, port={port}"
                        )

                    unix_socket = args.unix_socket
                    oss_cluster_api_enabled = args.cluster_mode
                    ssl_cert_reqs = "required"
                    if tls_skip_verify:
                        ssl_cert_reqs = None

                    # Create Redis connection - use URI if provided, otherwise use individual parameters
                    if hasattr(args, "uri") and args.uri:
                        # Use URI connection (redis-py handles URI parsing automatically)
                        redis_params = {}

                        # Only add SSL parameters if TLS is enabled
                        if tls_enabled:
                            redis_params["ssl_cert_reqs"] = ssl_cert_reqs
                            redis_params["ssl_check_hostname"] = False
                            if tls_key is not None and tls_key != "":
                                redis_params["ssl_keyfile"] = tls_key
                            if tls_cert is not None and tls_cert != "":
                                redis_params["ssl_certfile"] = tls_cert
                            if tls_cacert is not None and tls_cacert != "":
                                redis_params["ssl_ca_certs"] = tls_cacert

                        r = redis.StrictRedis.from_url(args.uri, **redis_params)
                        logging.info(f"Connected to Redis using URI: {args.uri}")
                    else:
                        # Use individual connection parameters
                        redis_params = {
                            "host": host,
                            "port": port,
                            "password": password,
                            "ssl": tls_enabled,
                            "ssl_cert_reqs": ssl_cert_reqs,
                            "ssl_check_hostname": False,
                        }

                        # Only add SSL certificate parameters if they are provided
                        if tls_enabled:
                            if tls_key is not None and tls_key != "":
                                redis_params["ssl_keyfile"] = tls_key
                            if tls_cert is not None and tls_cert != "":
                                redis_params["ssl_certfile"] = tls_cert
                            if tls_cacert is not None and tls_cacert != "":
                                redis_params["ssl_ca_certs"] = tls_cacert

                        r = redis.StrictRedis(**redis_params)
                        logging.info(
                            f"Connected to Redis using individual parameters: {host}:{port}"
                        )
                    setup_name = topology_spec_name
                    r.ping()

                    # Auto-detect server information if not explicitly provided
                    from redis_benchmarks_specification.__runner__.remote_profiling import (
                        extract_server_info_for_args,
                        extract_server_metadata_for_timeseries,
                    )

                    detected_info = extract_server_info_for_args(r)
                    server_metadata = extract_server_metadata_for_timeseries(r)

                    # Use detected values if arguments weren't explicitly provided
                    github_org = args.github_org
                    github_repo = args.github_repo

                    # Auto-detect github_org if it's the default value
                    if (
                        args.github_org == "redis"
                        and detected_info["github_org"] != "redis"
                    ):
                        github_org = detected_info["github_org"]
                        logging.info(f"Auto-detected github_org: {github_org}")

                    # Auto-detect github_repo if it's the default value
                    if (
                        args.github_repo == "redis"
                        and detected_info["github_repo"] != "redis"
                    ):
                        github_repo = detected_info["github_repo"]
                        logging.info(f"Auto-detected github_repo: {github_repo}")

                    # Auto-detect version if it's the default value
                    if (
                        args.github_version == "NA"
                        and detected_info["github_version"] != "unknown"
                    ):
                        git_version = detected_info["github_version"]
                        logging.info(f"Auto-detected github_version: {git_version}")

                    # Auto-detect git hash if it's the default value
                    if (git_hash is None or git_hash == "NA") and detected_info[
                        "github_hash"
                    ] != "unknown":
                        git_hash = detected_info["github_hash"]
                        logging.info(f"Auto-detected git_hash: {git_hash}")

                    # Update tf_github_org and tf_github_repo with detected values
                    tf_github_org = github_org
                    tf_github_repo = github_repo
                    redis_conns = [r]

                    if oss_cluster_api_enabled:
                        redis_conns = []
                        logging.info("updating redis connections from cluster slots")
                        slots = r.cluster("slots")
                        for slot in slots:
                            # Master for slot range represented as nested networking information starts at pos 2
                            # example: [0, 5460, [b'127.0.0.1', 30001, b'eccd21c2e7e9b7820434080d2e394cb8f2a7eff2', []]]
                            slot_network_info = slot[2]
                            prefered_endpoint = slot_network_info[0]
                            prefered_port = slot_network_info[1]
                            # Build shard connection parameters
                            shard_params = {
                                "host": prefered_endpoint,
                                "port": prefered_port,
                                "password": password,
                                "ssl": tls_enabled,
                                "ssl_cert_reqs": ssl_cert_reqs,
                                "ssl_check_hostname": False,
                            }

                            # Only add SSL certificate parameters if they are provided
                            if tls_enabled:
                                if tls_key is not None and tls_key != "":
                                    shard_params["ssl_keyfile"] = tls_key
                                if tls_cert is not None and tls_cert != "":
                                    shard_params["ssl_certfile"] = tls_cert
                                if tls_cacert is not None and tls_cacert != "":
                                    shard_params["ssl_ca_certs"] = tls_cacert

                            shard_conn = redis.StrictRedis(**shard_params)
                            redis_conns.append(shard_conn)
                        logging.info(
                            "There are a total of {} shards".format(len(redis_conns))
                        )
                        setup_name = "oss-cluster"

                    redis_pids = []
                    for conn in redis_conns:
                        redis_info = conn.info()
                        redis_pid = redis_info.get("process_id")
                        if redis_pid is not None:
                            redis_pids.append(redis_pid)
                        else:
                            logging.warning(
                                "Redis process_id not found in INFO command, skipping PID collection for this connection"
                            )

                    # Check if all tested commands are supported by this Redis instance
                    supported_commands = get_supported_redis_commands(redis_conns)
                    commands_supported, unsupported_commands = (
                        check_test_command_support(benchmark_config, supported_commands)
                    )

                    if not commands_supported:
                        logging.warning(
                            f"Skipping test {test_name} due to unsupported commands: {unsupported_commands}"
                        )
                        delete_temporary_files(
                            temporary_dir_client=temporary_dir_client,
                            full_result_path=None,
                            benchmark_tool_global=benchmark_tool_global,
                        )
                        continue

                    github_actor = f"{tf_triggering_env}-{running_platform}"
                    dso = "redis-server"
                    profilers_artifacts_matrix = []

                    collection_summary_str = ""
                    if profilers_enabled:
                        collection_summary_str = local_profilers_platform_checks(
                            dso,
                            github_actor,
                            git_branch,
                            tf_github_repo,
                            git_hash,
                        )
                        logging.info(
                            "Using the following collection summary string for profiler description: {}".format(
                                collection_summary_str
                            )
                        )

                    ceil_client_cpu_limit = extract_client_cpu_limit(benchmark_config)
                    client_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(
                        ceil_client_cpu_limit, current_cpu_pos
                    )
                    if args.flushall_on_every_test_start:
                        logging.info("Sending FLUSHALL to the DB")
                        for conn in redis_conns:
                            conn.flushall()

                        # Send MEMORY PURGE after FLUSHALL for memory comparison mode
                        if memory_comparison_only:
                            try:
                                logging.info(
                                    "Sending MEMORY PURGE after FLUSHALL at test start"
                                )
                                for conn in redis_conns:
                                    conn.execute_command("MEMORY", "PURGE")
                            except Exception as e:
                                logging.warning(
                                    f"MEMORY PURGE failed after FLUSHALL at test start: {e}"
                                )

                    benchmark_required_memory = get_benchmark_required_memory(
                        benchmark_config
                    )
                    maxmemory = 0
                    if args.maxmemory > 0:
                        maxmemory = args.maxmemory
                    else:
                        for conn in redis_conns:
                            maxmemory = maxmemory + get_maxmemory(conn)

                    # Only perform memory check if we have valid maxmemory information
                    if maxmemory > 0 and benchmark_required_memory > maxmemory:
                        logging.warning(
                            "Skipping test {} given maxmemory of server is bellow the benchmark required memory: {} < {}".format(
                                test_name, maxmemory, benchmark_required_memory
                            )
                        )
                        delete_temporary_files(
                            temporary_dir_client=temporary_dir_client,
                            full_result_path=None,
                            benchmark_tool_global=benchmark_tool_global,
                        )
                        continue
                    elif maxmemory == 0 and benchmark_required_memory > 0:
                        logging.warning(
                            "Cannot enforce memory checks for test {} - maxmemory information unavailable. Proceeding with test.".format(
                                test_name
                            )
                        )

                    reset_commandstats(redis_conns)

                    client_mnt_point = "/mnt/client/"
                    benchmark_tool_workdir = client_mnt_point

                    metadata = {}
                    # Add server metadata from Redis INFO SERVER
                    metadata.update(server_metadata)

                    # Add connection mode metadata
                    if tls_enabled:
                        metadata["conn_mode"] = "TLS"
                        metadata["tls"] = "true"
                    else:
                        metadata["conn_mode"] = "PLAINTEXT"

                    # Add deployment metadata
                    metadata["deployment_type"] = args.deployment_type
                    metadata["deployment_name"] = args.deployment_name

                    # Add core count if specified
                    if args.core_count is not None:
                        metadata["core_count"] = str(args.core_count)

                    test_tls_cacert = None
                    test_tls_cert = None
                    test_tls_key = None
                    if tls_enabled:
                        if tls_cert is not None and tls_cert != "":
                            _, test_tls_cert = cp_to_workdir(
                                temporary_dir_client, tls_cert
                            )
                        if tls_cacert is not None and tls_cacert != "":
                            _, test_tls_cacert = cp_to_workdir(
                                temporary_dir_client, tls_cacert
                            )
                        if tls_key is not None and tls_key != "":
                            _, test_tls_key = cp_to_workdir(
                                temporary_dir_client, tls_key
                            )
                    priority = None
                    if "priority" in benchmark_config:
                        priority = benchmark_config["priority"]

                        if priority is not None:
                            if priority > priority_upper_limit:
                                logging.warning(
                                    "Skipping test {} giving the priority limit ({}) is above the priority value ({})".format(
                                        test_name, priority_upper_limit, priority
                                    )
                                )
                                delete_temporary_files(
                                    temporary_dir_client=temporary_dir_client,
                                    full_result_path=None,
                                    benchmark_tool_global=benchmark_tool_global,
                                )
                                continue
                            if priority < priority_lower_limit:
                                logging.warning(
                                    "Skipping test {} giving the priority limit ({}) is bellow the priority value ({})".format(
                                        test_name, priority_lower_limit, priority
                                    )
                                )
                                delete_temporary_files(
                                    temporary_dir_client=temporary_dir_client,
                                    full_result_path=None,
                                    benchmark_tool_global=benchmark_tool_global,
                                )
                                continue
                            logging.debug(
                                "Test {} priority ({}) is within the priority limit [{},{}]".format(
                                    test_name,
                                    priority,
                                    priority_lower_limit,
                                    priority_upper_limit,
                                )
                            )
                    if "dbconfig" in benchmark_config:
                        if "dataset" in benchmark_config["dbconfig"]:
                            if args.run_tests_with_dataset is False:
                                logging.warning(
                                    "Skipping test {} giving it implies dataset preload".format(
                                        test_name
                                    )
                                )
                                delete_temporary_files(
                                    temporary_dir_client=temporary_dir_client,
                                    full_result_path=None,
                                    benchmark_tool_global=benchmark_tool_global,
                                )
                                continue
                        if "preload_tool" in benchmark_config["dbconfig"]:
                            if args.skip_tests_with_preload_via_tool is True:
                                logging.warning(
                                    "Skipping test {} giving it implies dataset preload via tool".format(
                                        test_name
                                    )
                                )
                                delete_temporary_files(
                                    temporary_dir_client=temporary_dir_client,
                                    full_result_path=None,
                                    benchmark_tool_global=benchmark_tool_global,
                                )
                                continue

                        # Check if we should skip tests without dataset
                        has_dataset = "preload_tool" in benchmark_config.get(
                            "dbconfig", {}
                        )
                        if args.skip_tests_without_dataset is True and not has_dataset:
                            logging.warning(
                                "Skipping test {} as it does not contain a dataset".format(
                                    test_name
                                )
                            )
                            delete_temporary_files(
                                temporary_dir_client=temporary_dir_client,
                                full_result_path=None,
                                benchmark_tool_global=benchmark_tool_global,
                            )
                            continue

                        # For memory comparison mode, only run tests with dbconfig
                        if (
                            memory_comparison_only
                            and "dbconfig" not in benchmark_config
                        ):
                            logging.warning(
                                "Skipping test {} in memory comparison mode as it does not contain dbconfig".format(
                                    test_name
                                )
                            )
                            delete_temporary_files(
                                temporary_dir_client=temporary_dir_client,
                                full_result_path=None,
                                benchmark_tool_global=benchmark_tool_global,
                            )
                            continue

                    if dry_run is True:
                        dry_run_count = dry_run_count + 1
                        dry_run_tests.append(test_name)
                        delete_temporary_files(
                            temporary_dir_client=temporary_dir_client,
                            full_result_path=None,
                            benchmark_tool_global=benchmark_tool_global,
                        )
                        continue
                    if "dbconfig" in benchmark_config:
                        if "preload_tool" in benchmark_config["dbconfig"]:
                            # Check if this dataset has already been loaded (for memory comparison mode)
                            dataset_name = benchmark_config["dbconfig"].get(
                                "dataset_name"
                            )
                            skip_preload = False

                            if memory_comparison_only and dataset_name:
                                if dataset_name in loaded_datasets:
                                    logging.info(
                                        f"Skipping preload for dataset '{dataset_name}' - already loaded"
                                    )
                                    skip_preload = True
                                    continue
                                else:
                                    logging.info(
                                        f"Loading dataset '{dataset_name}' for the first time"
                                    )
                                    loaded_datasets.add(dataset_name)

                            if not skip_preload:
                                # Get timeout buffer for preload
                                buffer_timeout = getattr(
                                    args,
                                    "timeout_buffer",
                                    getattr(args, "container_timeout_buffer", 60),
                                )

                                res = data_prepopulation_step(
                                    benchmark_config,
                                    benchmark_tool_workdir,
                                    client_cpuset_cpus,
                                    docker_client,
                                    git_hash,
                                    port,
                                    temporary_dir_client,
                                    test_name,
                                    host,
                                    tls_enabled,
                                    tls_skip_verify,
                                    test_tls_cert,
                                    test_tls_key,
                                    test_tls_cacert,
                                    resp_version,
                                    args.benchmark_local_install,
                                    password,
                                    oss_cluster_api_enabled,
                                    unix_socket,
                                    buffer_timeout,
                                    args,
                                )
                                if res is False:
                                    logging.warning(
                                        "Skipping this test given preload result was false"
                                    )
                                    delete_temporary_files(
                                        temporary_dir_client=temporary_dir_client,
                                        full_result_path=None,
                                        benchmark_tool_global=benchmark_tool_global,
                                    )
                                    continue
                    # Send MEMORY PURGE before preload for memory comparison mode (if FLUSHALL wasn't already done)
                    if memory_comparison_only and not args.flushall_on_every_test_start:
                        try:
                            logging.info(
                                "Sending MEMORY PURGE before preload for memory comparison mode"
                            )
                            for conn in redis_conns:
                                conn.execute_command("MEMORY", "PURGE")
                        except Exception as e:
                            logging.warning(f"MEMORY PURGE failed before preload: {e}")

                    execute_init_commands(
                        benchmark_config, r, dbconfig_keyname="dbconfig"
                    )

                    used_memory_check(
                        test_name,
                        benchmark_required_memory,
                        redis_conns,
                        "start of benchmark",
                        used_memory_check_fail,
                    )

                    logging.info("Checking if there is a keyspace check being enforced")
                    dbconfig_keyspacelen_check(
                        benchmark_config,
                        redis_conns,
                    )

                    # For memory comparison mode, collect memory stats after preload and skip client benchmark
                    if memory_comparison_only:
                        # Initialize timing variables for memory comparison mode
                        (
                            start_time,
                            start_time_ms,
                            start_time_str,
                        ) = get_start_time_vars()
                        dataset_load_duration_seconds = (
                            0  # No dataset loading time for memory comparison
                        )

                        # Skip load tests (keyspacelen = 0) in memory comparison mode
                        keyspacelen = (
                            benchmark_config.get("dbconfig", {})
                            .get("check", {})
                            .get("keyspacelen", None)
                        )
                        if keyspacelen is not None and keyspacelen == 0:
                            logging.info(
                                f"Skipping load test {test_name} in memory comparison mode (keyspacelen=0)"
                            )
                            delete_temporary_files(
                                temporary_dir_client=temporary_dir_client,
                                full_result_path=None,
                                benchmark_tool_global=benchmark_tool_global,
                            )
                            continue

                        # Handle dry run for memory comparison mode
                        if dry_run:
                            dry_run_count = dry_run_count + 1
                            dry_run_tests.append(test_name)
                            logging.info(
                                f"[DRY RUN] Would collect memory stats for test {test_name}"
                            )

                            # Add dataset info to dry run output
                            dataset_name = benchmark_config.get("dbconfig", {}).get(
                                "dataset_name"
                            )
                            if dataset_name:
                                logging.info(f"[DRY RUN]   Dataset: {dataset_name}")

                            delete_temporary_files(
                                temporary_dir_client=temporary_dir_client,
                                full_result_path=None,
                                benchmark_tool_global=benchmark_tool_global,
                            )
                            continue

                        logging.info(f"Collecting memory stats for test {test_name}")
                        try:
                            # Use raw command to avoid parsing issues with some Redis versions
                            memory_stats_raw = r.execute_command("MEMORY", "STATS")
                            # Convert list response to dict
                            memory_stats = {}
                            for i in range(0, len(memory_stats_raw), 2):
                                key = (
                                    memory_stats_raw[i].decode()
                                    if isinstance(memory_stats_raw[i], bytes)
                                    else str(memory_stats_raw[i])
                                )
                                value = memory_stats_raw[i + 1]
                                if isinstance(value, bytes):
                                    try:
                                        value = float(value.decode())
                                    except ValueError:
                                        value = value.decode()
                                memory_stats[key] = value
                        except Exception as e:
                            logging.error(f"Failed to collect memory stats: {e}")
                            # Fallback to basic memory info
                            info = r.info("memory")
                            memory_stats = {
                                "total.allocated": info.get("used_memory", 0),
                                "dataset.bytes": info.get("used_memory_dataset", 0),
                                "keys.count": r.dbsize(),
                                "keys.bytes-per-key": 0,
                                "dataset.percentage": 0,
                                "overhead.total": 0,
                                "fragmentation": info.get(
                                    "mem_fragmentation_ratio", 1.0
                                ),
                                "fragmentation.bytes": 0,
                                "allocator.allocated": info.get("used_memory", 0),
                                "allocator.resident": info.get("used_memory_rss", 0),
                                "allocator-fragmentation.ratio": 1.0,
                            }

                        # Detect object encoding by scanning 1% of the dataset
                        object_encoding_info = detect_object_encoding(
                            r, benchmark_config.get("dbconfig", {})
                        )
                        logging.info(
                            f"Object encoding detection: {object_encoding_info.get('encoding', 'unknown')} "
                            f"({object_encoding_info.get('confidence', 0)*100:.1f}% confidence)"
                        )

                        # Extract key memory metrics
                        memory_result = {
                            "test_name": test_name,
                            "total_allocated": memory_stats.get("total.allocated", 0),
                            "dataset_bytes": memory_stats.get("dataset.bytes", 0),
                            "keys_count": memory_stats.get("keys.count", 0),
                            "keys_bytes_per_key": memory_stats.get(
                                "keys.bytes-per-key", 0
                            ),
                            "dataset_percentage": memory_stats.get(
                                "dataset.percentage", 0
                            ),
                            "overhead_total": memory_stats.get("overhead.total", 0),
                            "fragmentation": memory_stats.get("fragmentation", 0),
                            "fragmentation_bytes": memory_stats.get(
                                "fragmentation.bytes", 0
                            ),
                            "allocator_allocated": memory_stats.get(
                                "allocator.allocated", 0
                            ),
                            "allocator_resident": memory_stats.get(
                                "allocator.resident", 0
                            ),
                            "allocator_fragmentation_ratio": memory_stats.get(
                                "allocator-fragmentation.ratio", 0
                            ),
                            # Object encoding information
                            "object_encoding": object_encoding_info.get(
                                "encoding", "unknown"
                            ),
                            "encoding_confidence": object_encoding_info.get(
                                "confidence", 0.0
                            ),
                            "encoding_sample_size": object_encoding_info.get(
                                "sample_size", 0
                            ),
                            "encoding_distribution": object_encoding_info.get(
                                "encoding_distribution", {}
                            ),
                            "encoding_is_complete_scan": object_encoding_info.get(
                                "is_complete_scan", False
                            ),
                        }
                        memory_results.append(memory_result)

                        # Push memory metrics to datasink
                        if datasink_push_results_redistimeseries:
                            memory_metrics_dict = {
                                "memory.total_allocated": memory_result[
                                    "total_allocated"
                                ],
                                "memory.dataset_bytes": memory_result["dataset_bytes"],
                                "memory.keys_count": memory_result["keys_count"],
                                "memory.keys_bytes_per_key": memory_result[
                                    "keys_bytes_per_key"
                                ],
                                "memory.dataset_percentage": memory_result[
                                    "dataset_percentage"
                                ],
                                "memory.overhead_total": memory_result[
                                    "overhead_total"
                                ],
                                "memory.fragmentation": memory_result["fragmentation"],
                                "memory.fragmentation_bytes": memory_result[
                                    "fragmentation_bytes"
                                ],
                                "memory.allocator_allocated": memory_result[
                                    "allocator_allocated"
                                ],
                                "memory.allocator_resident": memory_result[
                                    "allocator_resident"
                                ],
                                "memory.allocator_fragmentation_ratio": memory_result[
                                    "allocator_fragmentation_ratio"
                                ],
                                "memory.encoding_confidence": memory_result[
                                    "encoding_confidence"
                                ],
                                "memory.encoding_sample_size": memory_result[
                                    "encoding_sample_size"
                                ],
                            }

                            # Add object encoding to metadata
                            metadata["object_encoding"] = memory_result[
                                "object_encoding"
                            ]
                            metadata["encoding_confidence"] = (
                                f"{memory_result['encoding_confidence']:.3f}"
                            )
                            metadata["encoding_sample_size"] = str(
                                memory_result["encoding_sample_size"]
                            )
                            metadata["encoding_scan_type"] = (
                                "complete"
                                if memory_result.get("encoding_is_complete_scan", False)
                                else "sample"
                            )

                            # Add encoding distribution to metadata if multiple encodings found
                            if len(memory_result["encoding_distribution"]) > 1:
                                for enc, percentage in memory_result[
                                    "encoding_distribution"
                                ].items():
                                    metadata[f"encoding_dist_{enc}"] = (
                                        f"{percentage:.1f}%"
                                    )

                            # Set datapoint_time_ms for memory comparison mode
                            datapoint_time_ms = start_time_ms
                            # 7 days from now
                            expire_redis_metrics_ms = 7 * 24 * 60 * 60 * 1000
                            metadata["metric-type"] = "memory-stats"

                            # Debug: Check git_hash value and memory metrics before export
                            logging.info(
                                f"DEBUG: About to export memory metrics with git_hash='{git_hash}', type={type(git_hash)}"
                            )
                            logging.info(
                                f"DEBUG: memory_metrics_dict has {len(memory_metrics_dict)} items: {list(memory_metrics_dict.keys())}"
                            )
                            logging.info(
                                f"DEBUG: Sample values: {dict(list(memory_metrics_dict.items())[:3])}"
                            )
                            export_redis_metrics(
                                git_version,
                                datapoint_time_ms,
                                memory_metrics_dict,
                                datasink_conn,
                                setup_name,
                                setup_type,
                                test_name,
                                git_branch,
                                tf_github_org,
                                tf_github_repo,
                                tf_triggering_env,
                                metadata,
                                expire_redis_metrics_ms,
                                git_hash,
                                running_platform,
                            )

                            exporter_datasink_common(
                                benchmark_config,
                                0,  # benchmark_duration_seconds = 0 for memory only
                                build_variant_name,
                                datapoint_time_ms,
                                dataset_load_duration_seconds,
                                datasink_conn,
                                datasink_push_results_redistimeseries,
                                git_branch,
                                git_version,
                                metadata,
                                redis_conns,
                                memory_metrics_dict,
                                running_platform,
                                args.deployment_name,
                                args.deployment_type,
                                test_name,
                                tf_github_org,
                                tf_github_repo,
                                tf_triggering_env,
                                topology_spec_name,
                                default_metrics,
                                git_hash,
                                False,
                                True,
                            )

                        # Send MEMORY PURGE after memory comparison (if FLUSHALL at test end is not enabled)
                        if not args.flushall_on_every_test_end:
                            try:
                                logging.info(
                                    "Sending MEMORY PURGE after memory comparison"
                                )
                                for conn in redis_conns:
                                    conn.execute_command("MEMORY", "PURGE")
                            except Exception as e:
                                logging.warning(
                                    f"MEMORY PURGE failed after memory comparison: {e}"
                                )

                        logging.info(
                            f"Memory comparison completed for test {test_name}"
                        )
                        delete_temporary_files(
                            temporary_dir_client=temporary_dir_client,
                            full_result_path=None,
                            benchmark_tool_global=benchmark_tool_global,
                        )
                        continue

                    if dry_run_include_preload is True:
                        dry_run_count = dry_run_count + 1
                        dry_run_tests.append(test_name)
                        delete_temporary_files(
                            temporary_dir_client=temporary_dir_client,
                            full_result_path=None,
                            benchmark_tool_global=benchmark_tool_global,
                        )
                        continue

                    benchmark_tool = extract_client_tool(benchmark_config)
                    benchmark_tool_global = benchmark_tool
                    # backwards compatible
                    if benchmark_tool is None:
                        benchmark_tool = "redis-benchmark"

                    # Set benchmark path based on local install option
                    if (
                        args.benchmark_local_install
                        and "memtier_benchmark" in benchmark_tool
                    ):
                        full_benchmark_path = getattr(
                            args, "memtier_bin_path", "memtier_benchmark"
                        )
                    else:
                        full_benchmark_path = f"/usr/local/bin/{benchmark_tool}"

                    # setup the benchmark
                    (
                        start_time,
                        start_time_ms,
                        start_time_str,
                    ) = get_start_time_vars()
                    local_benchmark_output_filename = get_local_run_full_filename(
                        start_time_str,
                        git_hash,
                        test_name,
                        setup_name,
                    )
                    logging.info(
                        "Will store benchmark json output to local file {}".format(
                            local_benchmark_output_filename
                        )
                    )
                    arbitrary_command = False

                    # Check if we have multiple client configurations
                    client_configs = extract_client_configs(benchmark_config)
                    is_multiple_clients = len(client_configs) > 1

                    if is_multiple_clients:
                        logging.info(
                            f"Running test with {len(client_configs)} client configurations"
                        )
                    else:
                        # Legacy single client mode - prepare benchmark parameters
                        client_container_image = extract_client_container_image(
                            benchmark_config
                        )
                        benchmark_tool = extract_client_tool(benchmark_config)

                        # Prepare benchmark command for single client
                        if "memtier_benchmark" in benchmark_tool:
                            (
                                _,
                                benchmark_command_str,
                                arbitrary_command,
                            ) = prepare_memtier_benchmark_parameters(
                                benchmark_config["clientconfig"],
                                full_benchmark_path,
                                port,
                                host,
                                password,
                                local_benchmark_output_filename,
                                oss_cluster_api_enabled,
                                tls_enabled,
                                tls_skip_verify,
                                test_tls_cert,
                                test_tls_key,
                                test_tls_cacert,
                                resp_version,
                                override_memtier_test_time,
                                override_test_runs,
                                unix_socket,
                            )
                        elif "pubsub-sub-bench" in benchmark_tool:
                            (
                                _,
                                benchmark_command_str,
                                arbitrary_command,
                            ) = prepare_pubsub_sub_bench_parameters(
                                benchmark_config["clientconfig"],
                                full_benchmark_path,
                                port,
                                host,
                                password,
                                local_benchmark_output_filename,
                                oss_cluster_api_enabled,
                                tls_enabled,
                                tls_skip_verify,
                                test_tls_cert,
                                test_tls_key,
                                test_tls_cacert,
                                resp_version,
                                override_memtier_test_time,
                                unix_socket,
                                None,  # username
                            )
                        elif "vector-db-benchmark" in benchmark_tool:
                            (
                                _,
                                benchmark_command_str,
                                arbitrary_command,
                                env_vars,
                            ) = prepare_vector_db_benchmark_parameters(
                                benchmark_config["clientconfig"],
                                full_benchmark_path,
                                port,
                                host,
                                password,
                                local_benchmark_output_filename,
                                oss_cluster_api_enabled,
                                tls_enabled,
                                tls_skip_verify,
                                test_tls_cert,
                                test_tls_key,
                                test_tls_cacert,
                                resp_version,
                                override_memtier_test_time,
                                unix_socket,
                                None,  # username
                            )
                        else:
                            # prepare the benchmark command for other tools
                            (
                                benchmark_command,
                                benchmark_command_str,
                            ) = prepare_benchmark_parameters(
                                benchmark_config,
                                full_benchmark_path,
                                port,
                                host,
                                local_benchmark_output_filename,
                                False,
                                benchmark_tool_workdir,
                                False,
                            )
                    profiler_call_graph_mode = "dwarf"
                    profiler_frequency = 99

                    # start the profile
                    (
                        profiler_name,
                        profilers_map,
                    ) = profilers_start_if_required(
                        profilers_enabled,
                        profilers_list,
                        redis_pids,
                        setup_name,
                        start_time_str,
                        test_name,
                        profiler_frequency,
                        profiler_call_graph_mode,
                    )

                    # start remote profiling if enabled
                    remote_profiler = None
                    if args.enable_remote_profiling:
                        try:
                            remote_profiler = RemoteProfiler(
                                args.remote_profile_host,
                                args.remote_profile_port,
                                args.remote_profile_output_dir,
                                args.remote_profile_username,
                                args.remote_profile_password,
                            )

                            # Extract expected benchmark duration
                            expected_duration = extract_expected_benchmark_duration(
                                benchmark_command_str, override_memtier_test_time
                            )

                            # Start remote profiling
                            profiling_started = remote_profiler.start_profiling(
                                redis_conns[0] if redis_conns else None,
                                test_name,
                                expected_duration,
                            )

                            if profiling_started:
                                logging.info(
                                    f"Started remote profiling for test: {test_name}"
                                )
                            else:
                                logging.warning(
                                    f"Failed to start remote profiling for test: {test_name}"
                                )
                                remote_profiler = None

                        except Exception as e:
                            logging.error(f"Error starting remote profiling: {e}")
                            remote_profiler = None

                    # run the benchmark
                    benchmark_start_time = datetime.datetime.now()

                    if is_multiple_clients:
                        # Run multiple client configurations
                        logging.info(
                            "Running multiple client configurations simultaneously"
                        )
                        client_container_stdout, client_results = run_multiple_clients(
                            benchmark_config,
                            docker_client,
                            temporary_dir_client,
                            client_mnt_point,
                            benchmark_tool_workdir,
                            client_cpuset_cpus,
                            port,
                            host,
                            password,
                            oss_cluster_api_enabled,
                            tls_enabled,
                            tls_skip_verify,
                            test_tls_cert,
                            test_tls_key,
                            test_tls_cacert,
                            resp_version,
                            override_memtier_test_time,
                            override_test_runs,
                            unix_socket,
                            args,
                        )
                        logging.info(
                            f"Completed {len(client_results)} client configurations"
                        )
                    else:
                        # Legacy single client execution
                        if args.benchmark_local_install:
                            logging.info("Running memtier benchmark outside of docker")
                            benchmark_command_str = (
                                "taskset -c "
                                + client_cpuset_cpus
                                + " "
                                + benchmark_command_str
                            )

                            # Calculate timeout for local process
                            buffer_timeout = getattr(
                                args,
                                "timeout_buffer",
                                getattr(args, "container_timeout_buffer", 60),
                            )
                            process_timeout = calculate_process_timeout(
                                benchmark_command_str, buffer_timeout
                            )

                            # Run with timeout
                            success, client_container_stdout, stderr = (
                                run_local_command_with_timeout(
                                    benchmark_command_str,
                                    process_timeout,
                                    "memtier benchmark",
                                )
                            )

                            if not success:
                                logging.error(f"Memtier benchmark failed: {stderr}")
                                # Clean up database after failure (timeout or error)
                                if (
                                    args.flushall_on_every_test_end
                                    or args.flushall_on_every_test_start
                                ):
                                    logging.warning(
                                        "Benchmark failed - cleaning up database with FLUSHALL"
                                    )
                                    try:
                                        for r in redis_conns:
                                            r.flushall()
                                    except Exception as e:
                                        logging.error(
                                            f"FLUSHALL failed after benchmark failure: {e}"
                                        )
                                # Continue with the test but log the failure
                                client_container_stdout = f"ERROR: {stderr}"

                            move_command = "mv {} {}".format(
                                local_benchmark_output_filename, temporary_dir_client
                            )
                            os.system(move_command)
                        else:
                            logging.info(
                                "Using docker image {} as benchmark client image (cpuset={}) with the following args: {}".format(
                                    client_container_image,
                                    client_cpuset_cpus,
                                    benchmark_command_str,
                                )
                            )

                            # Use explicit container management for single client
                            # Set working directory based on tool
                            working_dir = benchmark_tool_workdir
                            if "vector-db-benchmark" in benchmark_tool:
                                working_dir = (
                                    "/app"  # vector-db-benchmark needs to run from /app
                                )

                            # Prepare volumes
                            volumes = {
                                temporary_dir_client: {
                                    "bind": client_mnt_point,
                                    "mode": "rw",
                                },
                            }

                            # For vector-db-benchmark, also mount the results directory
                            if "vector-db-benchmark" in benchmark_tool:
                                volumes[temporary_dir_client] = {
                                    "bind": "/app/results",
                                    "mode": "rw",
                                }

                            container_kwargs = {
                                "image": client_container_image,
                                "volumes": volumes,
                                "auto_remove": False,
                                "privileged": True,
                                "working_dir": working_dir,
                                "command": benchmark_command_str,
                                "network_mode": "host",
                                "detach": True,
                                "cpuset_cpus": client_cpuset_cpus,
                            }

                            # Only add user for non-vector-db-benchmark tools to avoid permission issues
                            if "vector-db-benchmark" not in benchmark_tool:
                                container_kwargs["user"] = (
                                    f"{os.getuid()}:{os.getgid()}"
                                )

                            # Add environment variables for vector-db-benchmark
                            if "vector-db-benchmark" in benchmark_tool:
                                try:
                                    container_kwargs["environment"] = env_vars
                                except NameError:
                                    # env_vars not defined, skip environment variables
                                    pass

                            container = docker_client.containers.run(**container_kwargs)

                            # Wait for container and get output
                            try:
                                exit_code = container.wait()
                                client_container_stdout = container.logs().decode(
                                    "utf-8"
                                )
                                logging.info(
                                    f"Single client completed with exit code: {exit_code}"
                                )
                            except Exception as wait_error:
                                logging.error(f"Single client wait error: {wait_error}")
                                client_container_stdout = container.logs().decode(
                                    "utf-8"
                                )
                            finally:
                                # Clean up container
                                try:
                                    container.remove(force=True)
                                except Exception as cleanup_error:
                                    logging.warning(
                                        f"Single client cleanup error: {cleanup_error}"
                                    )

                    benchmark_end_time = datetime.datetime.now()
                    benchmark_duration_seconds = (
                        calculate_client_tool_duration_and_check(
                            benchmark_end_time, benchmark_start_time
                        )
                    )
                    (
                        _,
                        overall_tabular_data_map,
                    ) = profilers_stop_if_required(
                        datasink_push_results_redistimeseries,
                        benchmark_duration_seconds,
                        collection_summary_str,
                        dso,
                        tf_github_org,
                        tf_github_repo,
                        profiler_name,
                        profilers_artifacts_matrix,
                        profilers_enabled,
                        profilers_map,
                        redis_pids,
                        S3_BUCKET_NAME,
                        test_name,
                    )

                    # wait for remote profiling completion
                    if remote_profiler is not None:
                        try:
                            logging.info("Waiting for remote profiling to complete...")
                            profiling_success = remote_profiler.wait_for_completion(
                                timeout=60
                            )
                            if profiling_success:
                                logging.info("Remote profiling completed successfully")
                            else:
                                logging.warning(
                                    "Remote profiling did not complete successfully"
                                )
                        except Exception as e:
                            logging.error(
                                f"Error waiting for remote profiling completion: {e}"
                            )

                    logging.info("Printing client tool stdout output")
                    if client_container_stdout:
                        print("=== Container Output ===")
                        print(client_container_stdout)
                        print("=== End Container Output ===")
                    else:
                        logging.warning("No container output captured")

                    used_memory_check(
                        test_name,
                        benchmark_required_memory,
                        redis_conns,
                        "end of benchmark",
                        used_memory_check_fail,
                    )
                    datapoint_time_ms = start_time_ms

                    post_process_benchmark_results(
                        benchmark_tool,
                        local_benchmark_output_filename,
                        datapoint_time_ms,
                        start_time_str,
                        client_container_stdout,
                        None,
                    )
                    # Check if we have multi-client results with aggregated JSON
                    if (
                        is_multiple_clients
                        and client_container_stdout.strip().startswith("{")
                    ):
                        # Use aggregated JSON from multi-client runner
                        logging.info(
                            "Using aggregated JSON results from multi-client execution"
                        )
                        results_dict = json.loads(client_container_stdout)

                        # Validate benchmark metrics
                        is_valid, validation_error = validate_benchmark_metrics(
                            results_dict, test_name, benchmark_config, default_metrics
                        )
                        if not is_valid:
                            logging.error(
                                f"Test {test_name} failed metric validation: {validation_error}"
                            )
                            test_result = False
                            delete_temporary_files(
                                temporary_dir_client=temporary_dir_client,
                                full_result_path=full_result_path,
                                benchmark_tool_global=benchmark_tool_global,
                            )
                            continue

                        # Print results table for multi-client
                        print_results_table_stdout(
                            benchmark_config,
                            default_metrics,
                            results_dict,
                            setup_type,
                            test_name,
                        )
                        # Add results to overall summary table
                        prepare_overall_total_test_results(
                            benchmark_config,
                            default_metrics,
                            results_dict,
                            test_name,
                            results_matrix,
                            redis_conns,
                            setup_name,
                        )
                    else:
                        # Single client - read from file as usual
                        full_result_path = local_benchmark_output_filename
                        if "memtier_benchmark" in benchmark_tool:
                            full_result_path = "{}/{}".format(
                                temporary_dir_client, local_benchmark_output_filename
                            )
                        elif "pubsub-sub-bench" in benchmark_tool:
                            full_result_path = "{}/{}".format(
                                temporary_dir_client, local_benchmark_output_filename
                            )
                        elif "vector-db-benchmark" in benchmark_tool:
                            # For vector-db-benchmark, look for summary JSON file
                            summary_files = [
                                f
                                for f in os.listdir(temporary_dir_client)
                                if f.endswith("-summary.json")
                            ]
                            if summary_files:
                                full_result_path = os.path.join(
                                    temporary_dir_client, summary_files[0]
                                )
                                logging.info(
                                    f"Found vector-db-benchmark summary file: {summary_files[0]}"
                                )
                            else:
                                logging.warning(
                                    "No vector-db-benchmark summary JSON file found"
                                )
                                # Create empty results dict to avoid crash
                                results_dict = {}

                        logging.info(f"Reading results json from {full_result_path}")

                        if (
                            "vector-db-benchmark" in benchmark_tool
                            and not os.path.exists(full_result_path)
                        ):
                            # Handle case where vector-db-benchmark didn't produce results
                            results_dict = {}
                            logging.warning(
                                "Vector-db-benchmark did not produce results file"
                            )
                        else:
                            with open(
                                full_result_path,
                                "r",
                            ) as json_file:
                                results_dict = json.load(json_file)

                        # Validate benchmark metrics
                        is_valid, validation_error = validate_benchmark_metrics(
                            results_dict, test_name, benchmark_config, default_metrics
                        )
                        if not is_valid:
                            logging.error(
                                f"Test {test_name} failed metric validation: {validation_error}"
                            )
                            test_result = False
                            delete_temporary_files(
                                temporary_dir_client=temporary_dir_client,
                                full_result_path=full_result_path,
                                benchmark_tool_global=benchmark_tool_global,
                            )
                            continue

                        print_results_table_stdout(
                            benchmark_config,
                            default_metrics,
                            results_dict,
                            setup_type,
                            test_name,
                            None,
                        )
                        prepare_overall_total_test_results(
                            benchmark_config,
                            default_metrics,
                            results_dict,
                            test_name,
                            results_matrix,
                            redis_conns,
                            setup_name,
                        )

                    dataset_load_duration_seconds = 0

                    exporter_datasink_common(
                        benchmark_config,
                        benchmark_duration_seconds,
                        build_variant_name,
                        datapoint_time_ms,
                        dataset_load_duration_seconds,
                        datasink_conn,
                        datasink_push_results_redistimeseries,
                        git_branch,
                        git_version,
                        metadata,
                        redis_conns,
                        results_dict,
                        running_platform,
                        args.deployment_name,
                        args.deployment_type,
                        test_name,
                        tf_github_org,
                        tf_github_repo,
                        tf_triggering_env,
                        topology_spec_name,
                        default_metrics,
                        git_hash,
                    )
                    test_result = True
                    total_test_suite_runs = total_test_suite_runs + 1

                    if args.flushall_on_every_test_end:
                        logging.info("Sending FLUSHALL to the DB")
                        for r in redis_conns:
                            r.flushall()

                        # Send MEMORY PURGE after FLUSHALL for memory comparison mode
                        if memory_comparison_only:
                            try:
                                logging.info(
                                    "Sending MEMORY PURGE after FLUSHALL at test end"
                                )
                                for r in redis_conns:
                                    r.execute_command("MEMORY", "PURGE")
                            except Exception as e:
                                logging.warning(
                                    f"MEMORY PURGE failed after FLUSHALL at test end: {e}"
                                )

                except KeyboardInterrupt:
                    logging.info("KeyboardInterrupt caught. Exiting...")
                    print("\nKeyboardInterrupt caught. Exiting...")
                    break
                except:
                    logging.critical(
                        "Some unexpected exception was caught "
                        "during local work. Failing test...."
                    )
                    logging.critical(sys.exc_info()[0])
                    print("-" * 60)
                    traceback.print_exc(file=sys.stdout)
                    print("-" * 60)
                    test_result = False

                    # Clean up database after exception to prevent contamination of next test
                    if (
                        args.flushall_on_every_test_end
                        or args.flushall_on_every_test_start
                    ):
                        logging.warning(
                            "Exception caught - cleaning up database with FLUSHALL"
                        )
                        try:
                            for r in redis_conns:
                                r.flushall()
                        except Exception as e:
                            logging.error(f"FLUSHALL failed after exception: {e}")

                    # Check if user requested exit via Ctrl+C
                    if _exit_requested:
                        logging.info(
                            "Exit requested by user. Stopping after exception."
                        )
                        break
                # tear-down
                logging.info("Tearing down setup")
                for container in client_containers:
                    if type(container) == Container:
                        try:
                            container.stop()
                        except docker.errors.NotFound:
                            logging.info(
                                "When trying to stop Client container with id {} and image {} it was already stopped".format(
                                    container.id, container.image
                                )
                            )
                            pass

                if client_aggregated_results_folder != "":
                    os.makedirs(client_aggregated_results_folder, exist_ok=True)
                    dest_fpath = "{}/{}".format(
                        client_aggregated_results_folder,
                        local_benchmark_output_filename,
                    )
                    # Safety check: ensure full_result_path exists before copying
                    if full_result_path is None:
                        logging.error(
                            f"Cannot preserve results: full_result_path is None for test {test_name}. "
                            f"This may indicate a missing benchmark tool handler in the result path construction."
                        )
                    elif not os.path.exists(full_result_path):
                        logging.error(
                            f"Cannot preserve results: file does not exist at {full_result_path} for test {test_name}"
                        )
                    else:
                        logging.info(
                            "Preserving local results file {} into {}".format(
                                full_result_path, dest_fpath
                            )
                        )
                        shutil.copy(full_result_path, dest_fpath)
                overall_result &= test_result

                delete_temporary_files(
                    temporary_dir_client=temporary_dir_client,
                    full_result_path=full_result_path,
                    benchmark_tool_global=benchmark_tool_global,
                )

    # Check if user requested exit via Ctrl+C
    if _exit_requested:
        logging.info("Exit requested by user. Printing summary before exit.")
        print(
            "\nExecution stopped by user request. Printing summary of completed tests..."
        )

    # Print Redis server information section before results
    if len(results_matrix) > 0:
        # Get redis_conns from the first test context (we need to pass it somehow)
        # For now, try to get it from the current context if available
        try:
            print_redis_info_section(redis_conns)
        except Exception as e:
            logging.info(f"Could not connect to Redis for server info: {e}")

    table_name = "Results for entire test-suite"
    results_matrix_headers = [
        "Test Name",
        "Metric JSON Path",
        "Metric Value",
    ]
    writer = MarkdownTableWriter(
        table_name=table_name,
        headers=results_matrix_headers,
        value_matrix=results_matrix,
    )
    writer.write_table()

    # Add note if execution was stopped early
    if _exit_requested:
        print(
            "\n(Note: Execution was stopped early by user request - showing results for completed tests only)"
        )

    if client_aggregated_results_folder != "":
        os.makedirs(client_aggregated_results_folder, exist_ok=True)
        dest_fpath = f"{client_aggregated_results_folder}/aggregate-results.csv"
        logging.info(f"Storing an aggregated results CSV into {full_result_path}")

        csv_writer = CsvTableWriter(
            table_name=table_name,
            headers=results_matrix_headers,
            value_matrix=results_matrix,
        )
        csv_writer.dump(dest_fpath)

    # Print memory comparison summary if in memory comparison mode
    if memory_comparison_only and memory_results:
        logging.info("\n" + "=" * 80)
        logging.info("MEMORY COMPARISON SUMMARY")
        logging.info("=" * 80)
        logging.info(f"Total unique datasets loaded: {len(loaded_datasets)}")
        if loaded_datasets:
            logging.info(f"Datasets: {', '.join(sorted(loaded_datasets))}")
        logging.info("=" * 80)

        # Create memory summary table
        memory_headers = [
            "Test Name",
            "Total Allocated",
            "Dataset Bytes",
            "Keys Count",
            "Bytes/Key",
            "Dataset %",
            "Overhead",
            "Fragmentation",
            "Alloc Fragmentation",
            "Object Encoding",
            "Encoding Confidence",
            "Scan Type",
        ]

        memory_matrix = []
        for result in memory_results:
            # Convert bytes to human readable format
            total_mb = result["total_allocated"] / (1024 * 1024)
            dataset_mb = result["dataset_bytes"] / (1024 * 1024)
            overhead_mb = result["overhead_total"] / (1024 * 1024)

            memory_matrix.append(
                [
                    result["test_name"],
                    f"{total_mb:.1f}MB",
                    f"{dataset_mb:.1f}MB",
                    f"{result['keys_count']:,}",
                    f"{result['keys_bytes_per_key']:.0f}B",
                    f"{result['dataset_percentage']:.1f}%",
                    f"{overhead_mb:.1f}MB",
                    f"{result['fragmentation']:.2f}",
                    f"{result['allocator_fragmentation_ratio']:.3f}",
                    result.get("object_encoding", "unknown"),
                    f"{result.get('encoding_confidence', 0.0)*100:.1f}%",
                    (
                        "complete"
                        if result.get("encoding_is_complete_scan", False)
                        else "sample"
                    ),
                ]
            )

        memory_writer = MarkdownTableWriter(
            table_name="Memory Usage Summary",
            headers=memory_headers,
            value_matrix=memory_matrix,
        )
        memory_writer.write_table()

    if dry_run is True:
        mode_description = (
            "memory comparison" if memory_comparison_only else "benchmark"
        )
        logging.info(
            "Number of tests that would have been run ({}): {}".format(
                mode_description, dry_run_count
            )
        )
        if _exit_requested:
            logging.info("(Note: Execution was stopped early by user request)")
        if dry_run_tests:
            logging.info(f"Tests that would be run ({mode_description} mode):")
            for test in dry_run_tests:
                logging.info(f"  - {test}")
            final_test_regex = "|".join(dry_run_tests)
            logging.info(f"Final test regex: {final_test_regex}")

            # For memory comparison mode, show dataset analysis
            if memory_comparison_only:
                unique_datasets = set()
                tests_with_datasets = 0

                for test_file in testsuite_spec_files:
                    if defaults_filename in test_file:
                        continue
                    try:
                        with open(test_file, "r") as stream:
                            benchmark_config = yaml.safe_load(stream)

                        test_name = extract_test_name_from_test_configuration_file(
                            test_file
                        )
                        if (
                            test_name in dry_run_tests
                            and "dbconfig" in benchmark_config
                        ):
                            # Skip load tests in dry run analysis too
                            keyspacelen = (
                                benchmark_config["dbconfig"]
                                .get("check", {})
                                .get("keyspacelen", None)
                            )
                            if keyspacelen is not None and keyspacelen == 0:
                                continue

                            dataset_name = benchmark_config["dbconfig"].get(
                                "dataset_name"
                            )
                            if dataset_name:
                                unique_datasets.add(dataset_name)
                                tests_with_datasets += 1

                    except Exception as e:
                        logging.debug(f"Error analyzing {test_file} for dry run: {e}")

                if tests_with_datasets > 0:
                    logging.info(f"\nMemory comparison analysis:")
                    logging.info(f"  Tests with datasets: {tests_with_datasets}")
                    logging.info(f"  Unique datasets: {len(unique_datasets)}")
                    logging.info(
                        f"  Dataset ingestion savings: {tests_with_datasets - len(unique_datasets)} skipped loads"
                    )
                    if unique_datasets:
                        logging.info(f"  Datasets that would be loaded:")
                        for dataset in sorted(unique_datasets):
                            logging.info(f"    - {dataset}")


def get_maxmemory(r):
    memory_info = r.info("memory")

    # Check if maxmemory key exists in Redis memory info
    if "maxmemory" not in memory_info:
        logging.warning(
            "maxmemory not present in Redis memory info. Cannot enforce memory checks."
        )
        return 0

    maxmemory = int(memory_info["maxmemory"])
    if maxmemory == 0:
        total_system_memory = int(memory_info["total_system_memory"])
        logging.info(" Using total system memory as max {}".format(total_system_memory))
        maxmemory = total_system_memory
    else:
        logging.info(" Detected redis maxmemory config value {}".format(maxmemory))

    return maxmemory


def get_benchmark_required_memory(benchmark_config):
    benchmark_required_memory = 0
    if "dbconfig" in benchmark_config:
        if "resources" in benchmark_config["dbconfig"]:
            resources = benchmark_config["dbconfig"]["resources"]
            if "requests" in resources:
                resources_requests = benchmark_config["dbconfig"]["resources"][
                    "requests"
                ]
                if "memory" in resources_requests:
                    benchmark_required_memory = resources_requests["memory"]
                    benchmark_required_memory = int(
                        parse_size(benchmark_required_memory)
                    )
                    logging.info(
                        "Benchmark required memory: {} Bytes".format(
                            benchmark_required_memory
                        )
                    )
    return benchmark_required_memory


def used_memory_check(
    test_name,
    benchmark_required_memory,
    redis_conns,
    stage,
    used_memory_check_fail=False,
):
    used_memory = 0
    for conn in redis_conns:
        info_mem = conn.info("memory")
        if "used_memory" in info_mem:
            used_memory = used_memory + info_mem["used_memory"]
        else:
            logging.warning(
                "used_memory not present in Redis memory info. Cannot enforce memory checks."
            )
    used_memory_gb = int(math.ceil(float(used_memory) / 1024.0 / 1024.0 / 1024.0))
    logging.info("Benchmark used memory at {}: {}g".format(stage, used_memory_gb))
    if used_memory > benchmark_required_memory:
        logging.error(
            "The benchmark {} specified a dbconfig resource request of memory ({}) bellow the REAL MEMORY USAGE OF: {}. FIX IT!.".format(
                test_name, benchmark_required_memory, used_memory_gb
            )
        )
        if used_memory_check_fail:
            exit(1)


def cp_to_workdir(benchmark_tool_workdir, srcfile):
    head, filename = os.path.split(srcfile)
    dstfile = f"{benchmark_tool_workdir}/{filename}"
    shutil.copyfile(srcfile, dstfile)
    logging.info(
        f"Copying to workdir the following file {srcfile}. Final workdir file {dstfile}"
    )
    return dstfile, filename


def print_results_table_stdout(
    benchmark_config,
    default_metrics,
    results_dict,
    setup_name,
    test_name,
    cpu_usage=None,
):
    # check which metrics to extract
    (
        _,
        metrics,
    ) = merge_default_and_config_metrics(
        benchmark_config,
        default_metrics,
        None,
    )
    table_name = f"Results for {test_name} test-case on {setup_name} topology"
    results_matrix_headers = [
        "Metric JSON Path",
        "Metric Value",
    ]
    results_matrix = extract_results_table(metrics, results_dict)

    # Use resolved metric name for precision_summary metrics, otherwise use original path
    def get_display_name(x):
        # For precision_summary metrics with wildcards, construct the resolved path
        if (
            len(x) > 1
            and isinstance(x[0], str)
            and "precision_summary" in x[0]
            and "*" in x[0]
        ):

            # Look for the precision level in the cleaned metrics logs
            # We need to find the corresponding cleaned metric to get the precision level
            # For now, let's extract it from the time series logs that we know are working
            # The pattern is: replace "*" with the actual precision level

            # Since we know from logs that the precision level is available,
            # let's reconstruct it from the metric context path (x[1]) if available
            if (
                len(x) > 1
                and isinstance(x[1], str)
                and x[1].startswith("'")
                and x[1].endswith("'")
            ):
                precision_level = x[1]  # This should be something like "'1.0000'"
                resolved_path = x[0].replace("*", precision_level)
                return resolved_path

        return x[0]  # Use original path

    results_matrix = [[get_display_name(x), f"{x[3]:.3f}"] for x in results_matrix]
    writer = MarkdownTableWriter(
        table_name=table_name,
        headers=results_matrix_headers,
        value_matrix=results_matrix,
    )
    writer.write_table()


def print_redis_info_section(redis_conns):
    """Print Redis server information as a separate section"""
    if redis_conns is not None and len(redis_conns) > 0:
        try:
            redis_info = redis_conns[0].info()
            server_name = "redis"
            if "server_name" in redis_info:
                server_name = redis_info["server_name"]

            print("\n# Redis Server Information")
            redis_info_data = [
                [
                    f"{server_name} version",
                    redis_info.get(f"{server_name}_version", "unknown"),
                ],
                ["redis version", redis_info.get("redis_version", "unknown")],
                ["io_threads_active", redis_info.get("io_threads_active", "unknown")],
                [
                    f"{server_name} Git SHA1",
                    redis_info.get("redis_git_sha1", "unknown"),
                ],
                [
                    f"{server_name} Git Dirty",
                    str(redis_info.get("redis_git_dirty", "unknown")),
                ],
                [
                    f"{server_name} Build ID",
                    redis_info.get("redis_build_id", "unknown"),
                ],
                [f"{server_name} Mode", redis_info.get("redis_mode", "unknown")],
                ["OS", redis_info.get("os", "unknown")],
                ["Arch Bits", str(redis_info.get("arch_bits", "unknown"))],
                ["GCC Version", redis_info.get("gcc_version", "unknown")],
                ["Process ID", str(redis_info.get("process_id", "unknown"))],
                ["TCP Port", str(redis_info.get("tcp_port", "unknown"))],
                [
                    "Uptime (seconds)",
                    str(redis_info.get("uptime_in_seconds", "unknown")),
                ],
            ]

            from pytablewriter import MarkdownTableWriter

            writer = MarkdownTableWriter(
                table_name="",
                headers=["Property", "Value"],
                value_matrix=redis_info_data,
            )
            writer.write_table()

            logging.info(
                f"Displayed Redis server information: Redis {redis_info.get('redis_version', 'unknown')}"
            )
        except Exception as e:
            logging.warning(f"Failed to collect Redis server information: {e}")


def get_supported_redis_commands(redis_conns):
    """Get list of supported Redis commands from the server"""
    if redis_conns is not None and len(redis_conns) > 0:
        try:
            # Execute COMMAND to get all supported commands
            commands_info = redis_conns[0].execute_command("COMMAND")
            logging.info(
                f"COMMAND response type: {type(commands_info)}, length: {len(commands_info) if hasattr(commands_info, '__len__') else 'N/A'}"
            )

            # Extract command names
            supported_commands = set()

            if isinstance(commands_info, dict):
                # COMMAND response is a dict with command names as keys
                for cmd_name in commands_info.keys():
                    if isinstance(cmd_name, bytes):
                        cmd_name = cmd_name.decode("utf-8")
                    supported_commands.add(str(cmd_name).upper())
            elif isinstance(commands_info, (list, tuple)):
                # Fallback for list format (first element of each command info array)
                for cmd_info in commands_info:
                    if isinstance(cmd_info, (list, tuple)) and len(cmd_info) > 0:
                        cmd_name = cmd_info[0]
                        if isinstance(cmd_name, bytes):
                            cmd_name = cmd_name.decode("utf-8")
                        supported_commands.add(str(cmd_name).upper())

            logging.info(
                f"Retrieved {len(supported_commands)} supported Redis commands"
            )

            # Handle case where COMMAND returns 0 commands (likely not supported)
            if len(supported_commands) == 0:
                logging.warning(
                    "COMMAND returned 0 commands - likely not supported by this Redis instance"
                )
                return None

            # Log some sample commands for debugging
            if supported_commands:
                sample_commands = sorted(list(supported_commands))[:10]
                logging.info(f"Sample commands: {sample_commands}")

                # Check specifically for vector commands
                vector_commands = [
                    cmd for cmd in supported_commands if cmd.startswith("V")
                ]
                if vector_commands:
                    logging.info(f"Vector commands found: {sorted(vector_commands)}")

            return supported_commands
        except Exception as e:
            logging.warning(f"Failed to get supported Redis commands: {e}")
            logging.warning("Proceeding without command validation")
            return None
    return None


def check_test_command_support(benchmark_config, supported_commands):
    """Check if all tested-commands in the benchmark config are supported"""
    if supported_commands is None or len(supported_commands) == 0:
        logging.warning(
            "No supported commands list available (COMMAND not supported or returned 0 commands), skipping command check"
        )
        return True, []

    if "tested-commands" not in benchmark_config:
        logging.info("No tested-commands specified in benchmark config")
        return True, []

    tested_commands = benchmark_config["tested-commands"]
    unsupported_commands = []

    for cmd in tested_commands:
        cmd_upper = cmd.upper()
        if cmd_upper not in supported_commands:
            unsupported_commands.append(cmd)

    if unsupported_commands:
        logging.warning(f"Unsupported commands found: {unsupported_commands}")
        return False, unsupported_commands
    else:
        logging.info(f"All tested commands are supported: {tested_commands}")
        return True, []


def prepare_overall_total_test_results(
    benchmark_config,
    default_metrics,
    results_dict,
    test_name,
    overall_results_matrix,
    redis_conns=None,
    topology=None,
):
    # check which metrics to extract
    (
        _,
        metrics,
    ) = merge_default_and_config_metrics(
        benchmark_config,
        default_metrics,
        None,
    )
    current_test_results_matrix = extract_results_table(metrics, results_dict)

    # Use the same display name logic as in the individual test results
    def get_overall_display_name(x):
        # For precision_summary metrics with wildcards, construct the resolved path
        if (
            len(x) > 1
            and isinstance(x[0], str)
            and "precision_summary" in x[0]
            and "*" in x[0]
        ):

            # Reconstruct resolved path from metric context path (x[1]) if available
            if (
                len(x) > 1
                and isinstance(x[1], str)
                and x[1].startswith("'")
                and x[1].endswith("'")
            ):
                precision_level = x[1]  # This should be something like "'1.0000'"
                resolved_path = x[0].replace("*", precision_level)
                return resolved_path

        return x[0]  # Use original path

    # Include topology in the test name if provided
    test_name_with_topology = test_name
    if topology:
        test_name_with_topology = f"{topology}-{test_name}"

    current_test_results_matrix = [
        [test_name_with_topology, get_overall_display_name(x), f"{x[3]:.3f}"]
        for x in current_test_results_matrix
    ]
    overall_results_matrix.extend(current_test_results_matrix)


def data_prepopulation_step(
    benchmark_config,
    benchmark_tool_workdir,
    client_cpuset_cpus,
    docker_client,
    git_hash,
    port,
    temporary_dir,
    test_name,
    host,
    tls_enabled=False,
    tls_skip_verify=False,
    tls_cert=None,
    tls_key=None,
    tls_cacert=None,
    resp_version=None,
    benchmark_local_install=False,
    password=None,
    oss_cluster_api_enabled=False,
    unix_socket="",
    timeout_buffer=60,
    args=None,
):
    result = True
    # setup the benchmark
    (
        start_time,
        start_time_ms,
        start_time_str,
    ) = get_start_time_vars()
    local_benchmark_output_filename = get_local_run_full_filename(
        start_time_str,
        git_hash,
        "preload__" + test_name,
        "oss-standalone",
    )
    preload_image = extract_client_container_image(
        benchmark_config["dbconfig"], "preload_tool"
    )
    preload_tool = extract_client_tool(benchmark_config["dbconfig"], "preload_tool")

    # Set preload tool path based on local install option
    if benchmark_local_install and "memtier_benchmark" in preload_tool and args:
        full_benchmark_path = getattr(args, "memtier_bin_path", "memtier_benchmark")
    else:
        full_benchmark_path = f"/usr/local/bin/{preload_tool}"
    client_mnt_point = "/mnt/client/"

    if "memtier_benchmark" in preload_tool:
        override_memtier_test_time_preload = 0
        (
            _,
            preload_command_str,
            arbitrary_command,
        ) = prepare_memtier_benchmark_parameters(
            benchmark_config["dbconfig"]["preload_tool"],
            full_benchmark_path,
            port,
            host,
            password,
            local_benchmark_output_filename,
            oss_cluster_api_enabled,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
            tls_cacert,
            resp_version,
            override_memtier_test_time_preload,
            1,
            unix_socket,
        )

        # run the benchmark
        preload_start_time = datetime.datetime.now()

        if benchmark_local_install:
            logging.info("Running memtier benchmark outside of docker")

            preload_command_str = (
                "taskset -c " + client_cpuset_cpus + " " + preload_command_str
            )

            # Calculate timeout for preload process
            process_timeout = calculate_process_timeout(
                preload_command_str, timeout_buffer
            )

            # Run with timeout
            success, client_container_stdout, stderr = run_local_command_with_timeout(
                preload_command_str, process_timeout, "memtier preload"
            )

            if not success:
                logging.error(f"Memtier preload failed: {stderr}")
                result = False
                return result

            move_command = "mv {} {}".format(
                local_benchmark_output_filename, temporary_dir
            )
            os.system(move_command)

        else:
            logging.info(
                "Using docker image {} as benchmark PRELOAD image (cpuset={}) with the following args: {}".format(
                    preload_image,
                    client_cpuset_cpus,
                    preload_command_str,
                )
            )
            # Use explicit container management for preload tool
            container = docker_client.containers.run(
                image=preload_image,
                volumes={
                    temporary_dir: {
                        "bind": client_mnt_point,
                        "mode": "rw",
                    },
                },
                auto_remove=False,
                privileged=True,
                working_dir=benchmark_tool_workdir,
                command=preload_command_str,
                network_mode="host",
                detach=True,
                cpuset_cpus=client_cpuset_cpus,
            )

            # Wait for preload container and get output
            try:
                exit_code = container.wait()
                client_container_stdout = container.logs().decode("utf-8")
                logging.info(f"Preload tool completed with exit code: {exit_code}")
            except Exception as wait_error:
                logging.error(f"Preload tool wait error: {wait_error}")
                client_container_stdout = container.logs().decode("utf-8")
            finally:
                # Clean up container
                try:
                    container.remove(force=True)
                except Exception as cleanup_error:
                    logging.warning(f"Preload tool cleanup error: {cleanup_error}")

        preload_end_time = datetime.datetime.now()
        preload_duration_seconds = calculate_client_tool_duration_and_check(
            preload_end_time, preload_start_time, "Preload", False
        )
        logging.info(
            "Tool {} seconds to load data. Output {}".format(
                preload_duration_seconds,
                client_container_stdout,
            )
        )
    return result


def generate_cpuset_cpus(ceil_db_cpu_limit, current_cpu_pos):
    previous_cpu_pos = current_cpu_pos
    current_cpu_pos = current_cpu_pos + int(ceil_db_cpu_limit)
    db_cpuset_cpus = ",".join(
        [str(x) for x in range(previous_cpu_pos, current_cpu_pos)]
    )
    return db_cpuset_cpus, current_cpu_pos
