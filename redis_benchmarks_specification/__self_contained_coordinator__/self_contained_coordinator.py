# Import warning suppression first
from redis_benchmarks_specification.__common__.suppress_warnings import *

import datetime
import json
import logging
import pathlib
import shutil
import subprocess
import tempfile
import threading
import traceback
import re
import docker
import docker.errors
import redis
import os
from pathlib import Path
import sys
import time
import base64
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from docker.models.containers import Container
from redis_benchmarks_specification.__self_contained_coordinator__.post_processing import (
    post_process_vector_db,
)
from redisbench_admin.profilers.profilers_local import (
    check_compatible_system_and_kernel_and_prepare_profile,
)

from redis_benchmarks_specification.__common__.env import (
    LOG_FORMAT,
    LOG_DATEFMT,
    LOG_LEVEL,
    REDIS_HEALTH_CHECK_INTERVAL,
    REDIS_SOCKET_TIMEOUT,
    REDIS_BINS_EXPIRE_SECS,
)
from redis_benchmarks_specification.__common__.github import (
    check_github_available_and_actionable,
    check_benchmark_running_comment,
    update_comment_if_needed,
    create_new_pr_comment,
    generate_benchmark_started_pr_comment,
    check_regression_comment,
)
from redis_benchmarks_specification.__common__.package import (
    get_version_string,
    populate_with_poetry_data,
)
from redis_benchmarks_specification.__common__.runner import (
    extract_testsuites,
    reset_commandstats,
    exporter_datasink_common,
    execute_init_commands,
)
from redis_benchmarks_specification.__common__.timeseries import (
    datasink_profile_tabular_data,
)
from redis_benchmarks_specification.__compare__.compare import (
    compute_regression_table,
    prepare_regression_comment,
    extract_default_branch_and_metric,
)
from redis_benchmarks_specification.__runner__.runner import (
    print_results_table_stdout,
    prepare_memtier_benchmark_parameters,
    validate_benchmark_metrics,
)
from redis_benchmarks_specification.__self_contained_coordinator__.args import (
    create_self_contained_coordinator_args,
)
from redis_benchmarks_specification.__self_contained_coordinator__.runners import (
    build_runners_consumer_group_create,
    get_runners_consumer_group_name,
    clear_pending_messages_for_consumer,
    reset_consumer_group_to_latest,
)
from redis_benchmarks_specification.__setups__.topologies import get_topologies


from redisbench_admin.profilers.profilers_local import (
    local_profilers_platform_checks,
    profilers_start_if_required,
    profilers_stop_if_required,
)
from redisbench_admin.run.common import (
    get_start_time_vars,
    prepare_benchmark_parameters,
)
from redisbench_admin.run.grafana import generate_artifacts_table_grafana_redis

from redisbench_admin.run.run import calculate_client_tool_duration_and_check
from redisbench_admin.utils.benchmark_config import (
    get_final_benchmark_config,
    get_defaults,
)
from redisbench_admin.utils.local import get_local_run_full_filename
from redisbench_admin.utils.results import post_process_benchmark_results

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
    get_arch_specific_stream_name,
    S3_BUCKET_NAME,
)
from redis_benchmarks_specification.__common__.spec import (
    extract_build_variant_variations,
    extract_client_cpu_limit,
    extract_client_tool,
    extract_client_container_image,
    extract_redis_dbconfig_parameters,
    extract_redis_configuration_from_topology,
)
from redis_benchmarks_specification.__self_contained_coordinator__.artifacts import (
    restore_build_artifacts_from_test_details,
)
from redis_benchmarks_specification.__self_contained_coordinator__.build_info import (
    extract_build_info_from_streamdata,
)

# Global variables for HTTP server control
_reset_queue_requested = False
_exclusive_hardware = False
_http_auth_username = None
_http_auth_password = None
_flush_timestamp = None


class CoordinatorHTTPHandler(BaseHTTPRequestHandler):
    """HTTP request handler for coordinator endpoints"""

    def log_message(self, format, *args):
        """Override to use our logging system"""
        logging.info(f"HTTP {format % args}")

    def _authenticate(self):
        """Check if the request is authenticated"""
        global _http_auth_username, _http_auth_password

        # Check for Authorization header
        auth_header = self.headers.get("Authorization")
        if not auth_header:
            return False

        # Parse Basic auth
        try:
            if not auth_header.startswith("Basic "):
                return False

            # Decode base64 credentials
            encoded_credentials = auth_header[6:]  # Remove 'Basic ' prefix
            decoded_credentials = base64.b64decode(encoded_credentials).decode("utf-8")
            username, password = decoded_credentials.split(":", 1)

            # Verify credentials
            return username == _http_auth_username and password == _http_auth_password

        except Exception as e:
            logging.warning(f"Authentication error: {e}")
            return False

    def _send_auth_required(self):
        """Send 401 Unauthorized response"""
        self.send_response(401)
        self.send_header(
            "WWW-Authenticate", 'Basic realm="Redis Benchmarks Coordinator"'
        )
        self.send_header("Content-type", "application/json")
        self.end_headers()
        response = {
            "error": "Authentication required",
            "message": "Please provide valid credentials using Basic authentication",
        }
        self.wfile.write(json.dumps(response).encode())

    def do_GET(self):
        """Handle GET requests"""
        # Check authentication
        if not self._authenticate():
            self._send_auth_required()
            return

        parsed_path = urlparse(self.path)

        if parsed_path.path == "/ping":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            response = {
                "status": "healthy",
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "service": "redis-benchmarks-self-contained-coordinator",
            }
            self.wfile.write(json.dumps(response).encode())

        elif parsed_path.path == "/containers":
            # Check for stuck containers
            stuck_containers = self._check_stuck_containers()

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            response = {
                "status": "success",
                "stuck_containers": stuck_containers,
                "total_stuck": len(stuck_containers),
                "timestamp": datetime.datetime.utcnow().isoformat(),
            }
            self.wfile.write(json.dumps(response).encode())

        else:
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Not found"}).encode())

    def do_POST(self):
        """Handle POST requests"""
        # Check authentication
        if not self._authenticate():
            self._send_auth_required()
            return

        global _reset_queue_requested, _flush_timestamp

        parsed_path = urlparse(self.path)

        if parsed_path.path == "/reset-queue":
            try:
                # Read request body
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length > 0:
                    post_data = self.rfile.read(content_length)
                    try:
                        request_data = json.loads(post_data.decode())
                    except json.JSONDecodeError:
                        request_data = {}
                else:
                    request_data = {}

                # Set the reset flag
                _reset_queue_requested = True
                logging.info("Queue reset requested via HTTP endpoint")

                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response = {
                    "status": "success",
                    "message": "Queue reset requested",
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                }
                self.wfile.write(json.dumps(response).encode())

            except Exception as e:
                logging.error(f"Error handling reset-queue request: {e}")
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif parsed_path.path == "/flush":
            try:
                # Read request body (optional)
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length > 0:
                    post_data = self.rfile.read(content_length)
                    try:
                        request_data = json.loads(post_data.decode())
                    except json.JSONDecodeError:
                        request_data = {}
                else:
                    request_data = {}

                # Record flush timestamp
                flush_time = datetime.datetime.utcnow()
                _flush_timestamp = flush_time

                logging.info(
                    "Flush requested via HTTP endpoint - stopping all containers and processes"
                )

                # Perform flush cleanup
                self._perform_flush_cleanup()

                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response = {
                    "status": "success",
                    "message": "Flush completed - all containers stopped and processes killed",
                    "flush_timestamp": flush_time.isoformat(),
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                }
                self.wfile.write(json.dumps(response).encode())

            except Exception as e:
                logging.error(f"Error during flush operation: {e}")
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                response = {
                    "status": "error",
                    "message": f"Flush failed: {str(e)}",
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                }
                self.wfile.write(json.dumps(response).encode())

        else:
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Not found"}).encode())

    def _perform_flush_cleanup(self):
        """Perform flush cleanup: stop all containers and kill memtier processes"""
        import subprocess

        # Kill all memtier processes
        try:
            logging.info("Killing all memtier_benchmark processes")
            result = subprocess.run(
                ["pkill", "-f", "memtier_benchmark"], capture_output=True, text=True
            )
            if result.returncode == 0:
                logging.info("Successfully killed memtier_benchmark processes")
            else:
                logging.info("No memtier_benchmark processes found to kill")

            result = subprocess.run(
                ["pkill", "-f", "memtier"], capture_output=True, text=True
            )
            if result.returncode == 0:
                logging.info("Successfully killed memtier processes")
            else:
                logging.info("No memtier processes found to kill")
        except Exception as e:
            logging.warning(f"Error killing memtier processes: {e}")

        # Stop all Docker containers with force if needed
        try:
            logging.info("Stopping all Docker containers")
            client = docker.from_env()
            containers = client.containers.list()

            if not containers:
                logging.info("No running containers found")
                return

            logging.info(f"Found {len(containers)} running containers")

            for container in containers:
                try:
                    # Get container info
                    created_time = container.attrs["Created"]
                    uptime = (
                        datetime.datetime.utcnow()
                        - datetime.datetime.fromisoformat(
                            created_time.replace("Z", "+00:00")
                        )
                    )

                    logging.info(
                        f"Stopping container: {container.name} ({container.id[:12]}) - uptime: {uptime}"
                    )

                    # Try graceful stop first
                    container.stop(timeout=10)
                    logging.info(f"Successfully stopped container: {container.name}")

                except Exception as e:
                    logging.warning(f"Error stopping container {container.name}: {e}")
                    try:
                        # Force kill if graceful stop failed
                        logging.info(f"Force killing container: {container.name}")
                        container.kill()
                        logging.info(
                            f"Successfully force killed container: {container.name}"
                        )
                    except Exception as e2:
                        logging.error(
                            f"Failed to force kill container {container.name}: {e2}"
                        )

        except Exception as e:
            logging.warning(f"Error accessing Docker client: {e}")

        logging.info("Flush cleanup completed")

    def _check_stuck_containers(self, max_hours=2):
        """Check for containers running longer than max_hours and return info"""
        try:
            client = docker.from_env()
            containers = client.containers.list()
            stuck_containers = []

            for container in containers:
                try:
                    created_time = container.attrs["Created"]
                    uptime = (
                        datetime.datetime.utcnow()
                        - datetime.datetime.fromisoformat(
                            created_time.replace("Z", "+00:00")
                        )
                    )
                    uptime_hours = uptime.total_seconds() / 3600

                    if uptime_hours > max_hours:
                        stuck_containers.append(
                            {
                                "name": container.name,
                                "id": container.id[:12],
                                "image": (
                                    container.image.tags[0]
                                    if container.image.tags
                                    else "unknown"
                                ),
                                "uptime_hours": round(uptime_hours, 2),
                                "status": container.status,
                            }
                        )
                except Exception as e:
                    logging.warning(f"Error checking container {container.name}: {e}")

            return stuck_containers
        except Exception as e:
            logging.warning(f"Error accessing Docker client: {e}")
            return []


def start_http_server(port=8080):
    """Start the HTTP server in a separate thread"""

    def run_server():
        try:
            server = HTTPServer(("0.0.0.0", port), CoordinatorHTTPHandler)
            logging.info(f"Starting HTTP server on port {port}")
            logging.info(f"Available endpoints:")
            logging.info(f"  GET  /ping - Health check")
            logging.info(f"  GET  /containers - Check for stuck containers")
            logging.info(
                f"  POST /reset-queue - Reset pending streams and skip running tests"
            )
            logging.info(
                f"  POST /flush - Stop all containers and processes, ignore work before flush time"
            )
            server.serve_forever()
        except Exception as e:
            logging.error(f"HTTP server error: {e}")

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    return server_thread


def cleanup_system_processes():
    """Clean up memtier processes and docker containers for exclusive hardware mode"""
    global _exclusive_hardware

    if not _exclusive_hardware:
        return

    logging.info("Exclusive hardware mode: Cleaning up system processes")

    try:
        # Kill all memtier_benchmark processes
        logging.info("Killing all memtier_benchmark processes")
        subprocess.run(["pkill", "-f", "memtier_benchmark"], check=False)

        # Stop all docker containers
        logging.info("Stopping all docker containers")
        docker_client = docker.from_env()
        containers = docker_client.containers.list()
        for container in containers:
            try:
                logging.info(
                    f"Stopping container: {container.name} ({container.id[:12]})"
                )
                container.stop(timeout=10)
                container.remove(force=True)
            except Exception as e:
                logging.warning(f"Error stopping container {container.id[:12]}: {e}")

        # Wait a moment for cleanup to complete
        time.sleep(2)
        logging.info("System cleanup completed")

    except Exception as e:
        logging.error(f"Error during system cleanup: {e}")


def print_directory_logs(directory_path, description=""):
    """Print all .log files in a directory for debugging purposes."""
    if not os.path.exists(directory_path):
        logging.warning(f"Directory {directory_path} does not exist")
        return

    logging.info(
        f"Printing all .log files in {description} directory: {directory_path}"
    )
    try:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                # Only process .log files
                if not file.endswith(".log"):
                    continue

                file_path = os.path.join(root, file)
                logging.info(f"Found log file: {file_path}")
                try:
                    # Try to read and print the log file content
                    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        if content.strip():  # Only print non-empty files
                            logging.info(f"Content of {file_path}:")
                            logging.info("-" * 40)
                            logging.info(content)
                            logging.info("-" * 40)
                        else:
                            logging.info(f"Log file {file_path} is empty")
                except Exception as e:
                    logging.warning(f"Could not read log file {file_path}: {e}")
    except Exception as e:
        logging.error(f"Error walking directory {directory_path}: {e}")


from redis_benchmarks_specification.__self_contained_coordinator__.cpuset import (
    extract_db_cpu_limit,
    generate_cpuset_cpus,
)
from redis_benchmarks_specification.__self_contained_coordinator__.clients import (
    prepare_vector_db_benchmark_parameters,
)
from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
    generate_standalone_redis_server_args,
)


def main():
    global _exclusive_hardware, _http_auth_username, _http_auth_password

    _, _, project_version = populate_with_poetry_data()
    project_name = "redis-benchmarks-spec runner(self-contained)"
    parser = create_self_contained_coordinator_args(
        get_version_string(project_name, project_version)
    )
    args = parser.parse_args()

    # Configure logging first, before any logging calls
    if args.logname is not None:
        print("Writting log to {}".format(args.logname))
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

    # Set global exclusive hardware flag
    _exclusive_hardware = args.exclusive_hardware
    if _exclusive_hardware:
        logging.info("Exclusive hardware mode enabled")

    # Set HTTP authentication credentials and start server only if credentials are provided
    _http_auth_username = args.http_auth_username
    _http_auth_password = args.http_auth_password

    if _http_auth_username and _http_auth_password:
        logging.info(
            "Starting HTTP server with authentication on port {}".format(args.http_port)
        )
        start_http_server(args.http_port)
    else:
        logging.info("HTTP server disabled - no authentication credentials provided")
    logging.info(get_version_string(project_name, project_version))
    topologies_folder = os.path.abspath(args.setups_folder + "/topologies")
    logging.info("Using topologies folder dir {}".format(topologies_folder))
    topologies_files = pathlib.Path(topologies_folder).glob("*.yml")
    topologies_files = [str(x) for x in topologies_files]
    logging.info(
        "Reading topologies specifications from: {}".format(
            " ".join([str(x) for x in topologies_files])
        )
    )
    topologies_map = get_topologies(topologies_files[0])
    testsuite_spec_files = extract_testsuites(args)

    logging.info(
        "Reading event streams from: {}:{} with user {}".format(
            args.event_stream_host, args.event_stream_port, args.event_stream_user
        )
    )
    try:
        gh_event_conn = redis.StrictRedis(
            host=args.event_stream_host,
            port=args.event_stream_port,
            decode_responses=False,  # dont decode due to binary archives
            password=args.event_stream_pass,
            username=args.event_stream_user,
            health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
            socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
            socket_keepalive=True,
        )
        gh_event_conn.ping()
    except redis.exceptions.ConnectionError as e:
        logging.error(
            "Unable to connect to redis available at: {}:{} to read the event streams".format(
                args.event_stream_host, args.event_stream_port
            )
        )
        logging.error("Error message {}".format(e.__str__()))
        exit(1)
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
        except redis.exceptions.ConnectionError as e:
            logging.error(
                "Unable to connect to redis available at: {}:{}".format(
                    args.datasink_redistimeseries_host,
                    args.datasink_redistimeseries_port,
                )
            )
            logging.error("Error message {}".format(e.__str__()))
            exit(1)

    logging.info("checking build spec requirements")
    running_platform = args.platform_name
    build_runners_consumer_group_create(gh_event_conn, running_platform, args.arch)

    # Clear pending messages and reset consumer group position by default (unless explicitly skipped)
    if not args.skip_clear_pending_on_startup:
        consumer_pos = args.consumer_pos
        logging.info(
            "Clearing pending messages and resetting consumer group position on startup (default behavior)"
        )
        clear_pending_messages_for_consumer(
            gh_event_conn, running_platform, consumer_pos, args.arch
        )
        reset_consumer_group_to_latest(gh_event_conn, running_platform, args.arch)
    else:
        logging.info(
            "Skipping pending message cleanup and consumer group reset as requested"
        )

    stream_id = None
    docker_client = docker.from_env()
    home = str(Path.home())
    cpuset_start_pos = args.cpuset_start_pos
    logging.info("Start CPU pinning at position {}".format(cpuset_start_pos))
    redis_proc_start_port = args.redis_proc_start_port
    logging.info("Redis Processes start port: {}".format(redis_proc_start_port))

    priority_lower_limit = args.tests_priority_lower_limit
    priority_upper_limit = args.tests_priority_upper_limit

    logging.info(
        f"Using priority for test filters [{priority_lower_limit},{priority_upper_limit}]"
    )

    default_baseline_branch, default_metrics_str = extract_default_branch_and_metric(
        args.defaults_filename
    )

    # TODO: confirm we do have enough cores to run the spec
    # availabe_cpus = args.cpu_count
    datasink_push_results_redistimeseries = args.datasink_push_results_redistimeseries
    grafana_profile_dashboard = args.grafana_profile_dashboard

    defaults_filename = args.defaults_filename
    get_defaults_result = get_defaults(defaults_filename)
    # Handle variable number of return values from get_defaults
    if len(get_defaults_result) >= 3:
        default_metrics = get_defaults_result[2]
    else:
        default_metrics = []
        logging.warning(
            "get_defaults returned fewer values than expected, using empty default_metrics"
        )

    # Consumer id
    consumer_pos = args.consumer_pos
    logging.info("Consumer pos {}".format(consumer_pos))

    # Arch
    arch = args.arch
    logging.info("Running for arch: {}".format(arch))

    # Github token
    github_token = args.github_token
    if github_token is not None:
        logging.info("Detected GITHUB token. will push PR comments with updates")

    # Docker air gap usage
    docker_air_gap = args.docker_air_gap
    if docker_air_gap:
        logging.info(
            "Using docker in an air-gapped way. Restoring running images from redis keys."
        )

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
    logging.info("Entering blocking read waiting for work.")
    if stream_id is None:
        stream_id = args.consumer_start_id
    while True:
        _, stream_id, _, _ = self_contained_coordinator_blocking_read(
            gh_event_conn,
            datasink_push_results_redistimeseries,
            docker_client,
            home,
            stream_id,
            datasink_conn,
            testsuite_spec_files,
            topologies_map,
            running_platform,
            profilers_enabled,
            profilers_list,
            grafana_profile_dashboard,
            cpuset_start_pos,
            redis_proc_start_port,
            consumer_pos,
            docker_air_gap,
            override_memtier_test_time,
            default_metrics,
            arch,
            github_token,
            priority_lower_limit,
            priority_upper_limit,
            default_baseline_branch,
            default_metrics_str,
        )


def check_health(container):
    logging.info(container.attrs["State"])
    health_status = container.attrs["State"].get("Health", {}).get("Status")
    return health_status


def self_contained_coordinator_blocking_read(
    github_event_conn,
    datasink_push_results_redistimeseries,
    docker_client,
    home,
    stream_id,
    datasink_conn,
    testsuite_spec_files,
    topologies_map,
    platform_name,
    profilers_enabled,
    profilers_list,
    grafana_profile_dashboard="",
    cpuset_start_pos=0,
    redis_proc_start_port=6379,
    consumer_pos=1,
    docker_air_gap=False,
    override_test_time=1,
    default_metrics=None,
    arch="amd64",
    github_token=None,
    priority_lower_limit=0,
    priority_upper_limit=10000,
    default_baseline_branch="unstable",
    default_metrics_str="ALL_STATS.Totals.Ops/sec",
    docker_keep_env=False,
    restore_build_artifacts_default=True,
):
    num_process_streams = 0
    num_process_test_suites = 0
    overall_result = False
    consumer_name = "{}-self-contained-proc#{}".format(
        get_runners_consumer_group_name(platform_name), consumer_pos
    )
    logging.info(
        "Consuming from group {}. Consumer id {}".format(
            get_runners_consumer_group_name(platform_name), consumer_name
        )
    )
    # Use architecture-specific stream
    arch_specific_stream = get_arch_specific_stream_name(arch)
    logging.info(
        f"Reading work from architecture-specific stream: {arch_specific_stream}"
    )
    newTestInfo = github_event_conn.xreadgroup(
        get_runners_consumer_group_name(platform_name),
        consumer_name,
        {arch_specific_stream: stream_id},
        count=1,
        block=0,
    )
    logging.info(f"New test info: {newTestInfo}")
    if len(newTestInfo[0]) < 2 or len(newTestInfo[0][1]) < 1:
        stream_id = ">"
    else:
        # Create args object with topology parameter
        class Args:
            def __init__(self):
                self.topology = ""

        args = Args()

        (
            stream_id,
            overall_result,
            total_test_suite_runs,
        ) = process_self_contained_coordinator_stream(
            github_event_conn,
            datasink_push_results_redistimeseries,
            docker_client,
            home,
            newTestInfo,
            datasink_conn,
            testsuite_spec_files,
            topologies_map,
            platform_name,
            profilers_enabled,
            profilers_list,
            grafana_profile_dashboard,
            cpuset_start_pos,
            redis_proc_start_port,
            docker_air_gap,
            "defaults.yml",
            override_test_time,
            default_metrics,
            arch,
            github_token,
            priority_lower_limit,
            priority_upper_limit,
            default_baseline_branch,
            default_metrics_str,
            docker_keep_env,
            restore_build_artifacts_default,
            args,
        )
        num_process_streams = num_process_streams + 1
        num_process_test_suites = num_process_test_suites + total_test_suite_runs

        # Always acknowledge the message, even if it was filtered out
        arch_specific_stream = get_arch_specific_stream_name(arch)
        ack_reply = github_event_conn.xack(
            arch_specific_stream,
            get_runners_consumer_group_name(platform_name),
            stream_id,
        )
        if type(ack_reply) == bytes:
            ack_reply = ack_reply.decode()
        if ack_reply == "1" or ack_reply == 1:
            if overall_result is True:
                logging.info(
                    "Successfully acknowledged BENCHMARK variation stream with id {} (processed).".format(
                        stream_id
                    )
                )
            else:
                logging.info(
                    "Successfully acknowledged BENCHMARK variation stream with id {} (filtered/skipped).".format(
                        stream_id
                    )
                )
        else:
            logging.error(
                "Unable to acknowledge build variation stream with id {}. XACK reply {}".format(
                    stream_id, ack_reply
                )
            )
    return overall_result, stream_id, num_process_streams, num_process_test_suites


#
# def prepare_memtier_benchmark_parameters(
#     clientconfig,
#     full_benchmark_path,
#     port,
#     server,
#     local_benchmark_output_filename,
#     oss_cluster_api_enabled,
# ):
#     benchmark_command = [
#         full_benchmark_path,
#         "--port",
#         "{}".format(port),
#         "--server",
#         "{}".format(server),
#         "--json-out-file",
#         local_benchmark_output_filename,
#     ]
#     if oss_cluster_api_enabled is True:
#         benchmark_command.append("--cluster-mode")
#     benchmark_command_str = " ".join(benchmark_command)
#     if "arguments" in clientconfig:
#         benchmark_command_str = benchmark_command_str + " " + clientconfig["arguments"]
#
#     return None, benchmark_command_str


def process_self_contained_coordinator_stream(
    github_event_conn,
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
    grafana_profile_dashboard="",
    cpuset_start_pos=0,
    redis_proc_start_port=6379,
    default_docker_air_gap=False,
    defaults_filename="defaults.yml",
    override_test_time=0,
    default_metrics=[],
    arch="amd64",
    github_token=None,
    priority_lower_limit=0,
    priority_upper_limit=10000,
    default_baseline_branch="unstable",
    default_metrics_str="ALL_STATS.Totals.Ops/sec",
    docker_keep_env=False,
    restore_build_artifacts_default=True,
    args=None,
    redis_password="redis_coordinator_password_2024",
):
    stream_id = "n/a"
    overall_result = False
    total_test_suite_runs = 0
    # github updates
    is_actionable_pr = False
    contains_benchmark_run_comment = False
    github_pr = None
    old_benchmark_run_comment_body = ""
    pr_link = ""
    regression_comment = None
    pull_request = None
    auto_approve_github = True
    # defaults
    default_github_org = "redis"
    default_github_repo = "redis"
    restore_build_artifacts = restore_build_artifacts_default

    try:
        stream_id, testDetails = newTestInfo[0][1][0]
        stream_id = stream_id.decode()
        logging.info("Received work . Stream id {}.".format(stream_id))

        if b"run_image" in testDetails:
            (
                build_variant_name,
                metadata,
                build_artifacts,
                git_hash,
                git_branch,
                git_version,
                run_image,
                use_git_timestamp,
                git_timestamp_ms,
                run_arch,
            ) = extract_build_info_from_streamdata(testDetails)

            # Check if this work should be ignored due to flush
            global _flush_timestamp
            if (
                _flush_timestamp is not None
                and use_git_timestamp
                and git_timestamp_ms is not None
            ):
                # Convert flush timestamp to milliseconds for comparison
                flush_timestamp_ms = int(_flush_timestamp.timestamp() * 1000)
                if git_timestamp_ms < flush_timestamp_ms:
                    logging.info(
                        f"Ignoring work with git_timestamp_ms {git_timestamp_ms} "
                        f"(before flush timestamp {flush_timestamp_ms}). Stream id: {stream_id}"
                    )
                    return stream_id, False, 0

            tf_github_org = default_github_org
            if b"github_org" in testDetails:
                tf_github_org = testDetails[b"github_org"].decode()
                logging.info(
                    f"detected a github_org definition on the streamdata: {tf_github_org}. Overriding the default one: {default_github_org}"
                )
            tf_github_repo = default_github_repo
            if b"github_repo" in testDetails:
                tf_github_repo = testDetails[b"github_repo"].decode()
                logging.info(
                    f"detected a github_org definition on the streamdata: {tf_github_repo}. Overriding the default one: {default_github_repo}"
                )

            mnt_point = "/mnt/redis/"
            if b"mnt_point" in testDetails:
                mnt_point = testDetails[b"mnt_point"].decode()
                logging.info(
                    f"detected a mnt_point definition on the streamdata: {mnt_point}."
                )

            executable = f"{mnt_point}redis-server"
            if b"executable" in testDetails:
                executable = testDetails[b"executable"].decode()
                logging.info(
                    f"detected a executable definition on the streamdata: {executable}."
                )

            server_name = "redis"
            if b"server_name" in testDetails:
                server_name = testDetails[b"server_name"].decode()
                logging.info(
                    f"detected a server_name definition on the streamdata: {server_name}."
                )
                new_executable = f"{mnt_point}{server_name}-server"
                logging.info(
                    f"changing executable from {executable} to {new_executable}"
                )
                executable = new_executable

            if b"restore_build_artifacts" in testDetails:
                restore_build_artifacts = bool(
                    testDetails[b"restore_build_artifacts"].decode()
                )
                logging.info(
                    f"detected a restore_build_artifacts config {restore_build_artifacts} overriding the default just for this test"
                )

            test_docker_air_gap = default_docker_air_gap
            # check if we override the docker air gap on this test details
            if b"docker_air_gap" in testDetails:
                test_docker_air_gap = bool(testDetails[b"docker_air_gap"].decode())
                logging.info(
                    f"detected a docker air gap config {test_docker_air_gap} overriding the default of {default_docker_air_gap} just for this test"
                )

            if b"priority_upper_limit" in testDetails:
                stream_priority_upper_limit = int(
                    testDetails[b"priority_upper_limit"].decode()
                )
                logging.info(
                    f"detected a priority_upper_limit definition on the streamdata {stream_priority_upper_limit}. will replace the default upper limit of {priority_upper_limit}"
                )
                priority_upper_limit = stream_priority_upper_limit

            if b"priority_lower_limit" in testDetails:
                stream_priority_lower_limit = int(
                    testDetails[b"priority_lower_limit"].decode()
                )
                logging.info(
                    f"detected a priority_lower_limit definition on the streamdata {stream_priority_lower_limit}. will replace the default lower limit of {priority_lower_limit}"
                )
                priority_lower_limit = stream_priority_lower_limit

            if b"pull_request" in testDetails:
                pull_request = testDetails[b"pull_request"].decode()
                logging.info(
                    f"detected a pull_request definition on the streamdata {pull_request}"
                )
                verbose = True
                fn = check_benchmark_running_comment
                (
                    contains_benchmark_run_comment,
                    github_pr,
                    is_actionable_pr,
                    old_benchmark_run_comment_body,
                    pr_link,
                    benchmark_run_comment,
                ) = check_github_available_and_actionable(
                    fn, github_token, pull_request, "redis", "redis", verbose
                )

            tests_regexp = ".*"
            if b"tests_regexp" in testDetails:
                tests_regexp = testDetails[b"tests_regexp"].decode()
                logging.info(
                    f"detected a regexp definition on the streamdata {tests_regexp}"
                )

            command_groups_regexp = None
            if b"tests_groups_regexp" in testDetails:
                command_groups_regexp = testDetails[b"tests_groups_regexp"].decode()
                logging.info(
                    f"detected a command groups regexp definition on the streamdata {command_groups_regexp}"
                )

            command_regexp = None
            if b"command_regexp" in testDetails:
                command_regexp = testDetails[b"command_regexp"].decode()
                logging.info(
                    f"detected a command regexp definition on the streamdata {command_regexp}"
                )

            skip_test = False
            if b"platform" in testDetails:
                platform = testDetails[b"platform"]
                # Decode bytes to string for proper comparison
                platform_str = (
                    platform.decode() if isinstance(platform, bytes) else platform
                )
                if running_platform != platform_str:
                    skip_test = True
                    logging.info(
                        "skipping stream_id {} given plaform {}!={}".format(
                            stream_id, running_platform, platform_str
                        )
                    )

            if run_arch != arch:
                skip_test = True
                logging.info(
                    "skipping stream_id {} given arch {}!={}".format(
                        stream_id, run_arch, arch
                    )
                )

            if skip_test is False:
                overall_result = True
                profiler_dashboard_links = []
                if test_docker_air_gap:
                    airgap_key = "docker:air-gap:{}".format(run_image)
                    logging.info(
                        "Restoring docker image: {} from {}".format(
                            run_image, airgap_key
                        )
                    )
                    airgap_docker_image_bin = github_event_conn.get(airgap_key)
                    images_loaded = docker_client.images.load(airgap_docker_image_bin)
                    logging.info("Successfully loaded images {}".format(images_loaded))

                stream_time_ms = stream_id.split("-")[0]
                zset_running_platform_benchmarks = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{running_platform}:zset"
                res = github_event_conn.zadd(
                    zset_running_platform_benchmarks,
                    {stream_id: stream_time_ms},
                )
                logging.info(
                    f"Added stream with id {stream_id} to zset {zset_running_platform_benchmarks}. res={res}"
                )

                stream_test_list_pending = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{running_platform}:tests_pending"
                stream_test_list_running = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{running_platform}:tests_running"
                stream_test_list_failed = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{running_platform}:tests_failed"
                stream_test_list_completed = f"ci.benchmarks.redis/ci/redis/redis:benchmarks:{stream_id}:{running_platform}:tests_completed"

                filtered_test_files = filter_test_files(
                    defaults_filename,
                    priority_lower_limit,
                    priority_upper_limit,
                    tests_regexp,
                    testsuite_spec_files,
                    command_groups_regexp,
                    command_regexp,
                )

                logging.info(
                    f"Adding {len(filtered_test_files)} tests to pending test list"
                )

                # Use pipeline for efficient bulk operations
                pipeline = github_event_conn.pipeline()
                test_names_added = []

                for test_file in filtered_test_files:
                    with open(test_file, "r") as stream:
                        (
                            _,
                            benchmark_config,
                            test_name,
                        ) = get_final_benchmark_config(None, None, stream, "")
                    pipeline.lpush(stream_test_list_pending, test_name)
                    test_names_added.append(test_name)
                    logging.debug(
                        f"Queued test named {test_name} for addition to pending test list"
                    )

                # Set expiration and execute pipeline
                pipeline.expire(stream_test_list_pending, REDIS_BINS_EXPIRE_SECS)
                pipeline.execute()

                logging.info(
                    f"Successfully added {len(test_names_added)} tests to pending test list in key {stream_test_list_pending}"
                )

                pending_tests = len(filtered_test_files)
                failed_tests = 0
                benchmark_suite_start_datetime = datetime.datetime.utcnow()
                # update on github if needed
                if is_actionable_pr:
                    comment_body = generate_benchmark_started_pr_comment(
                        stream_id,
                        pending_tests,
                        len(filtered_test_files),
                        failed_tests,
                        benchmark_suite_start_datetime,
                        0,
                    )
                    if contains_benchmark_run_comment:
                        update_comment_if_needed(
                            auto_approve_github,
                            comment_body,
                            old_benchmark_run_comment_body,
                            benchmark_run_comment,
                            verbose,
                        )
                    else:
                        benchmark_run_comment = create_new_pr_comment(
                            auto_approve_github, comment_body, github_pr, pr_link
                        )

                for test_file in filtered_test_files:
                    # Check if queue reset was requested
                    global _reset_queue_requested
                    if _reset_queue_requested:
                        logging.info(
                            "Queue reset requested. Skipping remaining tests and clearing queues."
                        )
                        # Clear all pending tests from the queue
                        github_event_conn.delete(stream_test_list_pending)
                        github_event_conn.delete(stream_test_list_running)
                        logging.info("Cleared pending and running test queues")
                        _reset_queue_requested = False
                        break

                    # Clean up system processes if in exclusive hardware mode
                    cleanup_system_processes()

                    redis_containers = []
                    client_containers = []
                    with open(test_file, "r") as stream:
                        (
                            _,
                            benchmark_config,
                            test_name,
                        ) = get_final_benchmark_config(None, None, stream, "")
                        github_event_conn.lrem(stream_test_list_pending, 1, test_name)
                        github_event_conn.lpush(stream_test_list_running, test_name)
                        github_event_conn.expire(
                            stream_test_list_running, REDIS_BINS_EXPIRE_SECS
                        )
                        logging.debug(
                            f"Added test named {test_name} to the running test list in key {stream_test_list_running}"
                        )
                        (
                            _,
                            _,
                            redis_configuration_parameters,
                            _,
                            _,
                        ) = extract_redis_dbconfig_parameters(
                            benchmark_config, "dbconfig"
                        )
                        build_variants = extract_build_variant_variations(
                            benchmark_config
                        )
                        if build_variants is not None:
                            logging.info("Detected build variant filter")
                            if build_variant_name not in build_variants:
                                logging.info(
                                    "Skipping {} given it's not part of build-variants for this test-suite {}".format(
                                        build_variant_name, build_variants
                                    )
                                )
                                continue
                            else:
                                logging.info(
                                    "Running build variant {} given it's present on the build-variants spec {}".format(
                                        build_variant_name, build_variants
                                    )
                                )
                        for topology_spec_name in benchmark_config["redis-topologies"]:
                            setup_name = topology_spec_name
                            setup_type = "oss-standalone"

                            # Filter by topology if specified
                            if (
                                args is not None
                                and args.topology
                                and topology_spec_name != args.topology
                            ):
                                logging.info(
                                    f"Skipping topology {topology_spec_name} as it doesn't match the requested topology {args.topology}"
                                )
                                continue

                            if topology_spec_name in topologies_map:
                                topology_spec = topologies_map[topology_spec_name]
                                setup_type = topology_spec["type"]
                            logging.info(
                                f"Running topology named {topology_spec_name} of type {setup_type}"
                            )
                            test_result = False
                            redis_container = None
                            try:
                                current_cpu_pos = cpuset_start_pos
                                ceil_db_cpu_limit = extract_db_cpu_limit(
                                    topologies_map, topology_spec_name
                                )
                                redis_arguments = (
                                    extract_redis_configuration_from_topology(
                                        topologies_map, topology_spec_name
                                    )
                                )
                                temporary_dir = tempfile.mkdtemp(dir=home)
                                temporary_dir_client = tempfile.mkdtemp(dir=home)
                                logging.info(
                                    "Using local temporary dir to persist redis build artifacts. Path: {}".format(
                                        temporary_dir
                                    )
                                )
                                logging.info(
                                    "Using local temporary dir to persist client output files. Path: {}".format(
                                        temporary_dir_client
                                    )
                                )

                                tf_triggering_env = "ci"
                                github_actor = "{}-{}".format(
                                    tf_triggering_env, running_platform
                                )
                                dso = server_name
                                profilers_artifacts_matrix = []

                                collection_summary_str = ""
                                if profilers_enabled:
                                    collection_summary_str = (
                                        local_profilers_platform_checks(
                                            dso,
                                            github_actor,
                                            git_branch,
                                            tf_github_repo,
                                            git_hash,
                                        )
                                    )
                                    logging.info(
                                        "Using the following collection summary string for profiler description: {}".format(
                                            collection_summary_str
                                        )
                                    )
                                if restore_build_artifacts:
                                    restore_build_artifacts_from_test_details(
                                        build_artifacts,
                                        github_event_conn,
                                        temporary_dir,
                                        testDetails,
                                    )

                                command = generate_standalone_redis_server_args(
                                    executable,
                                    redis_proc_start_port,
                                    mnt_point,
                                    redis_configuration_parameters,
                                    redis_arguments,
                                    redis_password,
                                )
                                command_str = " ".join(command)
                                db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(
                                    ceil_db_cpu_limit, current_cpu_pos
                                )
                                redis_container = start_redis_container(
                                    command_str,
                                    db_cpuset_cpus,
                                    docker_client,
                                    mnt_point,
                                    redis_containers,
                                    run_image,
                                    temporary_dir,
                                )

                                r = redis.StrictRedis(
                                    port=redis_proc_start_port, password=redis_password
                                )
                                r.ping()
                                redis_conns = [r]
                                reset_commandstats(redis_conns)
                                redis_pids = []
                                redis_info = r.info()
                                first_redis_pid = redis_info.get("process_id")
                                if first_redis_pid is None:
                                    logging.warning(
                                        "Redis process_id not found in INFO command"
                                    )
                                    first_redis_pid = "unknown"
                                if git_hash is None and "redis_git_sha1" in redis_info:
                                    git_hash = redis_info["redis_git_sha1"]
                                    if (
                                        git_hash == "" or git_hash == 0
                                    ) and "redis_build_id" in redis_info:
                                        git_hash = redis_info["redis_build_id"]
                                    logging.info(
                                        f"Given git_hash was None, we've collected that info from the server reply. git_hash={git_hash}"
                                    )

                                server_version_keyname = f"{server_name}_version"
                                if (
                                    git_version is None
                                    and server_version_keyname in redis_info
                                ):
                                    git_version = redis_info[server_version_keyname]
                                    logging.info(
                                        f"Given git_version was None, we've collected that info from the server reply key named {server_version_keyname}. git_version={git_version}"
                                    )
                                redis_pids.append(first_redis_pid)
                                ceil_client_cpu_limit = extract_client_cpu_limit(
                                    benchmark_config
                                )
                                (
                                    client_cpuset_cpus,
                                    current_cpu_pos,
                                ) = generate_cpuset_cpus(
                                    ceil_client_cpu_limit, current_cpu_pos
                                )
                                client_mnt_point = "/mnt/client/"
                                benchmark_tool_workdir = client_mnt_point

                                if "preload_tool" in benchmark_config["dbconfig"]:
                                    data_prepopulation_step(
                                        benchmark_config,
                                        benchmark_tool_workdir,
                                        client_cpuset_cpus,
                                        docker_client,
                                        git_hash,
                                        redis_proc_start_port,
                                        temporary_dir,
                                        test_name,
                                        redis_password,
                                    )

                                execute_init_commands(
                                    benchmark_config, r, dbconfig_keyname="dbconfig"
                                )

                                benchmark_tool = extract_client_tool(benchmark_config)
                                # backwards compatible
                                if benchmark_tool is None:
                                    benchmark_tool = "redis-benchmark"
                                if benchmark_tool == "vector_db_benchmark":
                                    full_benchmark_path = "python /code/run.py"
                                else:
                                    full_benchmark_path = "/usr/local/bin/{}".format(
                                        benchmark_tool
                                    )

                                # setup the benchmark
                                (
                                    start_time,
                                    start_time_ms,
                                    start_time_str,
                                ) = get_start_time_vars()
                                local_benchmark_output_filename = (
                                    get_local_run_full_filename(
                                        start_time_str,
                                        git_hash,
                                        test_name,
                                        setup_name,
                                    )
                                )
                                logging.info(
                                    "Will store benchmark json output to local file {}".format(
                                        local_benchmark_output_filename
                                    )
                                )
                                if "memtier_benchmark" in benchmark_tool:
                                    # prepare the benchmark command
                                    (
                                        _,
                                        benchmark_command_str,
                                        arbitrary_command,
                                    ) = prepare_memtier_benchmark_parameters(
                                        benchmark_config["clientconfig"],
                                        full_benchmark_path,
                                        redis_proc_start_port,
                                        "localhost",
                                        redis_password,
                                        local_benchmark_output_filename,
                                        False,
                                        False,
                                        False,
                                        None,
                                        None,
                                        None,
                                        None,
                                        override_test_time,
                                    )
                                elif "vector_db_benchmark" in benchmark_tool:
                                    (
                                        _,
                                        benchmark_command_str,
                                    ) = prepare_vector_db_benchmark_parameters(
                                        benchmark_config["clientconfig"],
                                        full_benchmark_path,
                                        redis_proc_start_port,
                                        "localhost",
                                        None,
                                        client_mnt_point,
                                    )
                                else:
                                    (
                                        benchmark_command,
                                        benchmark_command_str,
                                    ) = prepare_benchmark_parameters(
                                        benchmark_config,
                                        full_benchmark_path,
                                        redis_proc_start_port,
                                        "localhost",
                                        local_benchmark_output_filename,
                                        False,
                                        benchmark_tool_workdir,
                                        False,
                                    )

                                client_container_image = extract_client_container_image(
                                    benchmark_config
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

                                logging.info(
                                    "Using docker image {} as benchmark client image (cpuset={}) with the following args: {}".format(
                                        client_container_image,
                                        client_cpuset_cpus,
                                        benchmark_command_str,
                                    )
                                )
                                # run the benchmark
                                benchmark_start_time = datetime.datetime.now()

                                # Calculate container timeout
                                container_timeout = 300  # 5 minutes default
                                buffer_timeout = 60  # Default buffer

                                # Try to extract test time from command and add buffer
                                import re

                                test_time_match = re.search(
                                    r"--?test-time[=\s]+(\d+)", benchmark_command_str
                                )
                                if test_time_match:
                                    test_time = int(test_time_match.group(1))
                                    container_timeout = test_time + buffer_timeout
                                    logging.info(
                                        f"Set container timeout to {container_timeout}s (test-time: {test_time}s + {buffer_timeout}s buffer)"
                                    )
                                else:
                                    logging.info(
                                        f"Using default container timeout: {container_timeout}s"
                                    )

                                try:
                                    # Start container with detach=True to enable timeout handling
                                    container = docker_client.containers.run(
                                        image=client_container_image,
                                        volumes={
                                            temporary_dir_client: {
                                                "bind": client_mnt_point,
                                                "mode": "rw",
                                            },
                                        },
                                        auto_remove=False,  # Don't auto-remove so we can get logs if timeout
                                        privileged=True,
                                        working_dir=benchmark_tool_workdir,
                                        command=benchmark_command_str,
                                        network_mode="host",
                                        detach=True,  # Detach to enable timeout
                                        cpuset_cpus=client_cpuset_cpus,
                                    )

                                    logging.info(
                                        f"Started container {container.name} ({container.id[:12]}) with {container_timeout}s timeout"
                                    )

                                    # Wait for container with timeout
                                    try:
                                        result = container.wait(
                                            timeout=container_timeout
                                        )
                                        client_container_stdout = container.logs(
                                            stdout=True, stderr=False
                                        ).decode("utf-8")
                                        container_stderr = container.logs(
                                            stdout=False, stderr=True
                                        ).decode("utf-8")

                                        # Check exit code
                                        if result["StatusCode"] != 0:
                                            logging.error(
                                                f"Container exited with code {result['StatusCode']}"
                                            )
                                            logging.error(
                                                f"Container stderr: {container_stderr}"
                                            )
                                            raise docker.errors.ContainerError(
                                                container,
                                                result["StatusCode"],
                                                benchmark_command_str,
                                                client_container_stdout,
                                                container_stderr,
                                            )

                                        logging.info(
                                            f"Container {container.name} completed successfully"
                                        )

                                    except Exception as timeout_error:
                                        if "timeout" in str(timeout_error).lower():
                                            logging.error(
                                                f"Container {container.name} timed out after {container_timeout}s"
                                            )
                                            # Get logs before killing
                                            try:
                                                timeout_logs = container.logs(
                                                    stdout=True, stderr=True
                                                ).decode("utf-8")
                                                logging.error(
                                                    f"Container logs before timeout: {timeout_logs}"
                                                )
                                            except:
                                                pass
                                            # Kill the container
                                            container.kill()
                                            raise Exception(
                                                f"Container timed out after {container_timeout} seconds"
                                            )
                                        else:
                                            raise timeout_error
                                    finally:
                                        # Clean up container
                                        try:
                                            container.remove(force=True)
                                        except:
                                            pass
                                except docker.errors.ContainerError as e:
                                    logging.info(
                                        "stdout: {}".format(
                                            e.container.logs(stdout=True)
                                        )
                                    )
                                    logging.info(
                                        "stderr: {}".format(
                                            e.container.logs(stderr=True)
                                        )
                                    )
                                    raise e

                                benchmark_end_time = datetime.datetime.now()
                                benchmark_duration_seconds = (
                                    calculate_client_tool_duration_and_check(
                                        benchmark_end_time, benchmark_start_time
                                    )
                                )
                                logging.info(
                                    "output {}".format(client_container_stdout)
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
                                if (
                                    profilers_enabled
                                    and datasink_push_results_redistimeseries
                                ):
                                    datasink_profile_tabular_data(
                                        git_branch,
                                        tf_github_org,
                                        tf_github_repo,
                                        git_hash,
                                        overall_tabular_data_map,
                                        github_event_conn,
                                        setup_name,
                                        start_time_ms,
                                        start_time_str,
                                        test_name,
                                        tf_triggering_env,
                                    )
                                    if len(profilers_artifacts_matrix) == 0:
                                        logging.error(
                                            "No profiler artifact was retrieved"
                                        )
                                    else:
                                        profilers_artifacts = []
                                        for line in profilers_artifacts_matrix:
                                            artifact_name = line[2]
                                            s3_link = line[4]
                                            profilers_artifacts.append(
                                                {
                                                    "artifact_name": artifact_name,
                                                    "s3_link": s3_link,
                                                }
                                            )
                                        https_link = (
                                            generate_artifacts_table_grafana_redis(
                                                datasink_push_results_redistimeseries,
                                                grafana_profile_dashboard,
                                                profilers_artifacts,
                                                datasink_conn,
                                                setup_name,
                                                start_time_ms,
                                                start_time_str,
                                                test_name,
                                                tf_github_org,
                                                tf_github_repo,
                                                git_hash,
                                                git_branch,
                                            )
                                        )
                                        profiler_dashboard_links.append(
                                            [
                                                setup_name,
                                                test_name,
                                                " {} ".format(https_link),
                                            ]
                                        )
                                        logging.info(
                                            "Published new profile info for this testcase. Access it via: {}".format(
                                                https_link
                                            )
                                        )

                                        # Delete all the perf artifacts, now that they are uploaded to S3.
                                        # The .script and .script.mainthread files are not part of the artifacts_matrix and thus have to be deleted separately
                                        line = profilers_artifacts_matrix[0]
                                        logging.info(
                                            "Deleting perf file {}".format(
                                                line[3].split(".")[0]
                                                + ".out.script.mainthread"
                                            )
                                        )
                                        os.remove(
                                            line[3].split(".")[0]
                                            + ".out.script.mainthread"
                                        )
                                        logging.info(
                                            "Deleteing perf file {}".format(
                                                line[3].split(".")[0] + ".out.script"
                                            )
                                        )
                                        os.remove(line[3].split(".")[0] + ".out.script")
                                        for line in profilers_artifacts_matrix:
                                            logging.info(
                                                "Deleting perf file {}".format(line[3])
                                            )
                                            os.remove(line[3])

                                datapoint_time_ms = start_time_ms
                                if (
                                    use_git_timestamp is True
                                    and git_timestamp_ms is not None
                                ):
                                    datapoint_time_ms = git_timestamp_ms
                                if "vector_db_benchmark" in benchmark_tool:
                                    results_dict = post_process_vector_db(
                                        temporary_dir_client
                                    )

                                    # Validate benchmark metrics for vector-db-benchmark
                                    is_valid, validation_error = (
                                        validate_benchmark_metrics(
                                            results_dict,
                                            test_name,
                                            benchmark_config,
                                            default_metrics,
                                        )
                                    )
                                    if not is_valid:
                                        logging.error(
                                            f"Test {test_name} failed metric validation: {validation_error}"
                                        )
                                        test_result = False
                                        failed_tests += 1
                                        continue
                                else:
                                    post_process_benchmark_results(
                                        benchmark_tool,
                                        local_benchmark_output_filename,
                                        datapoint_time_ms,
                                        start_time_str,
                                        client_container_stdout,
                                        None,
                                    )
                                    full_result_path = local_benchmark_output_filename
                                    if "memtier_benchmark" in benchmark_tool:
                                        full_result_path = "{}/{}".format(
                                            temporary_dir_client,
                                            local_benchmark_output_filename,
                                        )
                                    logging.info(
                                        "Reading results json from {}".format(
                                            full_result_path
                                        )
                                    )

                                    with open(
                                        full_result_path,
                                        "r",
                                    ) as json_file:
                                        results_dict = json.load(json_file)

                                        # Validate benchmark metrics
                                        is_valid, validation_error = (
                                            validate_benchmark_metrics(
                                                results_dict,
                                                test_name,
                                                benchmark_config,
                                                default_metrics,
                                            )
                                        )
                                        if not is_valid:
                                            logging.error(
                                                f"Test {test_name} failed metric validation: {validation_error}"
                                            )
                                            test_result = False
                                            failed_tests += 1
                                            continue

                                        print_results_table_stdout(
                                            benchmark_config,
                                            default_metrics,
                                            results_dict,
                                            setup_type,
                                            test_name,
                                            None,
                                        )

                                dataset_load_duration_seconds = 0
                                try:
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
                                        setup_name,
                                        setup_type,
                                        test_name,
                                        tf_github_org,
                                        tf_github_repo,
                                        tf_triggering_env,
                                        topology_spec_name,
                                        default_metrics,
                                        git_hash,
                                    )
                                    r.shutdown(save=False)

                                except redis.exceptions.ConnectionError as e:
                                    logging.critical(
                                        "Some unexpected exception was caught during metric fetching. Skipping it..."
                                    )
                                    logging.critical(
                                        f"Exception type: {type(e).__name__}"
                                    )
                                    logging.critical(f"Exception message: {str(e)}")
                                    logging.critical("Traceback details:")
                                    logging.critical(traceback.format_exc())
                                    print("-" * 60)
                                    traceback.print_exc(file=sys.stdout)
                                    print("-" * 60)

                                test_result = True
                                total_test_suite_runs = total_test_suite_runs + 1

                            except Exception as e:
                                logging.critical(
                                    "Some unexpected exception was caught during local work. Failing test...."
                                )
                                logging.critical(f"Exception type: {type(e).__name__}")
                                logging.critical(f"Exception message: {str(e)}")
                                logging.critical("Traceback details:")
                                logging.critical(traceback.format_exc())
                                print("-" * 60)
                                traceback.print_exc(file=sys.stdout)
                                print("-" * 60)
                                if redis_container is not None:
                                    logging.critical("Printing redis container log....")

                                    print("-" * 60)
                                    try:
                                        print(
                                            redis_container.logs(
                                                stdout=True, stderr=True
                                            )
                                        )
                                        redis_container.stop()
                                        redis_container.remove()
                                    except docker.errors.NotFound:
                                        logging.info(
                                            "When trying to fetch logs from DB container with id {} and image {} it was already stopped".format(
                                                redis_container.id,
                                                redis_container.image,
                                            )
                                        )
                                    pass

                                    print("-" * 60)

                                # Print all log files in the temporary directories for debugging
                                logging.critical(
                                    "Printing all files in temporary directories for debugging..."
                                )
                                try:
                                    print_directory_logs(temporary_dir, "Redis server")
                                    print_directory_logs(temporary_dir_client, "Client")
                                except Exception as log_error:
                                    logging.error(
                                        f"Failed to print directory logs: {log_error}"
                                    )

                                test_result = False
                            # tear-down
                            logging.info("Tearing down setup")
                            if docker_keep_env is False:
                                for redis_container in redis_containers:
                                    try:
                                        redis_container.stop()
                                        redis_container.remove()
                                    except docker.errors.NotFound:
                                        logging.info(
                                            "When trying to stop DB container with id {} and image {} it was already stopped".format(
                                                redis_container.id,
                                                redis_container.image,
                                            )
                                        )
                                        pass

                                for redis_container in client_containers:
                                    if type(redis_container) == Container:
                                        try:
                                            redis_container.stop()
                                            redis_container.remove()
                                        except docker.errors.NotFound:
                                            logging.info(
                                                "When trying to stop Client container with id {} and image {} it was already stopped".format(
                                                    redis_container.id,
                                                    redis_container.image,
                                                )
                                            )
                                            pass

                                # Only remove temporary directories if test passed
                                if test_result:
                                    logging.info(
                                        "Test passed. Removing temporary dirs {} and {}".format(
                                            temporary_dir, temporary_dir_client
                                        )
                                    )
                                    shutil.rmtree(temporary_dir, ignore_errors=True)
                                    shutil.rmtree(
                                        temporary_dir_client, ignore_errors=True
                                    )
                                else:
                                    logging.warning(
                                        "Test failed. Preserving temporary dirs for debugging: {} and {}".format(
                                            temporary_dir, temporary_dir_client
                                        )
                                    )
                                    # Print all log files in the temporary directories for debugging
                                    print_directory_logs(temporary_dir, "Redis server")
                                    print_directory_logs(temporary_dir_client, "Client")

                            overall_result &= test_result

                    # Clean up system processes after test completion if in exclusive hardware mode
                    cleanup_system_processes()

                    github_event_conn.lrem(stream_test_list_running, 1, test_name)
                    github_event_conn.lpush(stream_test_list_completed, test_name)
                    github_event_conn.expire(
                        stream_test_list_completed, REDIS_BINS_EXPIRE_SECS
                    )
                    if test_result is False:
                        github_event_conn.lpush(stream_test_list_failed, test_name)
                        failed_tests = failed_tests + 1
                        logging.warning(
                            f"updating key {stream_test_list_failed} with the failed test: {test_name}. Total failed tests {failed_tests}."
                        )
                    pending_tests = pending_tests - 1

                    benchmark_suite_end_datetime = datetime.datetime.utcnow()
                    benchmark_suite_duration = (
                        benchmark_suite_end_datetime - benchmark_suite_start_datetime
                    )
                    benchmark_suite_duration_secs = (
                        benchmark_suite_duration.total_seconds()
                    )

                    # update on github if needed
                    if is_actionable_pr:
                        comment_body = generate_benchmark_started_pr_comment(
                            stream_id,
                            pending_tests,
                            len(filtered_test_files),
                            failed_tests,
                            benchmark_suite_start_datetime,
                            benchmark_suite_duration_secs,
                        )
                        update_comment_if_needed(
                            auto_approve_github,
                            comment_body,
                            old_benchmark_run_comment_body,
                            benchmark_run_comment,
                            verbose,
                        )
                        logging.info(
                            f"Updated github comment with latest test info {benchmark_run_comment.html_url}"
                        )

                        ###########################
                        # regression part
                        ###########################
                        fn = check_regression_comment
                        (
                            contains_regression_comment,
                            github_pr,
                            is_actionable_pr,
                            old_regression_comment_body,
                            pr_link,
                            regression_comment,
                        ) = check_github_available_and_actionable(
                            fn,
                            github_token,
                            pull_request,
                            tf_github_org,
                            tf_github_repo,
                            verbose,
                        )
                        logging.info("Preparing regression info for the data available")
                        print_improvements_only = False
                        print_regressions_only = False
                        skip_unstable = False
                        regressions_percent_lower_limit = 10.0
                        simplify_table = False
                        testname_regex = ""
                        test = ""
                        last_n_baseline = 1
                        last_n_comparison = 31
                        use_metric_context_path = False
                        baseline_tag = None
                        baseline_deployment_name = "oss-standalone"
                        comparison_deployment_name = "oss-standalone"
                        metric_name = "ALL_STATS.Totals.Ops/sec"
                        metric_mode = "higher-better"
                        to_date = datetime.datetime.utcnow()
                        from_date = to_date - datetime.timedelta(days=180)
                        baseline_branch = default_baseline_branch
                        comparison_tag = git_version
                        comparison_branch = git_branch
                        to_ts_ms = None
                        from_ts_ms = None

                        (
                            detected_regressions,
                            table_output,
                            improvement_list,
                            regressions_list,
                            total_stable,
                            total_unstable,
                            total_comparison_points,
                        ) = compute_regression_table(
                            datasink_conn,
                            tf_github_org,
                            tf_github_repo,
                            tf_triggering_env,
                            metric_name,
                            comparison_branch,
                            baseline_branch,
                            None,  # we only compare by branch on CI automation
                            None,  # we only compare by branch on CI automation
                            baseline_deployment_name,
                            comparison_deployment_name,
                            print_improvements_only,
                            print_regressions_only,
                            skip_unstable,
                            regressions_percent_lower_limit,
                            simplify_table,
                            test,
                            testname_regex,
                            verbose,
                            last_n_baseline,
                            last_n_comparison,
                            metric_mode,
                            from_date,
                            from_ts_ms,
                            to_date,
                            to_ts_ms,
                            use_metric_context_path,
                            running_platform,
                        )
                        total_regressions = len(regressions_list)
                        total_improvements = len(improvement_list)
                        auto_approve = True
                        grafana_link_base = "https://benchmarksredisio.grafana.net/d/1fWbtb7nz/experimental-oss-spec-benchmarks"
                        try:
                            prepare_regression_comment(
                                auto_approve,
                                baseline_branch,
                                baseline_tag,
                                comparison_branch,
                                comparison_tag,
                                contains_regression_comment,
                                github_pr,
                                grafana_link_base,
                                is_actionable_pr,
                                old_regression_comment_body,
                                pr_link,
                                regression_comment,
                                datasink_conn,
                                running_platform,
                                table_output,
                                tf_github_org,
                                tf_github_repo,
                                tf_triggering_env,
                                total_comparison_points,
                                total_improvements,
                                total_regressions,
                                total_stable,
                                total_unstable,
                                verbose,
                                regressions_percent_lower_limit,
                                regressions_list,
                            )
                        except Exception as e:
                            logging.error(
                                "Failed to produce regression comment but continuing... Error: {}".format(
                                    e.__str__()
                                )
                            )
                    logging.debug(
                        f"Added test named {test_name} to the completed test list in key {stream_test_list_completed}"
                    )
        else:
            logging.error("Missing run image information within received message.")

    except Exception as e:
        logging.critical(
            "Some unexpected exception was caught "
            "during local work on stream {}. Failing test....".format(stream_id)
        )
        logging.critical(f"Exception type: {type(e).__name__}")
        logging.critical(f"Exception message: {str(e)}")
        logging.critical("Traceback details:")
        logging.critical(traceback.format_exc())
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
        overall_result = False
    return stream_id, overall_result, total_test_suite_runs


def start_redis_container(
    command_str,
    db_cpuset_cpus,
    docker_client,
    mnt_point,
    redis_containers,
    run_image,
    temporary_dir,
    auto_remove=False,
):
    logging.info(
        "Running redis-server on docker image {} (cpuset={}) with the following args: {}".format(
            run_image, db_cpuset_cpus, command_str
        )
    )
    volumes = {}
    working_dir = "/"
    if mnt_point != "":
        volumes = {
            temporary_dir: {
                "bind": mnt_point,
                "mode": "rw",
            },
        }
        logging.info(f"setting volume as follow: {volumes}. working_dir={mnt_point}")
        working_dir = mnt_point
    redis_container = docker_client.containers.run(
        image=run_image,
        volumes=volumes,
        auto_remove=auto_remove,
        privileged=True,
        working_dir=mnt_point,
        command=command_str,
        network_mode="host",
        detach=True,
        cpuset_cpus=db_cpuset_cpus,
        pid_mode="host",
        publish_all_ports=True,
    )
    time.sleep(5)
    redis_containers.append(redis_container)
    return redis_container


def filter_test_files(
    defaults_filename,
    priority_lower_limit,
    priority_upper_limit,
    tests_regexp,
    testsuite_spec_files,
    command_groups_regexp=None,
    command_regexp=None,
):
    filtered_test_files = []
    for test_file in testsuite_spec_files:
        if defaults_filename in test_file:
            continue

        if tests_regexp != ".*":
            logging.debug(
                "Filtering all tests via a regular expression: {}".format(tests_regexp)
            )
            tags_regex_string = re.compile(tests_regexp)

            match_obj = re.search(tags_regex_string, test_file)
            if match_obj is None:
                logging.debug(
                    "Skipping {} given it does not match regex {}".format(
                        test_file, tests_regexp
                    )
                )
                continue

        with open(test_file, "r") as stream:
            (
                result,
                benchmark_config,
                test_name,
            ) = get_final_benchmark_config(None, None, stream, "")
            if result is False:
                logging.error(
                    "Skipping {} given there were errors while calling get_final_benchmark_config()".format(
                        test_file
                    )
                )
                continue

            if command_groups_regexp is not None:
                logging.debug(
                    "Filtering all test command groups via a regular expression: {}".format(
                        command_groups_regexp
                    )
                )
                if "tested-groups" in benchmark_config:
                    command_groups = benchmark_config["tested-groups"]
                    logging.debug(
                        f"The file {test_file} (test name = {test_name}) contains the following groups: {command_groups}"
                    )
                    groups_regex_string = re.compile(command_groups_regexp)
                    found = False
                    for command_group in command_groups:
                        match_obj = re.search(groups_regex_string, command_group)
                        if match_obj is not None:
                            found = True
                            logging.debug(f"found the command group {command_group}")
                    if found is False:
                        logging.info(
                            f"Skipping {test_file} given the following groups: {command_groups} does not match command group regex {command_groups_regexp}"
                        )
                        continue
                else:
                    logging.debug(
                        f"The file {test_file} (test name = {test_name}) does not contain the property 'tested-groups'. Cannot filter based uppon groups..."
                    )

            # Filter by command regex if specified
            if command_regexp is not None and command_regexp != ".*":
                if "tested-commands" in benchmark_config:
                    tested_commands = benchmark_config["tested-commands"]
                    command_regex_compiled = re.compile(command_regexp, re.IGNORECASE)
                    found = False
                    for command in tested_commands:
                        if re.search(command_regex_compiled, command):
                            found = True
                            logging.info(
                                f"found the command {command} matching regex {command_regexp}"
                            )
                            break
                    if found is False:
                        logging.info(
                            f"Skipping {test_file} given the following commands: {tested_commands} does not match command regex {command_regexp}"
                        )
                        continue
                else:
                    logging.warning(
                        f"The file {test_file} (test name = {test_name}) does not contain the property 'tested-commands'. Cannot filter based upon commands..."
                    )

            if "priority" in benchmark_config:
                priority = benchmark_config["priority"]

                if priority is not None:
                    if priority > priority_upper_limit:
                        logging.warning(
                            "Skipping test {} giving the priority limit ({}) is above the priority value ({})".format(
                                test_name, priority_upper_limit, priority
                            )
                        )

                        continue
                    if priority < priority_lower_limit:
                        logging.warning(
                            "Skipping test {} giving the priority limit ({}) is bellow the priority value ({})".format(
                                test_name, priority_lower_limit, priority
                            )
                        )

                        continue
                    logging.info(
                        "Test {} priority ({}) is within the priority limit [{},{}]".format(
                            test_name,
                            priority,
                            priority_lower_limit,
                            priority_upper_limit,
                        )
                    )
        filtered_test_files.append(test_file)
    return filtered_test_files


def data_prepopulation_step(
    benchmark_config,
    benchmark_tool_workdir,
    client_cpuset_cpus,
    docker_client,
    git_hash,
    port,
    temporary_dir,
    test_name,
    redis_password,
):
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
    full_benchmark_path = "/usr/local/bin/{}".format(preload_tool)
    client_mnt_point = "/mnt/client/"
    if "memtier_benchmark" in preload_tool:
        (
            _,
            preload_command_str,
            _,
        ) = prepare_memtier_benchmark_parameters(
            benchmark_config["dbconfig"]["preload_tool"],
            full_benchmark_path,
            port,
            "localhost",
            redis_password,
            local_benchmark_output_filename,
            False,
        )

        logging.info(
            "Using docker image {} as benchmark PRELOAD image (cpuset={}) with the following args: {}".format(
                preload_image,
                client_cpuset_cpus,
                preload_command_str,
            )
        )
        # run the benchmark
        preload_start_time = datetime.datetime.now()

        # Set preload timeout (preload can take longer than benchmarks)
        preload_timeout = 1800  # 30 minutes default for data loading
        logging.info(f"Starting preload container with {preload_timeout}s timeout")

        try:
            # Start container with detach=True to enable timeout handling
            container = docker_client.containers.run(
                image=preload_image,
                volumes={
                    temporary_dir: {
                        "bind": client_mnt_point,
                        "mode": "rw",
                    },
                },
                auto_remove=False,  # Don't auto-remove so we can get logs if timeout
                privileged=True,
                working_dir=benchmark_tool_workdir,
                command=preload_command_str,
                network_mode="host",
                detach=True,  # Detach to enable timeout
                cpuset_cpus=client_cpuset_cpus,
            )

            logging.info(
                f"Started preload container {container.name} ({container.id[:12]}) with {preload_timeout}s timeout"
            )

            # Wait for container with timeout
            try:
                result = container.wait(timeout=preload_timeout)
                client_container_stdout = container.logs(
                    stdout=True, stderr=False
                ).decode("utf-8")
                container_stderr = container.logs(stdout=False, stderr=True).decode(
                    "utf-8"
                )

                # Check exit code
                if result["StatusCode"] != 0:
                    logging.error(
                        f"Preload container exited with code {result['StatusCode']}"
                    )
                    logging.error(f"Preload container stderr: {container_stderr}")
                    raise docker.errors.ContainerError(
                        container,
                        result["StatusCode"],
                        preload_command_str,
                        client_container_stdout,
                        container_stderr,
                    )

                logging.info(
                    f"Preload container {container.name} completed successfully"
                )

            except Exception as timeout_error:
                if "timeout" in str(timeout_error).lower():
                    logging.error(
                        f"Preload container {container.name} timed out after {preload_timeout}s"
                    )
                    # Get logs before killing
                    try:
                        timeout_logs = container.logs(stdout=True, stderr=True).decode(
                            "utf-8"
                        )
                        logging.error(
                            f"Preload container logs before timeout: {timeout_logs}"
                        )
                    except:
                        pass
                    # Kill the container
                    container.kill()
                    raise Exception(
                        f"Preload container timed out after {preload_timeout} seconds"
                    )
                else:
                    raise timeout_error
            finally:
                # Clean up container
                try:
                    container.remove(force=True)
                except:
                    pass
        except Exception as e:
            logging.error(f"Preload container failed: {e}")
            raise e

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


def get_benchmark_specs(testsuites_folder):
    files = pathlib.Path(testsuites_folder).glob("*.yml")
    files = [str(x) for x in files]
    logging.info(
        "Running all specified benchmarks: {}".format(" ".join([str(x) for x in files]))
    )
    return files
