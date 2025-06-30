import datetime
import json
import logging
import math
import os
import shutil
import sys
import tempfile
import traceback
from pathlib import Path
import re
import tqdm
import docker
import redis
from docker.models.containers import Container
from pytablewriter import CsvTableWriter, MarkdownTableWriter
from redisbench_admin.profilers.profilers_local import (
    check_compatible_system_and_kernel_and_prepare_profile,
    local_profilers_platform_checks,
    profilers_start_if_required,
    profilers_stop_if_required,
)
from redisbench_admin.run.common import (
    get_start_time_vars,
    merge_default_and_config_metrics,
    prepare_benchmark_parameters,
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
                (
                    _,
                    benchmark_command_str,
                    arbitrary_command,
                ) = prepare_memtier_benchmark_parameters(
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
            buffer_timeout = (
                args.container_timeout_buffer
            )  # Configurable buffer from command line
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
            import os

            container = docker_client.containers.run(
                image=client_image,
                volumes={
                    temporary_dir_client: {
                        "bind": client_mnt_point,
                        "mode": "rw",
                    },
                },
                auto_remove=False,
                privileged=True,
                working_dir=benchmark_tool_workdir,
                command=benchmark_command_str,
                network_mode="host",
                detach=True,
                cpuset_cpus=client_cpuset_cpus,
                user=f"{os.getuid()}:{os.getgid()}",  # Run as current user to fix permissions
            )

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
        import json
        import os

        aggregated_json = {}
        memtier_json = None
        pubsub_json = None

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

        # Merge JSON outputs from both tools
        if memtier_json and pubsub_json:
            # Use memtier as base and add pubsub metrics
            aggregated_json = memtier_json.copy()
            # Add pubsub metrics to the aggregated result
            aggregated_json.update(pubsub_json)
            aggregated_stdout = json.dumps(aggregated_json, indent=2)
            logging.info(
                "Using merged JSON results from memtier and pubsub-sub-bench clients"
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
        else:
            # Fall back to concatenated stdout
            aggregated_stdout = "\n".join([r["stdout"] for r in successful_results])
            logging.warning(
                "No JSON results found, falling back to concatenated stdout"
            )

    return aggregated_stdout, results


def main():
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
    dry_run = args.dry_run
    dry_run_include_preload = args.dry_run_include_preload
    defaults_filename = args.defaults_filename
    override_test_runs = args.override_test_runs
    (
        _,
        _,
        default_metrics,
        _,
        _,
        _,
    ) = get_defaults(defaults_filename)

    for test_file in tqdm.tqdm(testsuite_spec_files):
        if defaults_filename in test_file:
            continue
        client_containers = []

        with open(test_file, "r") as stream:
            _, benchmark_config, test_name = get_final_benchmark_config(
                None, None, stream, ""
            )

            if tls_enabled:
                test_name = test_name + "-tls"
                logging.info(
                    "Given that TLS is enabled, appending -tls to the testname: {}.".format(
                        test_name
                    )
                )

            for topology_spec_name in benchmark_config["redis-topologies"]:
                test_result = False
                benchmark_tool_global = ""
                full_result_path = None
                try:
                    current_cpu_pos = args.cpuset_start_pos
                    temporary_dir_client = tempfile.mkdtemp(dir=home)

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

                    port = args.db_server_port
                    host = args.db_server_host
                    unix_socket = args.unix_socket
                    password = args.db_server_password
                    oss_cluster_api_enabled = args.cluster_mode
                    ssl_cert_reqs = "required"
                    if tls_skip_verify:
                        ssl_cert_reqs = None
                    r = redis.StrictRedis(
                        host=host,
                        port=port,
                        password=password,
                        ssl=tls_enabled,
                        ssl_cert_reqs=ssl_cert_reqs,
                        ssl_keyfile=tls_key,
                        ssl_certfile=tls_cert,
                        ssl_ca_certs=tls_cacert,
                        ssl_check_hostname=False,
                    )
                    setup_name = "oss-standalone"
                    r.ping()
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
                            shard_conn = redis.StrictRedis(
                                host=prefered_endpoint,
                                port=prefered_port,
                                password=password,
                                ssl=tls_enabled,
                                ssl_cert_reqs=ssl_cert_reqs,
                                ssl_keyfile=tls_key,
                                ssl_certfile=tls_cert,
                                ssl_ca_certs=tls_cacert,
                                ssl_check_hostname=False,
                            )
                            redis_conns.append(shard_conn)
                        logging.info(
                            "There are a total of {} shards".format(len(redis_conns))
                        )
                        setup_name = "oss-cluster"

                    redis_pids = []
                    for conn in redis_conns:
                        redis_pid = conn.info()["process_id"]
                        redis_pids.append(redis_pid)

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

                    benchmark_required_memory = get_benchmark_required_memory(
                        benchmark_config
                    )
                    maxmemory = 0
                    if args.maxmemory > 0:
                        maxmemory = args.maxmemory
                    else:
                        for conn in redis_conns:
                            maxmemory = maxmemory + get_maxmemory(conn)
                    if benchmark_required_memory > maxmemory:
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

                    reset_commandstats(redis_conns)

                    client_mnt_point = "/mnt/client/"
                    benchmark_tool_workdir = client_mnt_point

                    metadata = {}
                    test_tls_cacert = None
                    test_tls_cert = None
                    test_tls_key = None
                    if tls_enabled:
                        metadata["tls"] = "true"
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
                            logging.info(
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

                    if dry_run is True:
                        dry_run_count = dry_run_count + 1
                        delete_temporary_files(
                            temporary_dir_client=temporary_dir_client,
                            full_result_path=None,
                            benchmark_tool_global=benchmark_tool_global,
                        )
                        continue
                    if "dbconfig" in benchmark_config:
                        if "preload_tool" in benchmark_config["dbconfig"]:
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

                    if dry_run_include_preload is True:
                        dry_run_count = dry_run_count + 1
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
                        setup_type,
                    )
                    logging.info(
                        "Will store benchmark json output to local file {}".format(
                            local_benchmark_output_filename
                        )
                    )
                    arbitrary_command = False

                    if (
                        arbitrary_command
                        and oss_cluster_api_enabled
                        and "memtier" in benchmark_tool
                    ):
                        logging.warning(
                            "Forcing skip this test given there is an arbitrary commmand and memtier usage. Check https://github.com/RedisLabs/memtier_benchmark/pull/117 ."
                        )
                        delete_temporary_files(
                            temporary_dir_client=temporary_dir_client,
                            full_result_path=None,
                            benchmark_tool_global=benchmark_tool_global,
                        )
                        continue

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
                            logging.info(
                                "Running memtier benchmark command {}".format(
                                    benchmark_command_str
                                )
                            )
                            stream = os.popen(benchmark_command_str)
                            client_container_stdout = stream.read()
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
                            container = docker_client.containers.run(
                                image=client_container_image,
                                volumes={
                                    temporary_dir_client: {
                                        "bind": client_mnt_point,
                                        "mode": "rw",
                                    },
                                },
                                auto_remove=False,
                                privileged=True,
                                working_dir=benchmark_tool_workdir,
                                command=benchmark_command_str,
                                network_mode="host",
                                detach=True,
                                cpuset_cpus=client_cpuset_cpus,
                            )

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

                    logging.info("Printing client tool stdout output")

                    used_memory_check(
                        test_name,
                        benchmark_required_memory,
                        redis_conns,
                        "end of benchmark",
                        used_memory_check_fail,
                    )

                    if args.flushall_on_every_test_end:
                        logging.info("Sending FLUSHALL to the DB")
                        for r in redis_conns:
                            r.flushall()
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
                        )
                    else:
                        # Single client - read from file as usual
                        full_result_path = local_benchmark_output_filename
                        if "memtier_benchmark" in benchmark_tool:
                            full_result_path = "{}/{}".format(
                                temporary_dir_client, local_benchmark_output_filename
                            )
                        logging.info(f"Reading results json from {full_result_path}")

                        with open(
                            full_result_path,
                            "r",
                        ) as json_file:
                            results_dict = json.load(json_file)
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
                        setup_name,
                        setup_type,
                        test_name,
                        tf_github_org,
                        tf_github_repo,
                        tf_triggering_env,
                        topology_spec_name,
                        default_metrics,
                    )
                    test_result = True
                    total_test_suite_runs = total_test_suite_runs + 1

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

    # Print Redis server information section before results
    if len(results_matrix) > 0:
        # Get redis_conns from the first test context (we need to pass it somehow)
        # For now, try to get it from the current context if available
        try:
            # Try to get redis connection to display server info
            import redis as redis_module

            r = redis_module.StrictRedis(
                host="localhost", port=6379, decode_responses=True
            )
            r.ping()  # Test connection
            print_redis_info_section([r])
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

    if dry_run is True:
        logging.info(
            "Number of tests that would have been run: {}".format(dry_run_count)
        )


def get_maxmemory(r):
    maxmemory = int(r.info("memory")["maxmemory"])
    if maxmemory == 0:
        total_system_memory = int(r.info("memory")["total_system_memory"])
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
        used_memory = used_memory + conn.info("memory")["used_memory"]
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

    results_matrix = [[x[0], f"{x[3]:.3f}"] for x in results_matrix]
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

            print("\n# Redis Server Information")
            redis_info_data = [
                ["Redis Version", redis_info.get("redis_version", "unknown")],
                ["Redis Git SHA1", redis_info.get("redis_git_sha1", "unknown")],
                ["Redis Git Dirty", str(redis_info.get("redis_git_dirty", "unknown"))],
                ["Redis Build ID", redis_info.get("redis_build_id", "unknown")],
                ["Redis Mode", redis_info.get("redis_mode", "unknown")],
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


def prepare_overall_total_test_results(
    benchmark_config,
    default_metrics,
    results_dict,
    test_name,
    overall_results_matrix,
    redis_conns=None,
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
    current_test_results_matrix = [
        [test_name, x[0], f"{x[3]:.3f}"] for x in current_test_results_matrix
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
        if arbitrary_command is True and oss_cluster_api_enabled:
            logging.warning(
                "Skipping this test given it implies arbitrary command on an cluster setup. Not supported on memtier: https://github.com/RedisLabs/memtier_benchmark/pull/117"
            )
            result = False
            return result

        # run the benchmark
        preload_start_time = datetime.datetime.now()

        if benchmark_local_install:
            logging.info("Running memtier benchmark outside of docker")

            preload_command_str = (
                "taskset -c " + client_cpuset_cpus + " " + preload_command_str
            )
            logging.info(
                "Pre-loading using memtier benchmark command {}".format(
                    preload_command_str
                )
            )
            stream = os.popen(preload_command_str)
            client_container_stdout = stream.read()

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
