import datetime
import json
import logging
import os
import pathlib
import sys
import tempfile
import traceback
from pathlib import Path
import shutil

import docker
import redis
from docker.models.containers import Container
from redisbench_admin.run.common import (
    get_start_time_vars,
    prepare_benchmark_parameters,
)
from redisbench_admin.run.redistimeseries import timeseries_test_sucess_flow
from redisbench_admin.run.run import calculate_client_tool_duration_and_check
from redisbench_admin.utils.benchmark_config import (
    get_final_benchmark_config,
)
from redisbench_admin.utils.local import get_local_run_full_filename
from redisbench_admin.utils.results import post_process_benchmark_results
from redistimeseries.client import Client

from redis_benchmarks_specification.__common__.env import (
    LOG_FORMAT,
    LOG_DATEFMT,
    LOG_LEVEL,
    REDIS_HEALTH_CHECK_INTERVAL,
    REDIS_SOCKET_TIMEOUT,
)
from redis_benchmarks_specification.__common__.package import (
    get_version_string,
    populate_with_poetry_data,
)
from redis_benchmarks_specification.__common__.spec import (
    extract_client_cpu_limit,
    extract_client_container_image,
    extract_client_tool,
)
from redis_benchmarks_specification.__runner__.args import create_client_runner_args


def main():
    _, _, project_version = populate_with_poetry_data()
    project_name_suffix = "redis-benchmarks-spec-client-runner"
    project_name = "{} (solely client)".format(project_name_suffix)
    parser = create_client_runner_args(
        get_version_string(project_name, project_version)
    )
    args = parser.parse_args()

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
    logging.info(get_version_string(project_name, project_version))
    testsuites_folder = os.path.abspath(args.test_suites_folder)
    logging.info("Using test-suites folder dir {}".format(testsuites_folder))
    testsuite_spec_files = get_benchmark_specs(testsuites_folder, args.test)
    logging.info(
        "There are a total of {} test-suites in folder {}".format(
            len(testsuite_spec_files), testsuites_folder
        )
    )

    rts = None
    if args.datasink_push_results_redistimeseries:
        logging.info(
            "Checking redistimeseries datasink connection is available at: {}:{} to push the timeseries data".format(
                args.datasink_redistimeseries_host, args.datasink_redistimeseries_port
            )
        )
        try:
            rts = Client(
                host=args.datasink_redistimeseries_host,
                port=args.datasink_redistimeseries_port,
                decode_responses=True,
                password=args.datasink_redistimeseries_pass,
                username=args.datasink_redistimeseries_user,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
                socket_keepalive=True,
            )
            rts.redis.ping()
            rts.redis.client_setname(project_name_suffix)
        except redis.exceptions.ConnectionError as e:
            logging.error(
                "Unable to connect to redis available at: {}:{}".format(
                    args.datasink_redistimeseries_host,
                    args.datasink_redistimeseries_port,
                )
            )
            logging.error("Error message {}".format(e.__str__()))
            exit(1)

    running_platform = args.platform_name
    tls_enabled = args.tls
    tls_skip_verify = args.tls_skip_verify
    tls_cert = args.cert
    tls_key = args.key
    tls_cacert = args.cacert
    docker_client = docker.from_env()
    home = str(Path.home())
    logging.info("Running the benchmark specs.")

    process_self_contained_coordinator_stream(
        args,
        args.datasink_push_results_redistimeseries,
        docker_client,
        home,
        None,
        rts,
        testsuite_spec_files,
        {},
        running_platform,
        tls_enabled,
        tls_skip_verify,
        tls_cert,
        tls_key,
        tls_cacert,
    )


def prepare_memtier_benchmark_parameters(
    clientconfig,
    full_benchmark_path,
    port,
    server,
    local_benchmark_output_filename,
    oss_cluster_api_enabled,
    tls_enabled=False,
    tls_skip_verify=False,
    tls_cert=None,
    tls_key=None,
    tls_cacert=None,
):
    benchmark_command = [
        full_benchmark_path,
        "--port",
        "{}".format(port),
        "--server",
        "{}".format(server),
        "--json-out-file",
        local_benchmark_output_filename,
    ]
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

    if oss_cluster_api_enabled is True:
        benchmark_command.append("--cluster-mode")
    benchmark_command_str = " ".join(benchmark_command)
    if "arguments" in clientconfig:
        benchmark_command_str = benchmark_command_str + " " + clientconfig["arguments"]

    return None, benchmark_command_str


def process_self_contained_coordinator_stream(
    args,
    datasink_push_results_redistimeseries,
    docker_client,
    home,
    newTestInfo,
    rts,
    testsuite_spec_files,
    topologies_map,
    running_platform,
    tls_enabled=False,
    tls_skip_verify=False,
    tls_cert=None,
    tls_key=None,
    tls_cacert=None,
):
    overall_result = True
    total_test_suite_runs = 0
    for test_file in testsuite_spec_files:
        client_containers = []

        with open(test_file, "r") as stream:
            _, benchmark_config, test_name = get_final_benchmark_config(
                None, stream, ""
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
                try:
                    current_cpu_pos = args.cpuset_start_pos
                    temporary_dir_client = tempfile.mkdtemp(dir=home)

                    tf_github_org = args.github_org
                    tf_github_repo = args.github_repo
                    tf_triggering_env = args.platform_name
                    setup_type = args.setup_type
                    git_hash = "NA"
                    git_version = args.github_version
                    build_variant_name = "NA"
                    git_branch = None

                    port = args.db_server_port
                    host = args.db_server_host

                    ssl_cert_reqs = "required"
                    if tls_skip_verify:
                        ssl_cert_reqs = None
                    r = redis.StrictRedis(
                        host=host,
                        port=port,
                        ssl=tls_enabled,
                        ssl_cert_reqs=ssl_cert_reqs,
                        ssl_keyfile=tls_key,
                        ssl_certfile=tls_cert,
                        ssl_ca_certs=tls_cacert,
                        ssl_check_hostname=False,
                    )
                    r.ping()

                    ceil_client_cpu_limit = extract_client_cpu_limit(benchmark_config)
                    client_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(
                        ceil_client_cpu_limit, current_cpu_pos
                    )
                    if args.flushall_on_every_test_start:
                        logging.info("Sending FLUSHALL to the DB")
                        r.flushall()
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

                    if "preload_tool" in benchmark_config["dbconfig"]:
                        data_prepopulation_step(
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
                        )

                    benchmark_tool = extract_client_tool(benchmark_config)
                    # backwards compatible
                    if benchmark_tool is None:
                        benchmark_tool = "redis-benchmark"
                    full_benchmark_path = "/usr/local/bin/{}".format(benchmark_tool)

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
                    if "memtier_benchmark" not in benchmark_tool:
                        # prepare the benchmark command
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
                    else:
                        (
                            _,
                            benchmark_command_str,
                        ) = prepare_memtier_benchmark_parameters(
                            benchmark_config["clientconfig"],
                            full_benchmark_path,
                            port,
                            host,
                            local_benchmark_output_filename,
                            False,
                            tls_enabled,
                            tls_skip_verify,
                            test_tls_cert,
                            test_tls_key,
                            test_tls_cacert,
                        )

                    client_container_image = extract_client_container_image(
                        benchmark_config
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

                    client_container_stdout = docker_client.containers.run(
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
                        detach=False,
                        cpuset_cpus=client_cpuset_cpus,
                    )

                    benchmark_end_time = datetime.datetime.now()
                    benchmark_duration_seconds = (
                        calculate_client_tool_duration_and_check(
                            benchmark_end_time, benchmark_start_time
                        )
                    )
                    logging.info("output {}".format(client_container_stdout))
                    if args.flushall_on_every_test_end:
                        logging.info("Sending FLUSHALL to the DB")
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
                    full_result_path = local_benchmark_output_filename
                    if "memtier_benchmark" in benchmark_tool:
                        full_result_path = "{}/{}".format(
                            temporary_dir_client, local_benchmark_output_filename
                        )
                    logging.info(
                        "Reading results json from {}".format(full_result_path)
                    )

                    with open(
                        full_result_path,
                        "r",
                    ) as json_file:
                        results_dict = json.load(json_file)
                        logging.info("Final JSON result {}".format(results_dict))
                    dataset_load_duration_seconds = 0

                    logging.info(
                        "Using datapoint_time_ms: {}".format(datapoint_time_ms)
                    )

                    timeseries_test_sucess_flow(
                        datasink_push_results_redistimeseries,
                        git_version,
                        benchmark_config,
                        benchmark_duration_seconds,
                        dataset_load_duration_seconds,
                        None,
                        topology_spec_name,
                        setup_type,
                        None,
                        results_dict,
                        rts,
                        datapoint_time_ms,
                        test_name,
                        git_branch,
                        tf_github_org,
                        tf_github_repo,
                        tf_triggering_env,
                        metadata,
                        build_variant_name,
                        running_platform,
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
                shutil.rmtree(temporary_dir_client, ignore_errors=True)
                overall_result &= test_result


def cp_to_workdir(benchmark_tool_workdir, srcfile):
    head, filename = os.path.split(srcfile)
    dstfile = "{}/{}".format(benchmark_tool_workdir, filename)
    shutil.copyfile(srcfile, dstfile)
    logging.info(
        "Copying to workdir the following file {}. Final workdir file {}".format(
            srcfile, dstfile
        )
    )
    return dstfile, filename


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
        (_, preload_command_str,) = prepare_memtier_benchmark_parameters(
            benchmark_config["dbconfig"]["preload_tool"],
            full_benchmark_path,
            port,
            host,
            local_benchmark_output_filename,
            False,
            tls_enabled,
            tls_skip_verify,
            tls_cert,
            tls_key,
            tls_cacert,
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

        client_container_stdout = docker_client.containers.run(
            image=preload_image,
            volumes={
                temporary_dir: {
                    "bind": client_mnt_point,
                    "mode": "rw",
                },
            },
            auto_remove=True,
            privileged=True,
            working_dir=benchmark_tool_workdir,
            command=preload_command_str,
            network_mode="host",
            detach=False,
            cpuset_cpus=client_cpuset_cpus,
        )

        preload_end_time = datetime.datetime.now()
        preload_duration_seconds = calculate_client_tool_duration_and_check(
            preload_end_time, preload_start_time
        )
        logging.info(
            "Tool {} seconds to load data. Output {}".format(
                preload_duration_seconds,
                client_container_stdout,
            )
        )


def get_benchmark_specs(testsuites_folder, test):
    if test == "":
        files = pathlib.Path(testsuites_folder).glob("*.yml")
        files = [str(x) for x in files]
        logging.info(
            "Running all specified benchmarks: {}".format(
                " ".join([str(x) for x in files])
            )
        )
    else:
        files = test.split(",")
        files = ["{}/{}".format(testsuites_folder, x) for x in files]
        logging.info("Running specific benchmark in file: {}".format(files))
    return files


def generate_cpuset_cpus(ceil_db_cpu_limit, current_cpu_pos):
    previous_cpu_pos = current_cpu_pos
    current_cpu_pos = current_cpu_pos + int(ceil_db_cpu_limit)
    db_cpuset_cpus = ",".join(
        [str(x) for x in range(previous_cpu_pos, current_cpu_pos)]
    )
    return db_cpuset_cpus, current_cpu_pos
