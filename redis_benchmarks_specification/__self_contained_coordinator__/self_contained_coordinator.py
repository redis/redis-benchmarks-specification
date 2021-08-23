import datetime
import json
import logging
import math
import pathlib
import sys
import tempfile
import shutil
import traceback
import docker
import redis
import os
from pathlib import Path
import redistimeseries
from docker.models.containers import Container

from redisbench_admin.run.common import (
    get_start_time_vars,
    prepare_benchmark_parameters,
)
from redisbench_admin.run.redistimeseries import timeseries_test_sucess_flow
from redisbench_admin.run.run import calculate_benchmark_duration_and_check
from redisbench_admin.utils.benchmark_config import (
    extract_redis_dbconfig_parameters,
    get_final_benchmark_config,
)
from redisbench_admin.utils.local import get_local_run_full_filename
from redisbench_admin.utils.results import post_process_benchmark_results

from redis_benchmarks_specification.__common__.env import (
    GH_REDIS_SERVER_HOST,
    GH_REDIS_SERVER_PORT,
    GH_REDIS_SERVER_AUTH,
    LOG_FORMAT,
    LOG_DATEFMT,
    LOG_LEVEL,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
    GH_REDIS_SERVER_USER,
    STREAM_GH_NEW_BUILD_RUNNERS_CG,
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
)
from redis_benchmarks_specification.__self_contained_coordinator__.args import (
    create_self_contained_coordinator_args,
)
from redis_benchmarks_specification.__setups__.topologies import get_topologies


def main():
    _, _, project_version = populate_with_poetry_data()
    project_name = "redis-benchmarks-spec runner(self-contained)"
    parser = create_self_contained_coordinator_args(
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
    testsuites_folder = os.path.abspath(args.test_suites_folder)
    logging.info("Using test-suites folder dir {}".format(testsuites_folder))
    testsuite_spec_files = get_benchmark_specs(testsuites_folder)
    logging.info(
        "There are a total of {} test-suites in folder {}".format(
            len(testsuite_spec_files), testsuites_folder
        )
    )

    logging.info(
        "Using redis available at: {}:{} to read the event streams".format(
            GH_REDIS_SERVER_HOST, GH_REDIS_SERVER_PORT
        )
    )
    try:
        conn = redis.StrictRedis(
            host=GH_REDIS_SERVER_HOST,
            port=GH_REDIS_SERVER_PORT,
            decode_responses=False,  # dont decode due to binary archives
            password=GH_REDIS_SERVER_AUTH,
            username=GH_REDIS_SERVER_USER,
            health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
            socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
            socket_keepalive=True,
        )
        conn.ping()
    except redis.exceptions.ConnectionError as e:
        logging.error(
            "Unable to connect to redis available at: {}:{} to read the event streams".format(
                GH_REDIS_SERVER_HOST, GH_REDIS_SERVER_PORT
            )
        )
        logging.error("Error message {}".format(e.__str__()))
        exit(1)
    rts = None
    if args.datasink_push_results_redistimeseries:
        logging.info(
            "Checking redistimeseries datasink connection is available at: {}:{} to push the timeseries data".format(
                args.datasink_redistimeseries_host, args.datasink_redistimeseries_port
            )
        )
        try:
            rts = redistimeseries.client.Client(
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
    build_runners_consumer_group_create(conn, running_platform)
    stream_id = None
    docker_client = docker.from_env()
    home = str(Path.home())
    # TODO: confirm we do have enough cores to run the spec
    # availabe_cpus = args.cpu_count
    datasink_push_results_redistimeseries = args.datasink_push_results_redistimeseries
    logging.info("Entering blocking read waiting for work.")
    if stream_id is None:
        stream_id = args.consumer_start_id
    while True:
        _, stream_id, _ = self_contained_coordinator_blocking_read(
            conn,
            datasink_push_results_redistimeseries,
            docker_client,
            home,
            stream_id,
            rts,
            testsuite_spec_files,
            topologies_map,
            running_platform,
        )


def build_runners_consumer_group_create(conn, running_platform, id="$"):
    consumer_group_name = get_runners_consumer_group_name(running_platform)
    logging.info("Will use consumer group named {}.".format(consumer_group_name))
    try:
        conn.xgroup_create(
            STREAM_KEYNAME_NEW_BUILD_EVENTS,
            consumer_group_name,
            mkstream=True,
            id=id,
        )
        logging.info(
            "Created consumer group named {} to distribute work.".format(
                consumer_group_name
            )
        )
    except redis.exceptions.ResponseError:
        logging.info(
            "Consumer group named {} already existed.".format(consumer_group_name)
        )


def get_runners_consumer_group_name(running_platform):
    consumer_group_name = "{}-{}".format(
        STREAM_GH_NEW_BUILD_RUNNERS_CG, running_platform
    )
    return consumer_group_name


def self_contained_coordinator_blocking_read(
    conn,
    datasink_push_results_redistimeseries,
    docker_client,
    home,
    stream_id,
    rts,
    testsuite_spec_files,
    topologies_map,
    platform_name,
):
    num_process_streams = 0
    overall_result = False
    consumer_name = "{}-self-contained-proc#{}".format(
        get_runners_consumer_group_name(platform_name), "1"
    )
    newTestInfo = conn.xreadgroup(
        get_runners_consumer_group_name(platform_name),
        consumer_name,
        {STREAM_KEYNAME_NEW_BUILD_EVENTS: stream_id},
        count=1,
        block=0,
    )
    if len(newTestInfo[0]) < 2 or len(newTestInfo[0][1]) < 1:
        stream_id = ">"
    else:
        stream_id, overall_result = process_self_contained_coordinator_stream(
            datasink_push_results_redistimeseries,
            docker_client,
            home,
            newTestInfo,
            rts,
            testsuite_spec_files,
            topologies_map,
            platform_name,
        )
        num_process_streams = num_process_streams + 1
        if overall_result is True:
            ack_reply = conn.xack(
                STREAM_KEYNAME_NEW_BUILD_EVENTS,
                STREAM_GH_NEW_BUILD_RUNNERS_CG,
                stream_id,
            )
            if type(ack_reply) == bytes:
                ack_reply = ack_reply.decode()
            if ack_reply == "1" or ack_reply == 1:
                logging.info(
                    "Sucessfully acknowledge build variation stream with id {}.".format(
                        stream_id
                    )
                )
            else:
                logging.error(
                    "Unable to acknowledge build variation stream with id {}. XACK reply {}".format(
                        stream_id, ack_reply
                    )
                )
    return overall_result, stream_id, num_process_streams


def process_self_contained_coordinator_stream(
    datasink_push_results_redistimeseries,
    docker_client,
    home,
    newTestInfo,
    rts,
    testsuite_spec_files,
    topologies_map,
    running_platform,
):
    stream_id, testDetails = newTestInfo[0][1][0]
    stream_id = stream_id.decode()
    logging.info("Received work . Stream id {}.".format(stream_id))
    overall_result = False

    if b"git_hash" in testDetails:
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
        ) = extract_build_info_from_streamdata(testDetails)

        overall_result = True
        for test_file in testsuite_spec_files:
            redis_containers = []
            client_containers = []

            with open(test_file, "r") as stream:
                benchmark_config, test_name = get_final_benchmark_config(
                    None, stream, ""
                )

                (
                    redis_configuration_parameters,
                    _,
                ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
                for topology_spec_name in benchmark_config["redis-topologies"]:
                    test_result = False
                    try:
                        current_cpu_pos = 0
                        ceil_db_cpu_limit = extract_db_cpu_limit(
                            topologies_map, topology_spec_name
                        )
                        temporary_dir = tempfile.mkdtemp(dir=home)
                        logging.info(
                            "Using local temporary dir to persist redis build artifacts. Path: {}".format(
                                temporary_dir
                            )
                        )
                        tf_github_org = "redis"
                        tf_github_repo = "redis"
                        tf_triggering_env = "ci"

                        benchmark_tool = "redis-benchmark"
                        for build_artifact in build_artifacts:
                            buffer = testDetails[
                                bytes("{}".format(build_artifact).encode())
                            ]
                            artifact_fname = "{}/{}".format(
                                temporary_dir, build_artifact
                            )
                            with open(artifact_fname, "wb") as fd:
                                fd.write(buffer)
                                os.chmod(artifact_fname, 755)
                            # TODO: re-enable
                            # if build_artifact == "redis-server":
                            #     redis_server_path = artifact_fname

                            logging.info(
                                "Successfully restored {} into {}".format(
                                    build_artifact, artifact_fname
                                )
                            )
                        port = 6379
                        mnt_point = "/mnt/redis/"
                        command = generate_standalone_redis_server_args(
                            "{}redis-server".format(mnt_point),
                            port,
                            mnt_point,
                            redis_configuration_parameters,
                        )
                        command_str = " ".join(command)
                        db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(
                            ceil_db_cpu_limit, current_cpu_pos
                        )
                        logging.info(
                            "Running redis-server on docker image {} (cpuset={}) with the following args: {}".format(
                                run_image, db_cpuset_cpus, command_str
                            )
                        )
                        container = docker_client.containers.run(
                            image=run_image,
                            volumes={
                                temporary_dir: {"bind": mnt_point, "mode": "rw"},
                            },
                            auto_remove=True,
                            privileged=True,
                            working_dir=mnt_point,
                            command=command_str,
                            network_mode="host",
                            detach=True,
                            cpuset_cpus=db_cpuset_cpus,
                        )
                        redis_containers.append(container)

                        full_benchmark_path = "/usr/local/bin/redis-benchmark"
                        client_mnt_point = "/mnt/client/"
                        benchmark_tool_workdir = client_mnt_point

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
                            "oss-standalone",
                        )
                        logging.info(
                            "Will store benchmark json output to local file {}".format(
                                local_benchmark_output_filename
                            )
                        )

                        # prepare the benchmark command
                        (
                            benchmark_command,
                            benchmark_command_str,
                        ) = prepare_benchmark_parameters(
                            benchmark_config,
                            full_benchmark_path,
                            port,
                            "localhost",
                            local_benchmark_output_filename,
                            False,
                            benchmark_tool_workdir,
                            False,
                        )
                        ceil_client_cpu_limit = extract_client_cpu_limit(
                            benchmark_config
                        )
                        client_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(
                            ceil_client_cpu_limit, current_cpu_pos
                        )
                        r = redis.StrictRedis(port=6379)
                        r.ping()

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
                                temporary_dir: {
                                    "bind": client_mnt_point,
                                    "mode": "rw",
                                },
                            },
                            auto_remove=True,
                            privileged=True,
                            working_dir=benchmark_tool_workdir,
                            command=benchmark_command_str,
                            network_mode="host",
                            detach=False,
                            cpuset_cpus=client_cpuset_cpus,
                        )

                        benchmark_end_time = datetime.datetime.now()
                        benchmark_duration_seconds = (
                            calculate_benchmark_duration_and_check(
                                benchmark_end_time, benchmark_start_time
                            )
                        )
                        logging.info("output {}".format(client_container_stdout))
                        r.shutdown(save=False)
                        datapoint_time_ms = start_time_ms
                        if use_git_timestamp is True and git_timestamp_ms is not None:
                            datapoint_time_ms = git_timestamp_ms
                        post_process_benchmark_results(
                            benchmark_tool,
                            local_benchmark_output_filename,
                            datapoint_time_ms,
                            start_time_str,
                            client_container_stdout,
                            None,
                        )

                        with open(local_benchmark_output_filename, "r") as json_file:
                            results_dict = json.load(json_file)
                            logging.info("Final JSON result {}".format(results_dict))
                        dataset_load_duration_seconds = 0

                        timeseries_test_sucess_flow(
                            datasink_push_results_redistimeseries,
                            git_version,
                            benchmark_config,
                            benchmark_duration_seconds,
                            dataset_load_duration_seconds,
                            None,
                            topology_spec_name,
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
                    for container in redis_containers:
                        try:
                            container.stop()
                        except docker.errors.NotFound:
                            logging.info(
                                "When trying to stop DB container with id {} and image {} it was already stopped".format(
                                    container.id, container.image
                                )
                            )
                            pass

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
                    shutil.rmtree(temporary_dir, ignore_errors=True)

                    overall_result &= test_result

    else:
        logging.error("Missing commit information within received message.")
    return stream_id, overall_result


def get_benchmark_specs(testsuites_folder):
    files = pathlib.Path(testsuites_folder).glob("*.yml")
    files = [str(x) for x in files]
    logging.info(
        "Running all specified benchmarks: {}".format(" ".join([str(x) for x in files]))
    )
    return files


def extract_build_info_from_streamdata(testDetails):
    use_git_timestamp = False
    git_timestamp_ms = None
    git_version = None
    git_branch = None
    metadata = None
    build_variant_name = None
    git_hash = testDetails[b"git_hash"]
    if b"use_git_timestamp" in testDetails:
        use_git_timestamp = bool(testDetails[b"use_git_timestamp"].decode())
    if b"git_timestamp_ms" in testDetails:
        git_timestamp_ms = int(testDetails[b"git_timestamp_ms"].decode())
    if b"id" in testDetails:
        build_variant_name = testDetails[b"id"]
        if type(build_variant_name) == bytes:
            build_variant_name = build_variant_name.decode()
    if b"git_branch" in testDetails:
        git_branch = testDetails[b"git_branch"]
        if type(git_branch) == bytes:
            git_branch = git_branch.decode()
    if b"git_version" in testDetails:
        git_version = testDetails[b"git_version"]
        if type(git_version) == bytes:
            git_version = git_version.decode()
    if type(git_hash) == bytes:
        git_hash = git_hash.decode()
    logging.info("Received commit hash specifier {}.".format(git_hash))
    build_artifacts_str = "redis-server"
    build_image = testDetails[b"build_image"].decode()
    run_image = build_image
    if b"run_image" in testDetails[b"run_image"]:
        run_image = testDetails[b"run_image"].decode()
    if b"build_artifacts" in testDetails:
        build_artifacts_str = testDetails[b"build_artifacts"].decode()
    build_artifacts = build_artifacts_str.split(",")
    if b"metadata" in testDetails:
        metadata = json.loads(testDetails[b"metadata"].decode())
    return (
        build_variant_name,
        metadata,
        build_artifacts,
        git_hash,
        git_branch,
        git_version,
        run_image,
        use_git_timestamp,
        git_timestamp_ms,
    )


def generate_cpuset_cpus(ceil_db_cpu_limit, current_cpu_pos):
    previous_cpu_pos = current_cpu_pos
    current_cpu_pos = current_cpu_pos + int(ceil_db_cpu_limit)
    db_cpuset_cpus = ",".join(
        [str(x) for x in range(previous_cpu_pos, current_cpu_pos)]
    )
    return db_cpuset_cpus, current_cpu_pos


def extract_db_cpu_limit(topologies_map, topology_spec_name):
    topology_spec = topologies_map[topology_spec_name]
    db_cpu_limit = topology_spec["resources"]["requests"]["cpus"]
    ceil_db_cpu_limit = math.ceil(float(db_cpu_limit))
    return ceil_db_cpu_limit


def generate_standalone_redis_server_args(
    binary, port, dbdir, configuration_parameters=None
):
    added_params = ["port", "protected-mode", "dir"]
    # start redis-server
    command = [
        binary,
        "--protected-mode",
        "no",
        "--port",
        "{}".format(port),
        "--dir",
        dbdir,
    ]
    if configuration_parameters is not None:
        for parameter, parameter_value in configuration_parameters.items():
            if parameter not in added_params:
                command.extend(
                    [
                        "--{}".format(parameter),
                        parameter_value,
                    ]
                )
    return command
