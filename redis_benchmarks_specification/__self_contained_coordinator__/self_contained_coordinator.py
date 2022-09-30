import logging
import pathlib
import docker
import redis
import os
from pathlib import Path

from redisbench_admin.profilers.profilers_local import (
    check_compatible_system_and_kernel_and_prepare_profile,
)

from redis_benchmarks_specification.__common__.env import (
    LOG_FORMAT,
    LOG_DATEFMT,
    LOG_LEVEL,
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
    REDIS_HEALTH_CHECK_INTERVAL,
    REDIS_SOCKET_TIMEOUT,
)
from redis_benchmarks_specification.__common__.package import (
    get_version_string,
    populate_with_poetry_data,
)
from redis_benchmarks_specification.__self_contained_coordinator__.args import (
    create_self_contained_coordinator_args,
)
from redis_benchmarks_specification.__self_contained_coordinator__.runners import (
    build_runners_consumer_group_create,
    get_runners_consumer_group_name,
    process_self_contained_coordinator_stream,
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
        "Reading event streams from: {}:{} with user {}".format(
            args.event_stream_host, args.event_stream_port, args.event_stream_user
        )
    )
    try:
        conn = redis.StrictRedis(
            host=args.event_stream_host,
            port=args.event_stream_port,
            decode_responses=False,  # dont decode due to binary archives
            password=args.event_stream_pass,
            username=args.event_stream_user,
            health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
            socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
            socket_keepalive=True,
        )
        conn.ping()
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
    build_runners_consumer_group_create(conn, running_platform)
    stream_id = None
    docker_client = docker.from_env()
    home = str(Path.home())
    cpuset_start_pos = args.cpuset_start_pos
    logging.info("Start CPU pinning at position {}".format(cpuset_start_pos))
    redis_proc_start_port = args.redis_proc_start_port
    logging.info("Redis Processes start port: {}".format(redis_proc_start_port))

    # TODO: confirm we do have enough cores to run the spec
    # availabe_cpus = args.cpu_count
    datasink_push_results_redistimeseries = args.datasink_push_results_redistimeseries
    grafana_profile_dashboard = args.grafana_profile_dashboard

    # Consumer id
    consumer_pos = args.consumer_pos
    logging.info("Consumer pos {}".format(consumer_pos))

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

    logging.info("Entering blocking read waiting for work.")
    if stream_id is None:
        stream_id = args.consumer_start_id
    while True:
        _, stream_id, _, _ = self_contained_coordinator_blocking_read(
            conn,
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
        )


def self_contained_coordinator_blocking_read(
    conn,
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
        (
            stream_id,
            overall_result,
            total_test_suite_runs,
        ) = process_self_contained_coordinator_stream(
            conn,
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
        )
        num_process_streams = num_process_streams + 1
        num_process_test_suites = num_process_test_suites + total_test_suite_runs
        if overall_result is True:
            ack_reply = conn.xack(
                STREAM_KEYNAME_NEW_BUILD_EVENTS,
                get_runners_consumer_group_name(platform_name),
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
    return overall_result, stream_id, num_process_streams, num_process_test_suites


def get_benchmark_specs(testsuites_folder):
    files = pathlib.Path(testsuites_folder).glob("*.yml")
    files = [str(x) for x in files]
    logging.info(
        "Running all specified benchmarks: {}".format(" ".join([str(x) for x in files]))
    )
    return files


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
