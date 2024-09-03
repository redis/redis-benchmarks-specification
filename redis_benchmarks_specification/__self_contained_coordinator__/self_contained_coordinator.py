import datetime
import json
import logging
import pathlib
import shutil
import tempfile
import traceback
import re
import docker
import redis
import os
from pathlib import Path
import sys
import time

from docker.models.containers import Container
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
)
from redis_benchmarks_specification.__self_contained_coordinator__.args import (
    create_self_contained_coordinator_args,
)
from redis_benchmarks_specification.__self_contained_coordinator__.runners import (
    build_runners_consumer_group_create,
    get_runners_consumer_group_name,
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
from redis_benchmarks_specification.__self_contained_coordinator__.cpuset import (
    extract_db_cpu_limit,
    generate_cpuset_cpus,
)
from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
    generate_standalone_redis_server_args,
)


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
    build_runners_consumer_group_create(gh_event_conn, running_platform)
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
    (
        _,
        _,
        default_metrics,
        _,
        _,
        _,
    ) = get_defaults(defaults_filename)

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
    newTestInfo = github_event_conn.xreadgroup(
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
        )
        num_process_streams = num_process_streams + 1
        num_process_test_suites = num_process_test_suites + total_test_suite_runs
        if overall_result is True:
            ack_reply = github_event_conn.xack(
                STREAM_KEYNAME_NEW_BUILD_EVENTS,
                get_runners_consumer_group_name(platform_name),
                stream_id,
            )
            if type(ack_reply) == bytes:
                ack_reply = ack_reply.decode()
            if ack_reply == "1" or ack_reply == 1:
                logging.info(
                    "Sucessfully acknowledge BENCHMARK variation stream with id {}.".format(
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
                    "changing executable from {executable} to {new_executable}"
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

            skip_test = False
            if b"platform" in testDetails:
                platform = testDetails[b"platform"]
                if running_platform != platform:
                    skip_test = True
                    logging.info(
                        "skipping stream_id {} given plaform {}!={}".format(
                            stream_id, running_platform, platform
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
                )

                for test_file in filtered_test_files:
                    with open(test_file, "r") as stream:
                        (
                            _,
                            benchmark_config,
                            test_name,
                        ) = get_final_benchmark_config(None, None, stream, "")
                    github_event_conn.lpush(stream_test_list_pending, test_name)
                    github_event_conn.expire(
                        stream_test_list_pending, REDIS_BINS_EXPIRE_SECS
                    )
                    logging.info(
                        f"Added test named {test_name} to the pending test list in key {stream_test_list_pending}"
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
                        logging.info(
                            f"Added test named {test_name} to the pending test list in key {stream_test_list_running}"
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

                                r = redis.StrictRedis(port=redis_proc_start_port)
                                r.ping()
                                redis_conns = [r]
                                reset_commandstats(redis_conns)
                                redis_pids = []
                                redis_info = r.info()
                                first_redis_pid = redis_info["process_id"]
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
                                    )

                                execute_init_commands(
                                    benchmark_config, r, dbconfig_keyname="dbconfig"
                                )

                                benchmark_tool = extract_client_tool(benchmark_config)
                                # backwards compatible
                                if benchmark_tool is None:
                                    benchmark_tool = "redis-benchmark"
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
                                if "memtier_benchmark" not in benchmark_tool:
                                    # prepare the benchmark command
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
                                else:
                                    (
                                        _,
                                        benchmark_command_str,
                                        arbitrary_command,
                                    ) = prepare_memtier_benchmark_parameters(
                                        benchmark_config["clientconfig"],
                                        full_benchmark_path,
                                        redis_proc_start_port,
                                        "localhost",
                                        None,
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

                                client_container_stdout = docker_client.containers.run(
                                    image=client_container_image,
                                    volumes={
                                        temporary_dir_client: {
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
                                logging.info(
                                    "Removing temporary dirs {} and {}".format(
                                        temporary_dir, temporary_dir_client
                                    )
                                )

                                shutil.rmtree(temporary_dir, ignore_errors=True)
                                shutil.rmtree(temporary_dir_client, ignore_errors=True)

                            overall_result &= test_result

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
                            total_improvements,
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
                            baseline_tag,
                            comparison_tag,
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
                        auto_approve = True
                        grafana_link_base = "https://benchmarksredisio.grafana.net/d/1fWbtb7nz/experimental-oss-spec-benchmarks"

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
                    logging.info(
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
):
    filtered_test_files = []
    for test_file in testsuite_spec_files:
        if defaults_filename in test_file:
            continue

        if tests_regexp != ".*":
            logging.info(
                "Filtering all tests via a regular expression: {}".format(tests_regexp)
            )
            tags_regex_string = re.compile(tests_regexp)

            match_obj = re.search(tags_regex_string, test_file)
            if match_obj is None:
                logging.info(
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
                logging.info(
                    "Filtering all test command groups via a regular expression: {}".format(
                        command_groups_regexp
                    )
                )
                if "tested-groups" in benchmark_config:
                    command_groups = benchmark_config["tested-groups"]
                    logging.info(
                        f"The file {test_file} (test name = {test_name}) contains the following groups: {command_groups}"
                    )
                    groups_regex_string = re.compile(command_groups_regexp)
                    found = False
                    for command_group in command_groups:
                        match_obj = re.search(groups_regex_string, command_group)
                        if match_obj is not None:
                            found = True
                            logging.info(f"found the command group {command_group}")
                    if found is False:
                        logging.info(
                            f"Skipping {test_file} given the following groups: {command_groups} does not match command group regex {command_groups_regexp}"
                        )
                        continue
                else:
                    logging.warning(
                        f"The file {test_file} (test name = {test_name}) does not contain the property 'tested-groups'. Cannot filter based uppon groups..."
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
            None,
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
