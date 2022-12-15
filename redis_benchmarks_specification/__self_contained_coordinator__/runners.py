import datetime
import json
import logging
import shutil
import sys
import tempfile
import traceback

import redis
from redisbench_admin.environments.oss_cluster import generate_cluster_redis_server_args

from redisbench_admin.utils.local import check_dataset_local_requirements

from redisbench_admin.run.common import (
    dbconfig_keyspacelen_check,
)

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
from redisbench_admin.run.redistimeseries import (
    datasink_profile_tabular_data,
    timeseries_test_sucess_flow,
)
from redisbench_admin.run.run import calculate_client_tool_duration_and_check
from redisbench_admin.utils.benchmark_config import (
    get_final_benchmark_config,
    extract_redis_dbconfig_parameters,
)
from redisbench_admin.utils.local import get_local_run_full_filename
from redisbench_admin.utils.results import post_process_benchmark_results

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
    STREAM_GH_NEW_BUILD_RUNNERS_CG,
    S3_BUCKET_NAME,
)
from redis_benchmarks_specification.__common__.spec import (
    extract_build_variant_variations,
    extract_client_cpu_limit,
    extract_client_tool,
    extract_client_container_image,
)
from redis_benchmarks_specification.__self_contained_coordinator__.artifacts import (
    restore_build_artifacts_from_test_details,
)
from redis_benchmarks_specification.__self_contained_coordinator__.build_info import (
    extract_build_info_from_streamdata,
)
from redis_benchmarks_specification.__self_contained_coordinator__.clients import (
    prepare_memtier_benchmark_parameters,
)
from redis_benchmarks_specification.__self_contained_coordinator__.cpuset import (
    extract_db_cpu_limit,
    generate_cpuset_cpus,
)
from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
    spin_docker_standalone_redis,
    teardown_containers,
)
from redis_benchmarks_specification.__self_contained_coordinator__.prepopulation import (
    data_prepopulation_step,
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


def process_self_contained_coordinator_stream(
    conn,
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
    docker_air_gap=False,
    verbose=False,
    run_tests_with_dataset=False,
):
    stream_id = "n/a"
    overall_result = False
    total_test_suite_runs = 0
    try:
        stream_id, testDetails = newTestInfo[0][1][0]
        stream_id = stream_id.decode()
        logging.info("Received work . Stream id {}.".format(stream_id))

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
            profiler_dashboard_links = []
            if docker_air_gap:
                airgap_key = "docker:air-gap:{}".format(run_image)
                logging.info(
                    "Restoring docker image: {} from {}".format(run_image, airgap_key)
                )
                if conn.exists(airgap_key):
                    airgap_docker_image_bin = conn.get(airgap_key)
                    images_loaded = docker_client.images.load(airgap_docker_image_bin)
                    logging.info("Successfully loaded images {}".format(images_loaded))
                else:
                    logging.error(
                        "docker image {} was not present on key {}".format(
                            run_image, airgap_key
                        )
                    )

            for test_file in testsuite_spec_files:
                redis_containers = []
                client_containers = []

                with open(test_file, "r") as stream:
                    result, benchmark_config, test_name = get_final_benchmark_config(
                        None, stream, ""
                    )
                    if result is False:
                        logging.error(
                            "Skipping {} given there were errors while calling get_final_benchmark_config()".format(
                                test_file
                            )
                        )
                        continue
                    (
                        _,
                        _,
                        redis_configuration_parameters,
                        _,
                        _,
                    ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
                    build_variants = extract_build_variant_variations(benchmark_config)
                    if build_variants is not None:
                        logging.info("Detected build variant filter")
                        if build_variant_name not in build_variants:
                            logging.error(
                                "Skipping {} given it's not part of build-variants for this test-suite {}".format(
                                    build_variant_name, build_variants
                                )
                            )
                            continue
                        else:
                            logging.error(
                                "Running build variant {} given it's present on the build-variants spec {}".format(
                                    build_variant_name, build_variants
                                )
                            )
                    for topology_spec_name in benchmark_config["redis-topologies"]:
                        test_result = False
                        try:
                            current_cpu_pos = cpuset_start_pos
                            ceil_db_cpu_limit = extract_db_cpu_limit(
                                topologies_map, topology_spec_name
                            )

                            temporary_dir_client = tempfile.mkdtemp(dir=home)
                            temporary_dir = tempfile.mkdtemp(dir=home)
                            logging.info(
                                "Using local temporary dir to persist redis build artifacts. Path: {}".format(
                                    temporary_dir
                                )
                            )
                            tf_github_org = "redis"
                            tf_github_repo = "redis"
                            setup_name = topology_spec_name
                            tf_triggering_env = "ci"
                            github_actor = "{}-{}".format(
                                tf_triggering_env, running_platform
                            )
                            restore_build_artifacts_from_test_details(
                                build_artifacts, conn, temporary_dir, testDetails
                            )

                            if "dataset" in benchmark_config["dbconfig"]:
                                if run_tests_with_dataset is False:
                                    logging.warning(
                                        "Skipping test {} giving it implies dataset preload".format(
                                            test_name
                                        )
                                    )
                                    continue
                            logging.info("Checking if there is a dataset requirement")
                            (
                                dataset,
                                dataset_name,
                                _,
                                _,
                            ) = check_dataset_local_requirements(
                                benchmark_config,
                                temporary_dir,
                                None,
                                "./datasets",
                                "dbconfig",
                                1,
                                False,
                            )

                            dso = "redis-server"
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
                            if setup_name == "oss-standalone":
                                current_cpu_pos = spin_docker_standalone_redis(
                                    ceil_db_cpu_limit,
                                    current_cpu_pos,
                                    docker_client,
                                    redis_configuration_parameters,
                                    redis_containers,
                                    redis_proc_start_port,
                                    run_image,
                                    temporary_dir,
                                )
                            else:
                                shard_count = 1
                                start_port = redis_proc_start_port
                                dbdir_folder = None
                                server_private_ip = "127.0.0.1"
                                for master_shard_id in range(1, shard_count + 1):
                                    shard_port = master_shard_id + start_port - 1

                                    (
                                        command,
                                        logfile,
                                    ) = generate_cluster_redis_server_args(
                                        "redis-server",
                                        dbdir_folder,
                                        None,
                                        server_private_ip,
                                        shard_port,
                                        redis_configuration_parameters,
                                        "yes",
                                        None,
                                        "",
                                        "yes",
                                        False,
                                    )
                                    logging.error(
                                        "Remote primary shard {} command: {}".format(
                                            master_shard_id, " ".join(command)
                                        )
                                    )

                            r = redis.StrictRedis(port=redis_proc_start_port)
                            r.ping()
                            redis_pids = []
                            first_redis_pid = r.info()["process_id"]
                            redis_pids.append(first_redis_pid)
                            ceil_client_cpu_limit = extract_client_cpu_limit(
                                benchmark_config
                            )
                            client_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(
                                ceil_client_cpu_limit, current_cpu_pos
                            )
                            client_mnt_point = "/mnt/client/"
                            benchmark_tool_workdir = client_mnt_point

                            logging.info(
                                "Checking if there is a data preload_tool requirement"
                            )
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

                            logging.info(
                                "Checking if there is a keyspace check being enforced"
                            )
                            dbconfig_keyspacelen_check(
                                benchmark_config,
                                [r],
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
                                    topology_spec_name,
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
                                ) = prepare_memtier_benchmark_parameters(
                                    benchmark_config["clientconfig"],
                                    full_benchmark_path,
                                    redis_proc_start_port,
                                    "localhost",
                                    local_benchmark_output_filename,
                                    benchmark_tool_workdir,
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
                            if verbose:
                                logging.info(
                                    "output {}".format(client_container_stdout)
                                )
                            r.shutdown(save=False)

                            (_, overall_tabular_data_map,) = profilers_stop_if_required(
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
                                    conn,
                                    setup_name,
                                    start_time_ms,
                                    start_time_str,
                                    test_name,
                                    tf_triggering_env,
                                )
                                if len(profilers_artifacts_matrix) == 0:
                                    logging.error("No profiler artifact was retrieved")
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
                                    https_link = generate_artifacts_table_grafana_redis(
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
                            logging.critical(
                                "Reading results json from {}".format(full_result_path)
                            )

                            with open(
                                full_result_path,
                                "r",
                            ) as json_file:
                                results_dict = json.load(json_file)
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
                                setup_name,
                                None,
                                results_dict,
                                datasink_conn,
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
                        teardown_containers(redis_containers, "DB")
                        teardown_containers(client_containers, "CLIENT")
                        shutil.rmtree(temporary_dir, ignore_errors=True)

                        overall_result &= test_result

        else:
            logging.error("Missing commit information within received message.")
    except:
        logging.critical(
            "Some unexpected exception was caught "
            "during local work on stream {}. Failing test....".format(stream_id)
        )
        logging.critical(sys.exc_info()[0])
        print("-" * 60)
        traceback.print_exc(file=sys.stdout)
        print("-" * 60)
        overall_result = False
    return stream_id, overall_result, total_test_suite_runs
