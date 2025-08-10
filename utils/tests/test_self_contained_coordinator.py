import os

import docker
import redis
import yaml
from pathlib import Path

from redisbench_admin.utils.remote import get_overall_dashboard_keynames
from redisbench_admin.utils.utils import get_ts_metric_name
import logging

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
)
from redis_benchmarks_specification.__common__.spec import (
    extract_client_cpu_limit,
    extract_client_container_image,
    extract_client_tool,
)
from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    self_contained_coordinator_blocking_read,
    start_redis_container,
)

from redis_benchmarks_specification.__self_contained_coordinator__.runners import (
    build_runners_consumer_group_create,
    get_runners_consumer_group_name,
)
from redis_benchmarks_specification.__self_contained_coordinator__.cpuset import (
    generate_cpuset_cpus,
)
from redis_benchmarks_specification.__setups__.topologies import get_topologies
from utils.tests.test_data.api_builder_common import flow_1_and_2_api_builder_checks


from redis_benchmarks_specification.__self_contained_coordinator__.docker import (
    generate_standalone_redis_server_args,
)


def test_extract_client_cpu_limit():
    with open(
        "./utils/tests/test_data/test-suites/redis-benchmark-full-suite-1Mkeys-100B.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_cpu_limit = extract_client_cpu_limit(benchmark_config)
        # we use a benchmark spec with smaller CPU limit for client given github machines only contain 2 cores
        # and we need 1 core for DB and another for CLIENT
        assert client_cpu_limit == 1


def test_extract_client_container_image():
    with open(
        "./utils/tests/test_data/test-suites/redis-benchmark-full-suite-1Mkeys-100B.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_container_image = extract_client_container_image(benchmark_config)
        assert client_container_image == "redis:6.2.4"

    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_container_image = extract_client_container_image(benchmark_config)
        assert client_container_image == "redislabs/memtier_benchmark:edge"


def test_extract_client_tool():
    with open(
        "./utils/tests/test_data/test-suites/redis-benchmark-full-suite-1Mkeys-100B.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_tool = extract_client_tool(benchmark_config)
        assert client_tool == "redis-benchmark"

    with open(
        "./redis_benchmarks_specification/test-suites/memtier_benchmark-1Mkeys-100B-expire-use-case.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        client_tool = extract_client_tool(benchmark_config)
        assert client_tool == "memtier_benchmark"


def test_generate_cpuset_cpus():
    db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(2.0, 0)
    assert db_cpuset_cpus == "0,1"

    db_cpuset_cpus, current_cpu_pos = generate_cpuset_cpus(3, current_cpu_pos)
    assert db_cpuset_cpus == "2,3,4"


def run_coordinator_tests():
    run_coordinator = True
    TST_RUNNER_X = os.getenv("TST_RUNNER_X", "0")
    if TST_RUNNER_X == "0":
        run_coordinator = False
    return run_coordinator


def test_self_contained_coordinator_blocking_read():
    try:
        if run_coordinator_tests():
            conn = redis.StrictRedis(port=6379)
            conn.ping()
            expected_datapoint_ts = None
            conn.flushall()
            build_variant_name, reply_fields = flow_1_and_2_api_builder_checks(conn)
            if b"git_timestamp_ms" in reply_fields:
                expected_datapoint_ts = int(reply_fields[b"git_timestamp_ms"].decode())
            if b"git_timestamp_ms" in reply_fields:
                expected_datapoint_ts = int(reply_fields[b"git_timestamp_ms"].decode())
            if "git_timestamp_ms" in reply_fields:
                expected_datapoint_ts = int(reply_fields["git_timestamp_ms"])

            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            assert conn.xlen(STREAM_KEYNAME_NEW_BUILD_EVENTS) > 0
            running_platform = "fco-ThinkPad-T490"

            build_runners_consumer_group_create(conn, running_platform, "0")
            datasink_conn = redis.StrictRedis(port=6379)
            rts = datasink_conn.ts()
            docker_client = docker.from_env()
            home = str(Path.home())
            stream_id = ">"
            topologies_map = get_topologies(
                "./redis_benchmarks_specification/setups/topologies/topologies.yml"
            )
            # we use a benchmark spec with smaller CPU limit for client given github machines only contain 2 cores
            # and we need 1 core for DB and another for CLIENT
            testsuite_spec_files = [
                "./utils/tests/test_data/test-suites/redis-benchmark-full-suite-1Mkeys-100B.yml"
            ]
            (
                result,
                stream_id,
                number_processed_streams,
                _,
            ) = self_contained_coordinator_blocking_read(
                conn,
                True,
                docker_client,
                home,
                stream_id,
                datasink_conn,
                testsuite_spec_files,
                topologies_map,
                running_platform,
                False,
                [],
            )
            assert result == True
            assert number_processed_streams == 1
            # ensure we're able to aknowledge the consumed message
            assert (
                conn.xinfo_consumers(
                    STREAM_KEYNAME_NEW_BUILD_EVENTS,
                    get_runners_consumer_group_name(running_platform),
                )[0]["pending"]
                == 0
            )
            tf_github_org = "redis"
            tf_github_repo = "redis"
            test_name = "redis-benchmark-full-suite-1Mkeys-100B"
            tf_triggering_env = "ci"
            deployment_type = "oss-standalone"
            deployment_name = "oss-standalone"
            metric_name = "rps"
            use_metric_context_path = True
            metric_context_path = "MSET"

            ts_key_name = get_ts_metric_name(
                "by.branch",
                "unstable",
                tf_github_org,
                tf_github_repo,
                deployment_name,
                deployment_type,
                test_name,
                tf_triggering_env,
                metric_name,
                metric_context_path,
                use_metric_context_path,
                build_variant_name,
                running_platform,
            )

            assert ts_key_name.encode() in conn.keys()
            assert len(rts.range(ts_key_name, 0, "+")) == 1
            if expected_datapoint_ts is not None:
                assert rts.range(ts_key_name, 0, "+")[0][0] == expected_datapoint_ts
            (
                prefix,
                testcases_setname,
                deployment_name_setname,
                tsname_project_total_failures,
                tsname_project_total_success,
                running_platforms_setname,
                build_variant_setname,
                testcases_metric_context_path_setname,
                testcases_and_metric_context_path_setname,
                project_archs_setname,
                project_oss_setname,
                project_branches_setname,
                project_versions_setname,
                project_compilers_setname,
            ) = get_overall_dashboard_keynames(
                tf_github_org,
                tf_github_repo,
                tf_triggering_env,
                build_variant_name,
                running_platform,
                test_name,
            )

            assert datasink_conn.exists(testcases_setname)
            assert datasink_conn.exists(running_platforms_setname)
            assert datasink_conn.exists(build_variant_setname)
            assert datasink_conn.exists(testcases_and_metric_context_path_setname)
            assert datasink_conn.exists(testcases_metric_context_path_setname)
            assert build_variant_name.encode() in datasink_conn.smembers(
                build_variant_setname
            )
            assert test_name.encode() in datasink_conn.smembers(testcases_setname)
            assert running_platform.encode() in datasink_conn.smembers(
                running_platforms_setname
            )
            testcases_and_metric_context_path_members = [
                x.decode()
                for x in datasink_conn.smembers(
                    testcases_and_metric_context_path_setname
                )
            ]
            metric_context_path_members = [
                x.decode()
                for x in datasink_conn.smembers(testcases_metric_context_path_setname)
            ]
            assert len(testcases_and_metric_context_path_members) == len(
                metric_context_path_members
            )

            assert [x.decode() for x in datasink_conn.smembers(testcases_setname)] == [
                test_name
            ]

            assert "amd64".encode() in datasink_conn.smembers(project_archs_setname)
            assert "debian-bookworm".encode() in datasink_conn.smembers(
                project_oss_setname
            )
            assert "gcc".encode() in datasink_conn.smembers(project_compilers_setname)
            assert build_variant_name.encode() in datasink_conn.smembers(
                build_variant_setname
            )
            assert running_platform.encode() in datasink_conn.smembers(
                running_platforms_setname
            )

            assert len(datasink_conn.smembers(project_archs_setname)) == 1
            assert len(datasink_conn.smembers(project_oss_setname)) == 1
            assert len(datasink_conn.smembers(project_compilers_setname)) == 1
            assert len(datasink_conn.smembers(build_variant_setname)) == 1
            assert len(datasink_conn.smembers(running_platforms_setname)) == 1
            assert len(datasink_conn.smembers(testcases_setname)) == 1
            assert len(datasink_conn.smembers(project_branches_setname)) == 1
            assert len(datasink_conn.smembers(project_versions_setname)) == 0

    except redis.exceptions.ConnectionError:
        pass


def test_start_redis_container():
    temporary_dir = os.getenv("TST_BINARY_REDIS_DIR", "")
    if temporary_dir == "":
        return

    mnt_point = "/mnt/redis/"
    executable = f"{mnt_point}redis-server"
    redis_proc_start_port = 6379
    current_cpu_pos = 0
    ceil_db_cpu_limit = 1
    redis_configuration_parameters = None
    redis_arguments = ""
    docker_client = docker.from_env()
    redis_containers = []

    redis_password = "test_password_123"
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
    run_image = "gcc:8.5"
    redis_container = start_redis_container(
        command_str,
        db_cpuset_cpus,
        docker_client,
        mnt_point,
        redis_containers,
        run_image,
        temporary_dir,
    )
    r = redis.StrictRedis(port=redis_proc_start_port, password=redis_password)
    try:
        r.ping()
    except redis.exceptions.ConnectionError:
        # Access and print the logs
        logs = redis_container.logs().decode("utf-8")
        logging.error("Container failed. Here are the logs:")
        logging.error(logs)
        raise
    redis_container.remove()
