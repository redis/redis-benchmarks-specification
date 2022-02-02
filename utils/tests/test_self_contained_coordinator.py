import os

import docker
import redis
import redistimeseries
import yaml
from pathlib import Path

from redisbench_admin.utils.remote import get_overall_dashboard_keynames
from redisbench_admin.utils.utils import get_ts_metric_name

from redis_benchmarks_specification.__common__.env import (
    STREAM_KEYNAME_NEW_BUILD_EVENTS,
)
from redis_benchmarks_specification.__common__.spec import (
    extract_client_cpu_limit,
    extract_client_container_image,
    extract_client_tool,
)
from redis_benchmarks_specification.__self_contained_coordinator__.self_contained_coordinator import (
    generate_cpuset_cpus,
    self_contained_coordinator_blocking_read,
    build_runners_consumer_group_create,
    get_runners_consumer_group_name,
    prepare_memtier_benchmark_parameters,
)
from redis_benchmarks_specification.__setups__.topologies import get_topologies
from utils.tests.test_data.api_builder_common import flow_1_and_2_api_builder_checks


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


def test_self_contained_coordinator_blocking_read():
    try:
        run_coordinator = True
        TST_RUNNER_X = os.getenv("TST_RUNNER_X", "1")
        if TST_RUNNER_X == "0":
            run_coordinator = False
        if run_coordinator:
            conn = redis.StrictRedis(port=16379)
            conn.ping()
            use_rdb = True
            TST_RUNNER_USE_RDB = os.getenv("TST_RUNNER_USE_RDB", "1")
            build_variant_name = "gcc:8.5.0-amd64-debian-buster-default"
            expected_datapoint_ts = None
            if TST_RUNNER_USE_RDB == "0":
                use_rdb = False
            if use_rdb:
                conn.execute_command("DEBUG", "RELOAD", "NOSAVE")
            else:
                conn.flushall()
                build_variant_name, reply_fields = flow_1_and_2_api_builder_checks(conn)
                expected_datapoint_ts = reply_fields["git_timestamp_ms"]

            assert conn.exists(STREAM_KEYNAME_NEW_BUILD_EVENTS)
            assert conn.xlen(STREAM_KEYNAME_NEW_BUILD_EVENTS) > 0
            running_platform = "fco-ThinkPad-T490"

            build_runners_consumer_group_create(conn, running_platform, "0")
            rts = redistimeseries.client.Client(port=16379)
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
                rts,
                testsuite_spec_files,
                topologies_map,
                running_platform,
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
            assert len(rts.range(ts_key_name, 0, -1)) == 1
            if expected_datapoint_ts is not None:
                assert rts.range(ts_key_name, 0, -1)[0][0] == expected_datapoint_ts
            (
                prefix,
                testcases_setname,
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

            assert rts.redis.exists(testcases_setname)
            assert rts.redis.exists(running_platforms_setname)
            assert rts.redis.exists(build_variant_setname)
            assert rts.redis.exists(testcases_and_metric_context_path_setname)
            assert rts.redis.exists(testcases_metric_context_path_setname)
            assert build_variant_name.encode() in rts.redis.smembers(
                build_variant_setname
            )
            assert test_name.encode() in rts.redis.smembers(testcases_setname)
            assert running_platform.encode() in rts.redis.smembers(
                running_platforms_setname
            )
            testcases_and_metric_context_path_members = [
                x.decode()
                for x in rts.redis.smembers(testcases_and_metric_context_path_setname)
            ]
            metric_context_path_members = [
                x.decode()
                for x in rts.redis.smembers(testcases_metric_context_path_setname)
            ]
            assert len(testcases_and_metric_context_path_members) == len(
                metric_context_path_members
            )

            assert [x.decode() for x in rts.redis.smembers(testcases_setname)] == [
                test_name
            ]

            assert "amd64".encode() in rts.redis.smembers(project_archs_setname)
            assert "debian-buster".encode() in rts.redis.smembers(project_oss_setname)
            assert "gcc".encode() in rts.redis.smembers(project_compilers_setname)
            assert build_variant_name.encode() in rts.redis.smembers(
                build_variant_setname
            )
            assert running_platform.encode() in rts.redis.smembers(
                running_platforms_setname
            )

            assert len(rts.redis.smembers(project_archs_setname)) == 1
            assert len(rts.redis.smembers(project_oss_setname)) == 1
            assert len(rts.redis.smembers(project_compilers_setname)) == 1
            assert len(rts.redis.smembers(build_variant_setname)) == 1
            assert len(rts.redis.smembers(running_platforms_setname)) == 1
            assert len(rts.redis.smembers(testcases_setname)) == 1
            assert len(rts.redis.smembers(project_branches_setname)) == 1
            assert len(rts.redis.smembers(project_versions_setname)) == 0
            # ensure we don't change state on the rdb
            conn.execute_command("DEBUG", "RELOAD", "NOSAVE")

    except redis.exceptions.ConnectionError:
        pass
